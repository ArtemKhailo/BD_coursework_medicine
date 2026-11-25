"""Адаптер MongoDB для DocumentRepository (колекція medicines, шардована).

Документ зберігається з _id = medicine_id (рядок UUID). Ключ шардінгу -
{ therapeutic_class: 1, medicine_id: 1 }:
  * запит, що містить обидва поля ключа, mongos відправляє на ОДИН шард;
  * запит лише за _id / medicine_id mongos розсилає на ВСІ шарди (scatter-gather).
Тому записи, де клас відомий (update/повернення картки після оновлення), адресуються
за повним ключем, а пошук картки за id іде на всі шарди (їх лише 3, а за _id є індекс).
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from uuid import UUID

from bson.min_key import MinKey
from pymongo import ASCENDING
from pymongo.errors import BulkWriteError, DuplicateKeyError, OperationFailure

from app.domain.entities.medicine import Medicine
from app.domain.errors import MedicineAlreadyExistsError
from app.domain.ports.outbound.document_repository import DocumentRepository

if TYPE_CHECKING:
    from pymongo import AsyncMongoClient

logger = logging.getLogger(__name__)

SHARD_KEY = [("therapeutic_class", ASCENDING), ("medicine_id", ASCENDING)]
UNKNOWN_CLASS = "unknown"
DUPLICATE_KEY = 11000

# Межі попереднього розбиття колекції на 3 діапазони (по одному на шард).
# Обчислено з prepared/medicines.jsonl: 30,4% / 33,9% / 35,7% документів.
# Без розбиття ~150 МБ даних лишилися б на одному шарді: балансувальник MongoDB 7.0
# переносить дані лише за різниці понад 3 x chunkSize (3 x 128 МБ).
# medicine_id = "" означає «початок класу»: межа ставиться в {клас, MinKey}, тож увесь клас
# лежить в одному діапазоні і запит лише за therapeutic_class іде на один шард.
DEFAULT_SPLIT_POINTS: list[tuple[str, str]] = [
    ("cardiac", ""),        # ["anti diabetic" .. "blood related"] | ["cardiac" ..
    ("neuro cns", "8"),     # .. половина "neuro cns"] | [друга половина "neuro cns" .. кінець]
]

# Службові значення датасету, які не є побічними ефектами (не мають потрапляти в статистику).
SIDE_EFFECT_PLACEHOLDERS = ["no common side effects seen", "limited data available"]

# Які поля можна змінювати через update (ідентифікатори незмінні).
UPDATABLE_FIELDS = {
    "therapeutic_class", "chemical_class", "action_class", "is_prescription", "manufacturer",
    "price", "in_stock", "uses", "side_effects", "substitutes",
}


class MongoDocumentAdapter(DocumentRepository):
    def __init__(self, client: AsyncMongoClient, database: str = "medicine",
                 collection: str = "medicines") -> None:
        self._client = client
        self._database = database
        self._collection_name = collection
        self._collection = client[database][collection]

    @property
    def namespace(self) -> str:
        return f"{self._database}.{self._collection_name}"

    # --- адміністрування кластера ---
    async def ensure_indexes(self) -> None:
        """Індекси: is_prescription + in_stock + price, chemical_class, name.

        Індекс за _id створюється автоматично, індекс ключа шардінгу - у ensure_sharding.
        """
        await self._collection.create_index(
            [("is_prescription", ASCENDING), ("in_stock", ASCENDING), ("price", ASCENDING)],
            name="prescription_stock_price",
        )
        await self._collection.create_index([("chemical_class", ASCENDING)], name="chemical_class")
        await self._collection.create_index([("name", ASCENDING)], name="name")
        logger.info("MongoDB: індекси колекції %s готові", self.namespace)

    async def ensure_sharding(self, split_points: list[tuple[str, str]] | None = None) -> bool:
        """enableSharding + shardCollection за ключем {therapeutic_class: 1, medicine_id: 1}.

        Якщо колекція створюється порожньою, одразу ділить її на діапазони за split_points
        і розносить їх по шардах. Повертає True, якщо колекцію щойно зашардовано,
        False - якщо вона вже була шардованою (повторний виклик нічого не змінює).
        """
        admin = self._client.admin
        existing = await self._client.config.collections.find_one({"_id": self.namespace})
        if existing is not None and not existing.get("dropped", False):
            logger.info("MongoDB: %s вже шардована за %s", self.namespace, existing.get("key"))
            return False

        try:   # з MongoDB 6.0 команда не обов'язкова, але й не шкодить
            await admin.command("enableSharding", self._database)
        except OperationFailure as exc:
            logger.debug("enableSharding: %s", exc)

        was_empty = await self._collection.estimated_document_count() == 0
        await self._collection.create_index(SHARD_KEY, name="shard_key")
        await admin.command("shardCollection", self.namespace, key=dict(SHARD_KEY))
        logger.info("MongoDB: %s зашардовано за ключем %s", self.namespace, dict(SHARD_KEY))

        if was_empty:
            await self._presplit(split_points if split_points is not None else DEFAULT_SPLIT_POINTS)
        return True

    async def _presplit(self, split_points: list[tuple[str, str]]) -> None:
        """Ділить порожню колекцію на len(split_points)+1 діапазонів і розносить їх по шардах."""
        admin = self._client.admin
        shards = sorted(s["_id"] for s in (await admin.command("listShards"))["shards"])
        db_info = await self._client.config.databases.find_one({"_id": self._database})
        primary = db_info["primary"] if db_info else shards[0]
        order = [primary] + [s for s in shards if s != primary]

        for therapeutic_class, medicine_id in split_points:
            await self._split_at(therapeutic_class, medicine_id)

        # Діапазон 0 лишається на primary-шарді, діапазон i переїжджає на order[i].
        for i, (therapeutic_class, medicine_id) in enumerate(split_points, start=1):
            target = order[i % len(order)]
            if target == primary:
                continue
            find = {"therapeutic_class": therapeutic_class, "medicine_id": medicine_id}
            await admin.command("moveChunk", self.namespace, find=find, to=target)
        logger.info("MongoDB: %s розбито на %d діапазони, шарди: %s",
                    self.namespace, len(split_points) + 1, order)

    async def _split_at(self, therapeutic_class: str, medicine_id: str) -> None:
        """split у точці {клас, id}; порожній id -> {клас, MinKey} (межа на початку класу)."""
        admin = self._client.admin
        if medicine_id == "":
            try:
                middle = {"therapeutic_class": therapeutic_class, "medicine_id": MinKey()}
                await admin.command("split", self.namespace, middle=middle)
                return
            except OperationFailure as exc:   # запасний варіант: найменший рядок
                logger.warning("split у {%s, MinKey} не вдався (%s), використовую \"\"",
                               therapeutic_class, exc)
        middle = {"therapeutic_class": therapeutic_class, "medicine_id": medicine_id}
        await admin.command("split", self.namespace, middle=middle)

    async def shard_distribution(self) -> dict[str, int]:
        """Кількість документів колекції на кожному шарді ($collStats через mongos)."""
        cursor = await self._collection.aggregate([{"$collStats": {"count": {}}}])
        rows = await cursor.to_list(length=None)
        return {row.get("shard", "?"): int(row.get("count", 0)) for row in rows}

    # --- мапінг ---
    @staticmethod
    def _to_document(medicine: Medicine) -> dict[str, Any]:
        medicine_id = str(medicine.medicine_id)
        return {
            "_id": medicine_id,
            "medicine_id": medicine_id,
            "source_id": medicine.source_id,
            "name": medicine.name,
            "therapeutic_class": (medicine.therapeutic_class or "").strip() or UNKNOWN_CLASS,
            "chemical_class": medicine.chemical_class,
            "action_class": medicine.action_class,
            "is_habit_forming": medicine.is_habit_forming,
            "is_prescription": medicine.is_prescription,
            "manufacturer": medicine.manufacturer,
            "price": float(medicine.price),
            "in_stock": medicine.in_stock,
            "uses": list(medicine.uses),
            "side_effects": list(medicine.side_effects),
            "substitutes": list(medicine.substitutes),
        }

    @staticmethod
    def _from_document(doc: dict[str, Any]) -> Medicine:
        return Medicine(
            medicine_id=UUID(str(doc["medicine_id"])),
            source_id=int(doc.get("source_id", 0)),
            name=doc["name"],
            therapeutic_class=doc["therapeutic_class"],
            is_habit_forming=bool(doc.get("is_habit_forming", False)),
            is_prescription=bool(doc.get("is_prescription", False)),
            manufacturer=doc.get("manufacturer", ""),
            price=float(doc["price"]),
            chemical_class=doc.get("chemical_class"),
            action_class=doc.get("action_class"),
            uses=list(doc.get("uses") or []),
            side_effects=list(doc.get("side_effects") or []),
            substitutes=list(doc.get("substitutes") or []),
            in_stock=bool(doc.get("in_stock", True)),
        )

    @staticmethod
    def _build_filter(filters: dict[str, Any]) -> dict[str, Any]:
        """Фільтри порту -> запит MongoDB. None означає «фільтр не задано»."""
        query: dict[str, Any] = {}
        for key, value in filters.items():
            if value is None:
                continue
            if key in ("is_prescription", "in_stock", "therapeutic_class", "chemical_class"):
                query[key] = value
            elif key == "name":
                query["name"] = str(value).strip().lower()
            elif key == "price_max":
                query.setdefault("price", {})["$lte"] = float(value)
            elif key == "price_min":
                query.setdefault("price", {})["$gte"] = float(value)
            else:
                raise ValueError(f"Невідомий фільтр: {key}")
        return query

    # --- DocumentRepository: Create ---
    async def create(self, medicine: Medicine) -> Medicine:
        """insert_one. Повторне створення того самого препарату -> MedicineAlreadyExistsError."""
        try:
            await self._collection.insert_one(self._to_document(medicine))
        except DuplicateKeyError as exc:
            raise MedicineAlreadyExistsError(str(medicine.medicine_id)) from exc
        return medicine

    async def create_many(self, medicines: list[Medicine]) -> int:
        """insert_many(ordered=False). Повертає кількість НОВИХ записаних документів.

        Ідемпотентність: _id = medicine_id, а той самий препарат завжди потрапляє на
        той самий шард (ключ шардінгу детермінований), тому повторний пакет дає лише
        помилки дубліката ключа (код 11000). Вони не вважаються збоєм: документ уже є.
        Будь-яка інша помилка запису прокидається далі (пакет не підтверджується).
        """
        if not medicines:
            return 0
        documents = [self._to_document(m) for m in medicines]
        try:
            result = await self._collection.insert_many(documents, ordered=False)
            inserted = len(result.inserted_ids)
        except BulkWriteError as exc:
            write_errors = exc.details.get("writeErrors", [])
            other = [e for e in write_errors if e.get("code") != DUPLICATE_KEY]
            if other:
                raise
            inserted = int(exc.details.get("nInserted", 0))
            logger.debug("insert_many: %d дублікатів пропущено", len(write_errors))
        logger.debug("insert_many: записано %d з %d", inserted, len(documents))
        return inserted

    # --- DocumentRepository: Read ---
    async def get_by_id(self, medicine_id: UUID) -> Medicine | None:
        """find_one({_id}). Ключ шардінгу невідомий -> запит іде на всі шарди."""
        doc = await self._collection.find_one({"_id": str(medicine_id)})
        return self._from_document(doc) if doc else None

    async def by_ids(self, ids: list[UUID]) -> list[Medicine]:
        """Один запит $in; порядок результату відповідає порядку ids."""
        if not ids:
            return []
        keys = [str(i) for i in ids]
        cursor = self._collection.find({"_id": {"$in": keys}})
        found = {doc["_id"]: doc for doc in await cursor.to_list(length=None)}
        return [self._from_document(found[k]) for k in keys if k in found]

    async def find(self, filters: dict[str, Any], limit: int = 20, offset: int = 0) -> list[Medicine]:
        """Фільтрація з пагінацією, сортування за ціною (індекс prescription_stock_price)."""
        cursor = (self._collection.find(self._build_filter(filters))
                  .sort([("price", ASCENDING), ("_id", ASCENDING)])
                  .skip(offset)
                  .limit(limit))
        return [self._from_document(doc) for doc in await cursor.to_list(length=None)]

    async def count(self, filters: dict[str, Any]) -> int:
        return await self._collection.count_documents(self._build_filter(filters))

    async def top_side_effects(self, chemical_class: str, limit: int = 10) -> list[tuple[str, int]]:
        """Aggregation: $match -> $unwind side_effects -> $group -> $sort -> $limit."""
        pipeline = [
            {"$match": {"chemical_class": chemical_class}},
            {"$unwind": "$side_effects"},
            {"$match": {"side_effects": {"$nin": SIDE_EFFECT_PLACEHOLDERS}}},
            {"$group": {"_id": "$side_effects", "count": {"$sum": 1}}},
            {"$sort": {"count": -1, "_id": 1}},
            {"$limit": limit},
        ]
        cursor = await self._collection.aggregate(pipeline)
        return [(row["_id"], int(row["count"])) for row in await cursor.to_list(length=None)]

    # --- DocumentRepository: Update / Delete ---
    async def update(self, medicine_id: UUID, changes: dict[str, Any]) -> Medicine | None:
        """Часткове оновлення.

        1) find_one({_id}) - дізнаємося поточний therapeutic_class (запит на всі шарди);
        2) update_one за ПОВНИМ ключем шардінгу - mongos адресує запис одному шарду.
           Зміна therapeutic_class змінює ключ шардінгу: документ може переїхати на інший
           шард (дозволено для retryable writes, які PyMongo вмикає за замовчуванням).
        """
        unknown = set(changes) - UPDATABLE_FIELDS
        if unknown:
            raise ValueError(f"Ці поля не можна змінювати: {sorted(unknown)}")
        key = str(medicine_id)
        current = await self._collection.find_one({"_id": key}, {"therapeutic_class": 1})
        if current is None:
            return None

        updates = {k: v for k, v in changes.items() if v is not None}
        if "therapeutic_class" in updates:
            updates["therapeutic_class"] = str(updates["therapeutic_class"]).strip() or UNKNOWN_CLASS
        if "price" in updates:
            updates["price"] = float(updates["price"])
        new_class = updates.get("therapeutic_class", current["therapeutic_class"])

        if updates:
            await self._collection.update_one(
                {"therapeutic_class": current["therapeutic_class"], "medicine_id": key},
                {"$set": updates},
            )
        doc = await self._collection.find_one({"therapeutic_class": new_class, "medicine_id": key})
        return self._from_document(doc) if doc else None

    async def delete(self, medicine_id: UUID) -> bool:
        """delete_one({_id}): на шардованій колекції дозволено за _id без повного ключа."""
        result = await self._collection.delete_one({"_id": str(medicine_id)})
        return result.deleted_count == 1
