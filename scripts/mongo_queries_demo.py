"""Демонстрація всіх запитів до MongoDB (етап 10-11): запис (W1-W6) і пошук (R1-R7).

Для кожного запиту друкує: еквівалент у mongosh, результат, час виконання і, для читання,
план mongos (explain): на скільки шардів пішов запит і який індекс використано.
Вивід призначений для скріншотів у звіт (розділ 8).

Запуск із кореня репозиторію (після scripts/load_mongo.py):
    python scripts/mongo_queries_demo.py

Скрипт створює тестовий препарат «demo drug xqz1» і наприкінці видаляє його (W4),
тож основні дані не змінюються. У журнали search_log та ingest_log додається по одному запису.
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pymongo import AsyncMongoClient  # noqa: E402

from app.application.dto.ingest import IngestDocumentDto  # noqa: E402
from app.application.dto.medicine import MedicineCreateDto  # noqa: E402
from app.application.mappers import create_dto_to_medicine, ingest_dto_to_medicine  # noqa: E402
from app.domain.services.query_normalizer import QueryNormalizer  # noqa: E402
from app.infrastructure.observability.mongo_metrics_adapter import MongoMetricsAdapter  # noqa: E402
from app.infrastructure.persistence.mongo_document_adapter import (  # noqa: E402
    SIDE_EFFECT_PLACEHOLDERS, MongoDocumentAdapter,
)
from app.infrastructure.settings import load_settings  # noqa: E402

DEMO_NAME = "demo drug xqz1"
LINE = "-" * 78


def title(code: str, text: str) -> None:
    print(f"\n{LINE}\n{code}. {text}\n{LINE}")


def shell(query: str) -> None:
    print(f"mongosh: {query}")


class Timer:
    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.ms = (time.perf_counter() - self.start) * 1000


def _values(node: Any, key: str) -> list[Any]:
    """Усі значення ключа key у вкладеному плані explain."""
    found = []
    if isinstance(node, dict):
        for k, v in node.items():
            if k == key:
                found.append(v)
            found.extend(_values(v, key))
    elif isinstance(node, list):
        for item in node:
            found.extend(_values(item, key))
    return found


def describe_find_plan(plan: dict[str, Any]) -> str:
    winning = plan.get("queryPlanner", {}).get("winningPlan", {})
    stage = winning.get("stage", "?")
    shards = [s.get("shardName", "?") for s in winning.get("shards", [])]
    indexes = sorted({str(i) for i in _values(winning, "indexName")})
    stages = {str(s) for s in _values(winning, "stage")}
    if not indexes:
        if stages & {"IDHACK", "EXPRESS_IXSCAN"}:      # точний пошук за _id: план без indexName
            indexes = ["_id_ (IDHACK)"]
        elif "COLLSCAN" in stages:
            indexes = ["немає (COLLSCAN)"]
        else:
            indexes = ["?"]
    where = "1 шард" if len(shards) == 1 else f"{len(shards)} шарди"
    return f"explain: {stage} -> {where} {shards}; індекси: {', '.join(indexes)}"


async def explain_find(collection, query: dict[str, Any]) -> str:
    return describe_find_plan(await collection.find(query).explain())


async def explain_aggregate(db, collection_name: str, pipeline: list[dict]) -> str:
    plan = await db.command("aggregate", collection_name, pipeline=pipeline, explain=True)
    shards = sorted(plan.get("shards", {}).keys())
    return f"explain: aggregate на {len(shards)} шарди {shards}"


def first_documents(path: Path, n: int) -> list[dict[str, Any]]:
    rows = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            rows.append(json.loads(line))
            if len(rows) >= n:
                break
    return rows


async def main() -> None:
    settings = load_settings(ROOT / "config.yaml")
    client = AsyncMongoClient(settings.mongo.uri, serverSelectionTimeoutMS=5000)
    db = client[settings.mongo.database]
    collection = db["medicines"]
    documents = MongoDocumentAdapter(client, settings.mongo.database)
    metrics = MongoMetricsAdapter(client, settings.mongo.database)
    sample = first_documents(ROOT / "prepared" / "medicines.jsonl", 1000)

    try:
        total = await documents.count({})
        print(f"Колекція {documents.namespace}: {total} документів")
        print("Розподіл по шардах:", await documents.shard_distribution())

        # ------------------------------------------------------------------ запис
        title("W1", "Пакетний запис (insert_many, ordered=False) - повтор 1000 наявних документів")
        shell("db.medicines.insertMany([...1000 документів...], {ordered: false})")
        batch = [ingest_dto_to_medicine(IngestDocumentDto.model_validate(d)) for d in sample]
        with Timer() as t:
            inserted = await documents.create_many(batch)
        print(f"Нових записано: {inserted} з {len(batch)} (решта - дублікати _id, пропущено) "
              f"за {t.ms:.0f} мс")

        title("W2", "Створення одного препарату (insert_one)")
        dto = MedicineCreateDto(name=DEMO_NAME, therapeutic_class="cardiac", manufacturer="Demo Labs",
                                price=120.0, uses=["demo use"], side_effects=["demo effect"])
        demo = create_dto_to_medicine(dto)
        await documents.delete(demo.medicine_id)           # якщо лишився з минулого запуску
        shell(f'db.medicines.insertOne({{_id: "{demo.medicine_id}", name: "{DEMO_NAME}", '
              f'therapeutic_class: "cardiac", ...}})')
        with Timer() as t:
            await documents.create(demo)
        print(f"Створено {demo.medicine_id} за {t.ms:.1f} мс")
        full_key = {"therapeutic_class": "cardiac", "medicine_id": str(demo.medicine_id)}
        print("Куди потрапив документ (запит за повним ключем шардінгу):")
        print("  " + await explain_find(collection, full_key))

        # ------------------------------------------------------------------ читання
        title("R1", "Картка за id (find_one за _id)")
        shell(f'db.medicines.findOne({{_id: "{demo.medicine_id}"}})')
        with Timer() as t:
            card = await documents.get_by_id(demo.medicine_id)
        print(f"Знайдено: {card.name}, ціна {card.price}, у наявності {card.in_stock} за {t.ms:.1f} мс")
        print("  лише _id:          " + await explain_find(collection, {"_id": str(demo.medicine_id)}))
        print("  повний ключ:       " + await explain_find(collection, full_key))

        title("W3", "Оновлення (update_one за повним ключем шардінгу)")
        shell(f'db.medicines.updateOne({{therapeutic_class: "cardiac", medicine_id: "{demo.medicine_id}"}}, '
              '{$set: {price: 99.5, in_stock: false}})')
        with Timer() as t:
            updated = await documents.update(demo.medicine_id, {"price": 99.5, "in_stock": False})
        print(f"Після оновлення: ціна {updated.price}, у наявності {updated.in_stock} за {t.ms:.1f} мс")

        title("R2", "Картки за списком id після пошуку в Elasticsearch ($in)")
        ids = [batch[i].medicine_id for i in (4, 0, 2)]
        shell(f'db.medicines.find({{_id: {{$in: ["{ids[0]}", ...]}}}})')
        with Timer() as t:
            cards = await documents.by_ids(ids)
        print(f"Отримано {len(cards)} у порядку запиту: {[c.name for c in cards]} за {t.ms:.1f} мс")
        print("  " + await explain_find(collection, {"_id": {"$in": [str(i) for i in ids]}}))

        title("R3", "Фільтрація: безрецептурні до 500 грн, які є в наявності (запит №2 табл. 1.2)")
        filters = {"is_prescription": False, "price_max": 500, "in_stock": True}
        shell("db.medicines.find({is_prescription: false, in_stock: true, price: {$lte: 500}})"
              ".sort({price: 1, _id: 1}).limit(5)")
        with Timer() as t:
            rows = await documents.find(filters, limit=5)
        for m in rows:
            print(f"  {m.price:8.2f}  {m.name}")
        print(f"за {t.ms:.1f} мс")
        print("  " + await explain_find(collection, MongoDocumentAdapter._build_filter(filters)))

        title("R3b", "Та сама фільтрація в межах класу cardiac (префікс ключа шардінгу)")
        filters_cls = {**filters, "therapeutic_class": "cardiac"}
        shell('db.medicines.find({therapeutic_class: "cardiac", is_prescription: false, '
              'in_stock: true, price: {$lte: 500}})')
        with Timer() as t:
            rows = await documents.find(filters_cls, limit=3)
        print(f"Перші: {[m.name for m in rows]} за {t.ms:.1f} мс")
        print("  " + await explain_find(collection, MongoDocumentAdapter._build_filter(filters_cls)))

        title("R4", "Кількість за тим самим фільтром (count_documents)")
        shell("db.medicines.countDocuments({is_prescription: false, in_stock: true, price: {$lte: 500}})")
        with Timer() as t:
            n = await documents.count(filters)
        print(f"Знайдено {n} препаратів за {t.ms:.1f} мс")

        title("R5", "Найчастіші побічні ефекти класу macrolides (запит №6, агрегація)")
        pipeline = [
            {"$match": {"chemical_class": "macrolides"}},
            {"$unwind": "$side_effects"},
            {"$match": {"side_effects": {"$nin": SIDE_EFFECT_PLACEHOLDERS}}},
            {"$group": {"_id": "$side_effects", "count": {"$sum": 1}}},
            {"$sort": {"count": -1, "_id": 1}},
            {"$limit": 5},
        ]
        shell('db.medicines.aggregate([{$match: {chemical_class: "macrolides"}}, {$unwind: "$side_effects"}, '
              '{$match: {side_effects: {$nin: [...]}}}, {$group: {_id: "$side_effects", count: {$sum: 1}}}, '
              '{$sort: {count: -1}}, {$limit: 5}])')
        with Timer() as t:
            top = await documents.top_side_effects("macrolides", limit=5)
        for effect, count in top:
            print(f"  {count:6d}  {effect}")
        print(f"за {t.ms:.1f} мс")
        print("  " + await explain_aggregate(db, "medicines", pipeline))

        # ------------------------------------------------------------- журнали
        title("W5", "Запис у журнал пошуку (search_log.insert_one)")
        raw = "Bacterial Infections"
        normalized = QueryNormalizer().normalize(raw)
        shell(f'db.search_log.insertOne({{raw_query: "{raw}", normalized_query: "{normalized}", ...}})')
        with Timer() as t:
            await metrics.record_query(raw, normalized, results_count=n, cache_hit=False, latency_ms=12)
        print(f"Записано за {t.ms:.1f} мс")

        title("R6", "Останні пошукові запити (search_log, сортування за часом)")
        shell("db.search_log.find().sort({created_at: -1}).limit(5)")
        with Timer() as t:
            recent = await metrics.recent_queries(5)
        for e in recent:
            print(f"  {e.created_at:%H:%M:%S}  {e.raw_query!r:30s} -> {e.status}, {e.latency_ms} мс, "
                  f"кеш: {e.cache_hit}")
        print(f"за {t.ms:.1f} мс")

        title("W6", "Запис статистики пакета (ingest_log.insert_one)")
        shell("db.ingest_log.insertOne({batch_size: 0, duration_ms: 1, created_at: ISODate()})")
        await metrics.record_ingest(0, 1)
        print("Записано")

        title("R7", "Скільки документів записано всього (ingest_log, агрегація $sum)")
        shell('db.ingest_log.aggregate([{$group: {_id: null, total: {$sum: "$batch_size"}}}])')
        with Timer() as t:
            ingested = await metrics.total_ingested()
        print(f"Записано за весь час: {ingested} за {t.ms:.1f} мс")

        title("W4", "Видалення тестового препарату (delete_one за _id)")
        shell(f'db.medicines.deleteOne({{_id: "{demo.medicine_id}"}})')
        with Timer() as t:
            deleted = await documents.delete(demo.medicine_id)
        after = await documents.get_by_id(demo.medicine_id)
        print(f"Видалено: {deleted}; пошук після видалення: {after} за {t.ms:.1f} мс")

        print(f"\n{LINE}\nДокументів у колекції наприкінці: {await documents.count({})}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
