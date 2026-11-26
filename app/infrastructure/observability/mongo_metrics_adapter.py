"""Адаптер MongoDB для MetricsSink (колекції search_log та ingest_log, без шардінгу).

Обидві колекції службові й невеликі, тому живуть на primary-шарді бази без шардінгу.

Партиціонування за часом (етап 13):
  * ingest_log - колекція часових рядів (time series): MongoDB сама групує записи
    в бакети за полем created_at, тобто дані фізично поділені на часові проміжки,
    а бакети старші за INGEST_LOG_TTL_DAYS видаляються цілими;
  * search_log - звичайна колекція з TTL-індексом за created_at: записи старші
    за SEARCH_LOG_TTL_DAYS видаляються фоновим потоком mongod.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from pymongo import DESCENDING
from pymongo.errors import OperationFailure

from app.domain.entities.search_log_entry import SearchLogEntry
from app.domain.ports.outbound.metrics_sink import MetricsSink

if TYPE_CHECKING:
    from pymongo import AsyncMongoClient

logger = logging.getLogger(__name__)

SEARCH_LOG_TTL_DAYS = 30
INGEST_LOG_TTL_DAYS = 90
DAY_SECONDS = 24 * 60 * 60
INDEX_OPTIONS_CONFLICT = (85, 86)   # IndexOptionsConflict, IndexKeySpecsConflict


class MongoMetricsAdapter(MetricsSink):
    def __init__(self, client: AsyncMongoClient, database: str = "medicine") -> None:
        self._db = client[database]
        self._search_log = self._db["search_log"]
        self._ingest_log = self._db["ingest_log"]

    async def ensure_indexes(self) -> None:
        """Партиціонування журналів за часом та індекси для запитів «останні N записів».

        ingest_log створюється як колекція часових рядів, лише якщо її ще немає:
        звичайну колекцію MongoDB не перетворює на часові ряди (load_mongo.py --drop
        видаляє ingest_log, і вона створюється наново вже правильного типу).
        """
        existing = set(await self._db.list_collection_names())
        if "ingest_log" not in existing:
            await self._db.create_collection(
                "ingest_log",
                timeseries={"timeField": "created_at", "granularity": "seconds"},
                expireAfterSeconds=INGEST_LOG_TTL_DAYS * DAY_SECONDS,
            )
            logger.info("MongoDB: ingest_log створено як колекцію часових рядів")
        elif not await self.is_time_series("ingest_log"):
            logger.warning("MongoDB: ingest_log - звичайна колекція; щоб зробити її часовими "
                           "рядами, перезавантажте дані: python scripts/load_mongo.py --drop")
        await self._ingest_log.create_index([("created_at", DESCENDING)], name="created_at")
        await self._ensure_ttl_index(self._search_log, SEARCH_LOG_TTL_DAYS * DAY_SECONDS)

    async def _ensure_ttl_index(self, collection, ttl_seconds: int) -> None:
        """TTL-індекс created_at. Якщо звичайний індекс з такою назвою вже є (створений
        до етапу 13), він перетворюється на TTL командою collMod без перебудови."""
        try:
            await collection.create_index([("created_at", DESCENDING)], name="created_at",
                                          expireAfterSeconds=ttl_seconds)
        except OperationFailure as exc:
            if exc.code not in INDEX_OPTIONS_CONFLICT:
                raise
            await self._db.command("collMod", collection.name, index={
                "keyPattern": {"created_at": DESCENDING},
                "expireAfterSeconds": ttl_seconds,
            })
            logger.info("MongoDB: індекс %s.created_at перетворено на TTL", collection.name)

    async def is_time_series(self, name: str) -> bool:
        cursor = await self._db.list_collections(filter={"name": name})
        info = await cursor.to_list(length=None)
        return bool(info) and info[0].get("type") == "timeseries"

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    async def record_query(self, raw_query: str, normalized_query: str,
                           results_count: int, cache_hit: bool, latency_ms: int) -> None:
        query_id = str(uuid4())
        await self._search_log.insert_one({
            "_id": query_id,
            "query_id": query_id,
            "raw_query": raw_query,
            "normalized_query": normalized_query,
            "results_count": int(results_count),
            "cache_hit": bool(cache_hit),
            "latency_ms": int(latency_ms),
            "created_at": self._now(),
        })

    @staticmethod
    def _to_entry(doc: dict[str, Any]) -> SearchLogEntry:
        return SearchLogEntry(
            query_id=UUID(str(doc["query_id"])),
            raw_query=doc["raw_query"],
            normalized_query=doc["normalized_query"],
            results_count=int(doc["results_count"]),
            cache_hit=bool(doc["cache_hit"]),
            latency_ms=int(doc["latency_ms"]),
            created_at=doc["created_at"],
        )

    async def recent_queries(self, limit: int = 20) -> list[SearchLogEntry]:
        """find().sort(created_at, -1).limit(n) - останні запити для панелі моніторингу."""
        cursor = self._search_log.find().sort("created_at", DESCENDING).limit(limit)
        return [self._to_entry(doc) for doc in await cursor.to_list(length=None)]

    async def record_ingest(self, batch_size: int, duration_ms: int) -> None:
        """Фіксує записаний пакет (для графіка швидкості запису)."""
        await self._ingest_log.insert_one({
            "batch_size": int(batch_size),
            "duration_ms": int(duration_ms),
            "created_at": self._now(),
        })

    async def total_ingested(self) -> int:
        """Кількість успішно збережених документів: сума batch_size усіх пакетів."""
        cursor = await self._ingest_log.aggregate(
            [{"$group": {"_id": None, "total": {"$sum": "$batch_size"}}}]
        )
        rows = await cursor.to_list(length=None)
        return int(rows[0]["total"]) if rows else 0
