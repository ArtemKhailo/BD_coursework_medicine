"""Адаптер MongoDB для MetricsSink (колекції search_log та ingest_log, без шардінгу).

Обидві колекції службові й невеликі, тому живуть на primary-шарді бази без шардінгу.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from pymongo import DESCENDING

from app.domain.entities.search_log_entry import SearchLogEntry
from app.domain.ports.outbound.metrics_sink import MetricsSink

if TYPE_CHECKING:
    from pymongo import AsyncMongoClient

logger = logging.getLogger(__name__)


class MongoMetricsAdapter(MetricsSink):
    def __init__(self, client: AsyncMongoClient, database: str = "medicine") -> None:
        self._search_log = client[database]["search_log"]
        self._ingest_log = client[database]["ingest_log"]

    async def ensure_indexes(self) -> None:
        """Індекси за часом: останні запити та графік швидкості запису."""
        await self._search_log.create_index([("created_at", DESCENDING)], name="created_at")
        await self._ingest_log.create_index([("created_at", DESCENDING)], name="created_at")

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
