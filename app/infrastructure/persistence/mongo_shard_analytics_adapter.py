"""Адаптер MongoDB для ShardAnalytics: один запит, виконаний на кожній шарді окремо (етап 14).

Як визначається, що лежить на шарді. Метадані кластера (config.chunks) описують
діапазони ключа шардінгу {therapeutic_class, medicine_id} і шарду-власника кожного.
Партиція шарди - усі її діапазони. Запит з умовою «ключ у діапазонах цієї шарди»
mongos надсилає лише туди, де ці діапазони лежать (так само ділить дані на партиції
конектор MongoDB для Spark). Якщо верхня межа діапазону збігається з початком
діапазону сусідньої шарди, mongos може звернутися й до неї; вона поверне порожній
результат, бо її документи під умову не підпадають.

Якщо колекція не шардована (MongoDB Atlas у Render), уся колекція - одна партиція.
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from bson.max_key import MaxKey
from bson.min_key import MinKey
from pymongo.errors import OperationFailure

from app.domain.entities.shard_stats import (
    GROUP_FIELDS, MISSING_KEY, UNSHARDED, GroupPartial, NativeAggregation, ShardPartial,
    ShardPartition,
)
from app.domain.ports.outbound.shard_analytics import ShardAnalytics
from app.domain.services.stats_reducer import StatsReducer

if TYPE_CHECKING:
    from pymongo import AsyncMongoClient

logger = logging.getLogger(__name__)

ALLOWED_FILTERS = ("in_stock", "is_prescription")


def _is_min(value: Any) -> bool:
    return isinstance(value, MinKey)


def _is_max(value: Any) -> bool:
    return isinstance(value, MaxKey)


class MongoShardAnalyticsAdapter(ShardAnalytics):
    def __init__(self, client: AsyncMongoClient, database: str = "medicine",
                 collection: str = "medicines") -> None:
        self._client = client
        self._db = client[database]
        self._collection_name = collection
        self._collection = self._db[collection]
        self._namespace = f"{database}.{collection}"

    # ------------------------------------------------------------ партиції
    async def partitions(self) -> list[ShardPartition]:
        try:
            meta = await self._client.config.collections.find_one({"_id": self._namespace})
        except OperationFailure as exc:       # немає доступу до config (керовані сервіси)
            logger.info("MongoDB: метадані шардінгу недоступні (%s)", exc)
            meta = None
        if meta is None or meta.get("dropped", False):
            return [ShardPartition(UNSHARDED)]

        fields = tuple(meta["key"].keys())
        cursor = self._client.config.chunks.find(
            {"uuid": meta["uuid"]}, {"min": 1, "max": 1, "shard": 1}).sort("min", 1)
        by_shard: dict[str, list] = defaultdict(list)
        for chunk in await cursor.to_list(length=None):
            lower = tuple(chunk["min"][f] for f in fields)
            upper = tuple(chunk["max"][f] for f in fields)
            by_shard[chunk["shard"]].append((fields, lower, upper))
        return [ShardPartition(shard, tuple(ranges)) for shard, ranges in sorted(by_shard.items())]

    # ------------------------------------------------------------ фільтр діапазонів
    @staticmethod
    def range_boxes(fields: tuple[str, ...], lower: tuple, upper: tuple) -> list[dict[str, Any]]:
        """Діапазон [lower, upper) складеного ключа -> умови-«коробки», об'єднані через $or.

        Для ключа (f1, f2) діапазон розкладається на три частини:
          f1 = lower1 і f2 >= lower2;   lower1 < f1 < upper1;   f1 = upper1 і f2 < upper2.
        Межі MinKey / MaxKey спрощуються. Такий вигляд (плоский $or) mongos уміє
        перетворити на точні межі ключа і надіслати запит лише на потрібні шарди.
        """
        if len(fields) == 1:
            (f,), (lo,), (hi,) = fields, lower, upper
            cond: dict[str, Any] = {}
            if not _is_min(lo):
                cond["$gte"] = lo
            if not _is_max(hi):
                cond["$lt"] = hi
            return [{f: cond} if cond else {}]
        if len(fields) != 2:
            raise ValueError("підтримуються ключі шардінгу з одного або двох полів")

        f1, f2 = fields
        lo1, lo2 = lower
        hi1, hi2 = upper

        same_class = not (_is_min(lo1) or _is_max(lo1)) and lo1 == hi1
        if same_class:                                   # діапазон усередині одного f1
            cond2: dict[str, Any] = {}
            if not _is_min(lo2):
                cond2["$gte"] = lo2
            if not _is_max(hi2):
                cond2["$lt"] = hi2
            return [{f1: lo1, **({f2: cond2} if cond2 else {})}]

        first, last = [], []
        middle: dict[str, Any] = {}
        # нижня межа
        if _is_min(lo1):
            pass                                         # від початку ключа
        elif _is_min(lo2):
            middle["$gte"] = lo1                         # {lo1, MinKey}: увесь клас lo1 входить
        elif _is_max(lo2):
            middle["$gt"] = lo1                          # {lo1, MaxKey}: клас lo1 не входить
        else:
            first.append({f1: lo1, f2: {"$gte": lo2}})
            middle["$gt"] = lo1
        # верхня межа (не включно)
        if _is_max(hi1):
            pass                                         # до кінця ключа
        elif _is_min(hi2):
            middle["$lt"] = hi1                          # {hi1, MinKey}: клас hi1 не входить
        elif _is_max(hi2):
            middle["$lte"] = hi1                         # {hi1, MaxKey}: клас hi1 входить увесь
        else:
            last.append({f1: hi1, f2: {"$lt": hi2}})
            middle["$lt"] = hi1
        return first + [{f1: middle} if middle else {}] + last

    @classmethod
    def partition_filter(cls, partition: ShardPartition) -> dict[str, Any]:
        """Умова «документ лежить у діапазонах цієї партиції»."""
        if not partition.ranges:
            return {}
        boxes = [box for fields, lower, upper in partition.ranges
                 for box in cls.range_boxes(fields, lower, upper)]
        if any(box == {} for box in boxes):
            return {}
        return boxes[0] if len(boxes) == 1 else {"$or": boxes}

    @staticmethod
    def _filters(filters: dict[str, Any]) -> dict[str, Any]:
        unknown = set(filters) - set(ALLOWED_FILTERS)
        if unknown:
            raise ValueError(f"Невідомі фільтри: {sorted(unknown)}")
        return {k: bool(v) for k, v in filters.items() if v is not None}

    @staticmethod
    def group_pipeline(match: dict[str, Any], group_by: str) -> list[dict[str, Any]]:
        """Часткові підсумки груп: лише суми, кількості, мінімуми і максимуми."""
        if group_by not in GROUP_FIELDS:
            raise ValueError(f"Групування за {group_by} не підтримується")
        return [
            {"$match": match},
            {"$group": {
                "_id": f"${group_by}",
                "count": {"$sum": 1},
                "price_sum": {"$sum": "$price"},
                "price_min": {"$min": "$price"},
                "price_max": {"$max": "$price"},
                "prescription": {"$sum": {"$cond": ["$is_prescription", 1, 0]}},
                "in_stock": {"$sum": {"$cond": ["$in_stock", 1, 0]}},
            }},
        ]

    @staticmethod
    def _to_partial(row: dict[str, Any]) -> GroupPartial:
        key = row["_id"]
        return GroupPartial(
            key=str(key) if key not in (None, "") else MISSING_KEY,
            count=int(row["count"]),
            price_sum=float(row["price_sum"]),
            price_min=float(row["price_min"]),
            price_max=float(row["price_max"]),
            prescription=int(row["prescription"]),
            in_stock=int(row["in_stock"]),
        )

    async def _explain(self, pipeline: list[dict[str, Any]]) -> dict[str, Any]:
        try:
            return await self._db.command("aggregate", self._collection_name,
                                          pipeline=pipeline, explain=True)
        except OperationFailure as exc:
            logger.info("MongoDB: explain недоступний (%s)", exc)
            return {}

    # ------------------------------------------------------------ map
    async def map_partition(self, partition: ShardPartition, group_by: str,
                            filters: dict[str, Any]) -> ShardPartial:
        match = {**self._filters(filters), **self.partition_filter(partition)}
        pipeline = self.group_pipeline(match, group_by)
        started = time.perf_counter()
        cursor = await self._collection.aggregate(pipeline)
        rows = await cursor.to_list(length=None)
        elapsed = (time.perf_counter() - started) * 1000
        plan = await self._explain(pipeline)
        return ShardPartial(
            shard=partition.shard,
            groups=[self._to_partial(row) for row in rows],
            elapsed_ms=elapsed,
            targeted=sorted(plan.get("shards", {}).keys()),
        )

    # ------------------------------------------------------------ вбудований варіант
    async def aggregate(self, group_by: str, filters: dict[str, Any]) -> NativeAggregation:
        pipeline = self.group_pipeline(self._filters(filters), group_by)
        started = time.perf_counter()
        cursor = await self._collection.aggregate(pipeline)
        rows = await cursor.to_list(length=None)
        elapsed = (time.perf_counter() - started) * 1000

        reduced = StatsReducer().reduce([ShardPartial("native", [self._to_partial(r) for r in rows])])
        plan = await self._explain(pipeline)
        split = plan.get("splitPipeline") or {}
        return NativeAggregation(
            groups=[replace(g, shards=()) for g in reduced],
            elapsed_ms=elapsed,
            shards=sorted(plan.get("shards", {}).keys()),
            merger=plan.get("mergeType"),
            shards_part=[next(iter(stage)) for stage in split.get("shardsPart", []) or []],
            merger_part=[next(iter(stage)) for stage in split.get("mergerPart", []) or []],
        )
