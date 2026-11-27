"""Вихідний порт: виконання одного запиту на кожній шарді окремо (map) і вбудованою агрегацією.

Адаптер: MongoDB (MongoShardAnalyticsAdapter). Партиція - це набір діапазонів ключа
шардінгу, що належать одній шарді; її вміст для домену непрозорий.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from app.domain.entities.shard_stats import NativeAggregation, ShardPartial, ShardPartition


class ShardAnalytics(ABC):
    @abstractmethod
    async def partitions(self) -> list[ShardPartition]:
        """Розміщення даних: які діапазони ключа лежать на якій шарді."""

    @abstractmethod
    async def map_partition(self, partition: ShardPartition, group_by: str,
                            filters: dict[str, Any]) -> ShardPartial:
        """map: часткові підсумки груп лише за документами однієї партиції."""

    @abstractmethod
    async def aggregate(self, group_by: str, filters: dict[str, Any]) -> NativeAggregation:
        """Той самий запит одним aggregate (map і reduce виконує сама СУБД)."""
