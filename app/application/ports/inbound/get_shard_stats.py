"""Вхідний порт: статистика каталогу, зібрана з усіх шард за схемою map-reduce."""
from __future__ import annotations

from abc import ABC, abstractmethod

from app.application.dto.analytics import ShardStatsQuery, ShardStatsResult


class GetShardStats(ABC):
    @abstractmethod
    async def handle(self, query: ShardStatsQuery) -> ShardStatsResult: ...
