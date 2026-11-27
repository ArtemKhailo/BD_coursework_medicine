"""REST: статистика каталогу, зібрана з усіх шард за схемою map-reduce (етап 14)."""
from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Query

from app.application.dto.analytics import ShardStatsQuery, ShardStatsResult
from app.application.ports.inbound.get_shard_stats import GetShardStats


class AnalyticsController:
    def __init__(self, get_shard_stats: GetShardStats) -> None:
        self._get_shard_stats = get_shard_stats
        self.router = APIRouter(prefix="/api/analytics", tags=["analytics"])
        self.router.add_api_route("/stats", self.stats, methods=["GET"],
                                  response_model=ShardStatsResult)

    async def stats(
        self,
        group_by: Literal["manufacturer", "therapeutic_class"] = Query(
            "manufacturer", description="Поле групування"),
        in_stock: bool | None = None,
        is_prescription: bool | None = None,
    ) -> ShardStatsResult:
        """GET /api/analytics/stats?group_by=manufacturer&in_stock=true"""
        return await self._get_shard_stats.handle(ShardStatsQuery(
            group_by=group_by, in_stock=in_stock, is_prescription=is_prescription,
        ))
