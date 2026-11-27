"""Map-reduce по шардах: map паралельно на кожній шарді, reduce і finalize у застосунку.

Для перевірки той самий запит виконується вбудованою агрегацією MongoDB, і результати
порівнюються (мають збігтися до похибки округлення сум).
"""
from __future__ import annotations

import asyncio
import time

from app.application.dto.analytics import (
    GroupPartialDto, GroupStatsDto, NativeAggregationDto, ShardPartialDto,
    ShardStatsQuery, ShardStatsResult,
)
from app.application.ports.inbound.get_shard_stats import GetShardStats
from app.domain.entities.shard_stats import UNSHARDED, GroupStats
from app.domain.ports.outbound import ShardAnalytics
from app.domain.services.stats_reducer import StatsReducer


def _group_dto(g: GroupStats) -> GroupStatsDto:
    return GroupStatsDto(
        key=g.key, count=g.count, avg_price=round(g.avg_price, 2),
        min_price=g.min_price, max_price=g.max_price,
        prescription_share=round(g.prescription_share, 4),
        in_stock_share=round(g.in_stock_share, 4), shards=list(g.shards),
    )


class GetShardStatsHandler(GetShardStats):
    def __init__(self, analytics: ShardAnalytics, reducer: StatsReducer | None = None) -> None:
        self._analytics = analytics
        self._reducer = reducer or StatsReducer()

    async def handle(self, query: ShardStatsQuery) -> ShardStatsResult:
        filters = query.model_dump(exclude_none=True, exclude={"group_by"})

        started = time.perf_counter()
        partitions = await self._analytics.partitions()
        lookup_ms = (time.perf_counter() - started) * 1000

        # map: на всіх шардах одночасно. Час map - найдовша шарда; адаптер міряє
        # лише сам запит, без допоміжного explain, який потрібен тільки для показу плану.
        partials = await asyncio.gather(*(
            self._analytics.map_partition(p, query.group_by, filters) for p in partitions
        ))
        map_ms = max((p.elapsed_ms for p in partials), default=0.0)

        # reduce + finalize
        started = time.perf_counter()
        groups = self._reducer.reduce(list(partials))
        reduce_ms = (time.perf_counter() - started) * 1000
        map_reduce_ms = lookup_ms + map_ms + reduce_ms

        native = await self._analytics.aggregate(query.group_by, filters)

        return ShardStatsResult(
            group_by=query.group_by,
            sharded=not (len(partitions) == 1 and partitions[0].shard == UNSHARDED),
            total_documents=sum(p.documents for p in partials),
            map_reduce_ms=round(map_reduce_ms, 1),
            lookup_ms=round(lookup_ms, 1),
            map_ms=round(map_ms, 1),
            reduce_ms=round(reduce_ms, 2),
            matches_native=self._reducer.same_result(groups, native.groups),
            groups=[_group_dto(g) for g in groups],
            shards=[
                ShardPartialDto(
                    shard=p.shard, documents=p.documents, elapsed_ms=round(p.elapsed_ms, 1),
                    targeted=p.targeted,
                    groups=[GroupPartialDto(**vars(g)) for g in
                            sorted(p.groups, key=lambda g: (-g.count, g.key))],
                )
                for p in partials
            ],
            native=NativeAggregationDto(
                elapsed_ms=round(native.elapsed_ms, 1), shards=native.shards,
                merger=native.merger, shards_part=native.shards_part,
                merger_part=native.merger_part,
            ),
        )
