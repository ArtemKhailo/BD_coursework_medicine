"""DTO аналітики по шардах (етап 14): GET /api/analytics/stats."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class ShardStatsQuery(BaseModel):
    group_by: Literal["manufacturer", "therapeutic_class"] = "manufacturer"
    in_stock: bool | None = None
    is_prescription: bool | None = None


class GroupStatsDto(BaseModel):
    key: str
    count: int
    avg_price: float
    min_price: float
    max_price: float
    prescription_share: float
    in_stock_share: float
    shards: list[str] = Field(default_factory=list)


class GroupPartialDto(BaseModel):
    key: str
    count: int
    price_sum: float
    price_min: float
    price_max: float
    prescription: int
    in_stock: int


class ShardPartialDto(BaseModel):
    shard: str
    documents: int
    elapsed_ms: float
    targeted: list[str] = Field(default_factory=list)
    groups: list[GroupPartialDto] = Field(default_factory=list)


class NativeAggregationDto(BaseModel):
    elapsed_ms: float
    shards: list[str] = Field(default_factory=list)
    merger: str | None = None
    shards_part: list[str] = Field(default_factory=list)
    merger_part: list[str] = Field(default_factory=list)


class ShardStatsResult(BaseModel):
    group_by: str
    sharded: bool                      # False - колекція не шардована (одна партиція)
    total_documents: int
    map_reduce_ms: float               # метадані + паралельний map + reduce
    lookup_ms: float = 0.0             # читання розміщення діапазонів (config.chunks)
    map_ms: float = 0.0                # map: найдовша шарда (шарди працюють одночасно)
    reduce_ms: float = 0.0             # reduce + finalize у застосунку
    matches_native: bool               # чи збігся результат із вбудованою агрегацією
    groups: list[GroupStatsDto] = Field(default_factory=list)
    shards: list[ShardPartialDto] = Field(default_factory=list)
    native: NativeAggregationDto
