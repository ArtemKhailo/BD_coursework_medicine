"""Статистика каталогу, зібрана з кількох шард за схемою map-reduce (етап 14).

map:      кожна шарда рахує часткові підсумки лише за своїми документами (GroupPartial);
reduce:   часткові підсумки однакових груп з різних шард складаються;
finalize: похідні величини (середня ціна, частки) рахуються лише після зведення.

Шарда повертає суму і кількість, а не готове середнє: середнє середніх неправильне,
якщо на шардах різна кількість документів.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Поля, за якими можна групувати статистику.
GROUP_FIELDS = ("manufacturer", "therapeutic_class")
UNSHARDED = "unsharded"          # колекція не шардована (наприклад, MongoDB Atlas у Render)
MISSING_KEY = "не вказано"       # документи без значення поля групування


@dataclass(frozen=True)
class ShardPartition:
    """Дані однієї шарди: її діапазони ключа шардінгу (формат діапазонів знає лише адаптер)."""
    shard: str                                       # назва шарди (або UNSHARDED)
    ranges: tuple[Any, ...] = field(default=())


@dataclass(frozen=True)
class GroupPartial:
    """Частковий підсумок однієї групи на одній шарді (результат map)."""
    key: str
    count: int
    price_sum: float
    price_min: float
    price_max: float
    prescription: int            # скільки рецептурних
    in_stock: int                # скільки в наявності


@dataclass
class ShardPartial:
    """Результат map на одній шарді."""
    shard: str
    groups: list[GroupPartial] = field(default_factory=list)
    elapsed_ms: float = 0.0
    targeted: list[str] = field(default_factory=list)   # куди mongos фактично надіслав запит

    @property
    def documents(self) -> int:
        return sum(g.count for g in self.groups)


@dataclass(frozen=True)
class GroupStats:
    """Підсумковий рядок після reduce і finalize."""
    key: str
    count: int
    avg_price: float
    min_price: float
    max_price: float
    prescription_share: float    # 0..1
    in_stock_share: float        # 0..1
    shards: tuple[str, ...] = () # шарди, що дали внесок у групу


@dataclass
class NativeAggregation:
    """Той самий запит одним aggregate через mongos (MongoDB сама ділить його на map і reduce)."""
    groups: list[GroupStats] = field(default_factory=list)
    elapsed_ms: float = 0.0
    shards: list[str] = field(default_factory=list)       # шарди, що виконували shardsPart
    merger: str | None = None                             # де виконувалося злиття (mergerPart)
    shards_part: list[str] = field(default_factory=list)  # стадії, виконані на шардах
    merger_part: list[str] = field(default_factory=list)  # стадії злиття
