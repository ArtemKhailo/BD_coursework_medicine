"""Reduce і finalize для статистики, зібраної з кількох шард.

Операції зведення асоціативні й комутативні (сума, мінімум, максимум), тому результат
не залежить ні від кількості шард, ні від порядку, в якому вони відповіли.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from app.domain.entities.shard_stats import GroupPartial, GroupStats, ShardPartial


@dataclass
class _Accumulator:
    count: int = 0
    price_sum: float = 0.0
    price_min: float = float("inf")
    price_max: float = float("-inf")
    prescription: int = 0
    in_stock: int = 0
    shards: set[str] = field(default_factory=set)

    def add(self, part: GroupPartial, shard: str) -> None:
        if part.count == 0:
            return
        self.count += part.count
        self.price_sum += part.price_sum
        self.price_min = min(self.price_min, part.price_min)
        self.price_max = max(self.price_max, part.price_max)
        self.prescription += part.prescription
        self.in_stock += part.in_stock
        self.shards.add(shard)


class StatsReducer:
    def reduce(self, partials: list[ShardPartial]) -> list[GroupStats]:
        """Зводить часткові підсумки всіх шард і рахує похідні величини.

        Групи впорядковано за кількістю препаратів (спадання), далі за назвою.
        """
        merged: dict[str, _Accumulator] = {}
        for partial in partials:
            for part in partial.groups:
                merged.setdefault(part.key, _Accumulator()).add(part, partial.shard)
        result = [self.finalize(key, acc) for key, acc in merged.items() if acc.count]
        return sorted(result, key=lambda g: (-g.count, g.key))

    @staticmethod
    def finalize(key: str, acc: _Accumulator) -> GroupStats:
        return GroupStats(
            key=key,
            count=acc.count,
            avg_price=acc.price_sum / acc.count,     # середнє лише після зведення сум
            min_price=acc.price_min,
            max_price=acc.price_max,
            prescription_share=acc.prescription / acc.count,
            in_stock_share=acc.in_stock / acc.count,
            shards=tuple(sorted(acc.shards)),
        )

    @staticmethod
    def naive_average(partials: list[ShardPartial], key: str) -> float | None:
        """НЕПРАВИЛЬНИЙ спосіб для порівняння у звіті: середнє з середніх по шардах."""
        averages = [g.price_sum / g.count for p in partials for g in p.groups
                    if g.key == key and g.count]
        return sum(averages) / len(averages) if averages else None

    @staticmethod
    def same_result(a: list[GroupStats], b: list[GroupStats], tolerance: float = 1e-6) -> bool:
        """Чи збігаються два результати (суми з плаваючою комою - з допуском)."""
        if len(a) != len(b):
            return False
        index = {g.key: g for g in b}
        for g in a:
            other = index.get(g.key)
            if other is None or other.count != g.count:
                return False
            for name in ("avg_price", "min_price", "max_price",
                         "prescription_share", "in_stock_share"):
                if abs(getattr(g, name) - getattr(other, name)) > tolerance * max(1.0, abs(getattr(g, name))):
                    return False
        return True
