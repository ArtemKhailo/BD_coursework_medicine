"""Етап 14: один запит, зібраний з усіх шард за схемою map-reduce (вивід для звіту).

Показує по кроках:
  1) як дані розкладено по шардах (діапазони ключа з config.chunks);
  2) map: часткові підсумки, які повернула кожна шарда, час і куди mongos надіслав запит;
  3) reduce + finalize: зведений результат;
  4) чому не можна усереднювати середні по шардах (для груп, що лежать на кількох шардах);
  5) той самий запит вбудованою агрегацією MongoDB і план explain (shardsPart / mergerPart).

Запуск із кореня репозиторію (кластер має працювати, дані завантажені):
    python scripts/mapreduce_demo.py                          # групування за виробником
    python scripts/mapreduce_demo.py --group-by therapeutic_class
    python scripts/mapreduce_demo.py --in-stock true
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pymongo import AsyncMongoClient  # noqa: E402

from app.domain.entities.shard_stats import GROUP_FIELDS, UNSHARDED  # noqa: E402
from app.domain.services.stats_reducer import StatsReducer  # noqa: E402
from app.infrastructure.persistence.mongo_shard_analytics_adapter import (  # noqa: E402
    MongoShardAnalyticsAdapter,
)
from app.infrastructure.settings import load_settings  # noqa: E402

LINE = "=" * 100


def title(text: str) -> None:
    print(f"\n{LINE}\n{text}\n{LINE}")


def bound(values: tuple) -> str:
    return "{" + ", ".join(repr(v) if isinstance(v, str) else type(v).__name__ for v in values) + "}"


def parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    return value.lower() in ("true", "1", "так", "yes")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Map-reduce по шардах MongoDB")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--group-by", choices=GROUP_FIELDS, default="manufacturer")
    parser.add_argument("--in-stock", default=None, help="true / false")
    parser.add_argument("--prescription", default=None, help="true / false")
    args = parser.parse_args()

    settings = load_settings(args.config)
    client = AsyncMongoClient(settings.mongo.uri, serverSelectionTimeoutMS=5000)
    analytics = MongoShardAnalyticsAdapter(client, settings.mongo.database)
    reducer = StatsReducer()
    filters = {k: v for k, v in {"in_stock": parse_bool(args.in_stock),
                                 "is_prescription": parse_bool(args.prescription)}.items()
               if v is not None}
    print(f"MongoDB: {settings.mongo.uri}, колекція {settings.mongo.database}.medicines")
    print(f"Запит: статистика препаратів, групування за {args.group_by}, фільтри: {filters or 'немає'}")

    try:
        # ------------------------------------------------------------ 1. партиції
        title("1. Розміщення даних: діапазони ключа шардінгу на кожній шарді (config.chunks)")
        partitions = await analytics.partitions()
        for p in partitions:
            if p.shard == UNSHARDED:
                print("  колекція не шардована: уся колекція - одна партиція")
                continue
            print(f"  {p.shard}: діапазонів {len(p.ranges)}")
            for _fields, lower, upper in p.ranges:
                print(f"      [{bound(lower)} .. {bound(upper)})")

        # ------------------------------------------------------------ 2. map
        title("2. Map: часткові підсумки на кожній шарді (запити виконуються одночасно)")
        example = MongoShardAnalyticsAdapter.group_pipeline(
            MongoShardAnalyticsAdapter.partition_filter(partitions[0]), args.group_by)
        print(f"  запит для {partitions[0].shard} (mongosh):")
        print(f"    db.medicines.aggregate({example})")
        partials = await asyncio.gather(*(
            analytics.map_partition(p, args.group_by, filters) for p in partitions))
        map_ms = max(p.elapsed_ms for p in partials)       # шарди працюють одночасно
        for partial in partials:
            others = [s for s in partial.targeted if s != partial.shard]
            print(f"\n  {partial.shard}: документів {partial.documents:,}, груп: {len(partial.groups)}, "
                  f"{partial.elapsed_ms:.1f} мс; mongos надіслав запит на: "
                  f"{', '.join(partial.targeted) or '-'}")
            if others:
                print(f"    ({', '.join(others)}: межа діапазону збігається з початком її діапазону, "
                      f"документів звідти 0)")
            print(f"    {'група':<28}{'кількість':>10}{'сума цін':>14}{'мін':>9}{'макс':>9}")
            for g in sorted(partial.groups, key=lambda x: (-x.count, x.key))[:5]:
                print(f"    {g.key[:27]:<28}{g.count:>10,}{g.price_sum:>14,.2f}"
                      f"{g.price_min:>9.2f}{g.price_max:>9.2f}")
            if len(partial.groups) > 5:
                print(f"    ... інших груп: {len(partial.groups) - 5}")

        # ------------------------------------------------------------ 3. reduce
        title("3. Reduce + finalize: зведений результат")
        started = time.perf_counter()
        groups = reducer.reduce(list(partials))
        reduce_ms = (time.perf_counter() - started) * 1000
        total = sum(g.count for g in groups)
        print(f"  {'група':<28}{'кількість':>10}{'сер. ціна':>11}{'мін':>9}{'макс':>9}"
              f"{'рецепт.':>9}{'наявн.':>9}  шарди")
        for g in groups:
            print(f"  {g.key[:27]:<28}{g.count:>10,}{g.avg_price:>11.2f}{g.min_price:>9.2f}"
                  f"{g.max_price:>9.2f}{g.prescription_share:>8.1%}{g.in_stock_share:>9.1%}  "
                  f"{', '.join(g.shards)}")
        print(f"\n  Разом документів: {total:,}; груп: {len(groups)}")
        print(f"  Час: map {map_ms:.1f} мс (найдовша шарда; шарди працюють одночасно), "
              f"reduce {reduce_ms:.2f} мс, разом {map_ms + reduce_ms:.1f} мс")

        # ------------------------------------------------------------ 4. пастка середнього
        title("4. Чому шарди повертають суму і кількість, а не середнє")
        spread = [g for g in groups if len(g.shards) > 1]
        if not spread:
            print("  жодна група не лежить на кількох шардах")
        for g in spread[:5]:
            naive = reducer.naive_average(list(partials), g.key)
            print(f"  {g.key}: правильне середнє {g.avg_price:.4f}; середнє середніх по шардах "
                  f"{naive:.4f}; різниця {naive - g.avg_price:+.4f}")

        # ------------------------------------------------------------ 5. вбудований варіант
        title("5. Той самий запит вбудованою агрегацією MongoDB (один aggregate через mongos)")
        native = await analytics.aggregate(args.group_by, filters)
        print(f"  час: {native.elapsed_ms:.1f} мс; шарди: {', '.join(native.shards) or '-'}; "
              f"злиття: {native.merger or '-'}")
        if native.shards_part:
            print(f"  explain.splitPipeline.shardsPart (map на шардах): {' -> '.join(native.shards_part)}")
            print(f"  explain.splitPipeline.mergerPart (reduce):       {' -> '.join(native.merger_part)}")
        else:
            print("  план без поділу на частини (колекція не шардована)")
        same = reducer.same_result(groups, native.groups)
        print(f"\n  Результати власного map-reduce і вбудованої агрегації "
              f"{'ЗБІГАЮТЬСЯ' if same else 'РІЗНЯТЬСЯ'} (груп: {len(groups)})")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
