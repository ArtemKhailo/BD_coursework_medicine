"""Етап 14: map-reduce по шардах - зведення підсумків, фільтри діапазонів, обробник, маршрут."""
import random
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

pytest.importorskip("bson")
from bson.max_key import MaxKey  # noqa: E402
from bson.min_key import MinKey  # noqa: E402

from app.application.dto.analytics import ShardStatsQuery  # noqa: E402
from app.application.queries.get_shard_stats_handler import GetShardStatsHandler  # noqa: E402
from app.domain.entities.shard_stats import (  # noqa: E402
    UNSHARDED, GroupPartial, NativeAggregation, ShardPartial, ShardPartition,
)
from app.domain.services.stats_reducer import StatsReducer  # noqa: E402
from app.infrastructure.persistence.mongo_shard_analytics_adapter import (  # noqa: E402
    MongoShardAnalyticsAdapter as Adapter,
)
from app.presentation.rest.analytics_controller import AnalyticsController  # noqa: E402


def part(key, prices, prescription=0, in_stock=0):
    return GroupPartial(key, len(prices), float(sum(prices)), float(min(prices)),
                        float(max(prices)), prescription, in_stock)


# ------------------------------------------------------------ reduce

def test_reduce_merges_partials_of_the_same_group_from_different_shards():
    partials = [
        ShardPartial("shard1rs", [part("Medisolve", [100], prescription=1, in_stock=1),
                                  part("Bioclair", [300, 500])]),
        ShardPartial("shard2rs", [part("Medisolve", [200, 200, 200], in_stock=3)]),
    ]
    result = {g.key: g for g in StatsReducer().reduce(partials)}

    medisolve = result["Medisolve"]
    assert medisolve.count == 4
    assert medisolve.avg_price == 175.0          # (100 + 600) / 4
    assert (medisolve.min_price, medisolve.max_price) == (100.0, 200.0)
    assert medisolve.prescription_share == 0.25 and medisolve.in_stock_share == 1.0
    assert medisolve.shards == ("shard1rs", "shard2rs")
    assert result["Bioclair"].shards == ("shard1rs",)


def test_average_of_averages_would_be_wrong():
    """Шарда 1: одна ціна 100; шарда 2: три ціни по 200. Правильно 175, а не 150."""
    partials = [ShardPartial("a", [part("x", [100])]),
                ShardPartial("b", [part("x", [200, 200, 200])])]
    reducer = StatsReducer()
    assert reducer.reduce(partials)[0].avg_price == 175.0
    assert reducer.naive_average(partials, "x") == 150.0


def test_result_does_not_depend_on_shard_order():
    partials = [ShardPartial(f"s{i}", [part("k", [10 * i, 10 * i + 5])]) for i in range(1, 5)]
    reducer = StatsReducer()
    assert reducer.reduce(partials) == reducer.reduce(list(reversed(partials)))


def test_groups_sorted_by_count_and_empty_input():
    partials = [ShardPartial("s", [part("b", [1]), part("a", [1, 1]), part("c", [1])])]
    assert [g.key for g in StatsReducer().reduce(partials)] == ["a", "b", "c"]
    assert StatsReducer().reduce([]) == []
    assert StatsReducer().reduce([ShardPartial("s", [])]) == []


def test_same_result_detects_differences():
    reducer = StatsReducer()
    a = reducer.reduce([ShardPartial("s", [part("x", [100, 200])])])
    b = reducer.reduce([ShardPartial("t", [part("x", [100]), part("x", [200])])])
    c = reducer.reduce([ShardPartial("s", [part("x", [100, 201])])])
    assert reducer.same_result(a, b)
    assert not reducer.same_result(a, c)
    assert not reducer.same_result(a, [])


# ------------------------------------------------------------ діапазони ключа

def _cmp(a, b):
    rank = lambda v: 0 if isinstance(v, MinKey) else 2 if isinstance(v, MaxKey) else 1  # noqa: E731
    if rank(a) != rank(b):
        return rank(a) - rank(b)
    if rank(a) != 1:
        return 0
    return (a > b) - (a < b)


def _matches(doc, flt):
    """Мінімальний інтерпретатор фільтрів MongoDB, які будує адаптер."""
    if not flt:
        return True
    if "$or" in flt:
        return any(_matches(doc, f) for f in flt["$or"])
    for field, cond in flt.items():
        value = doc[field]
        if isinstance(cond, dict):
            ops = {"$gt": lambda c: c > 0, "$gte": lambda c: c >= 0,
                   "$lt": lambda c: c < 0, "$lte": lambda c: c <= 0}
            if not all(ops[op](_cmp(value, arg)) for op, arg in cond.items()):
                return False
        elif _cmp(value, cond) != 0:
            return False
    return True


FIELDS = ("therapeutic_class", "medicine_id")


def _partitions(bounds, shards):
    """Ланцюжок меж -> діапазони [b_i, b_i+1), розкладені по шардах по колу."""
    ranges = {s: [] for s in shards}
    for i, (lower, upper) in enumerate(zip(bounds, bounds[1:])):
        ranges[shards[i % len(shards)]].append((FIELDS, lower, upper))
    return [ShardPartition(s, tuple(r)) for s, r in ranges.items()]


@pytest.mark.parametrize("inner", [
    # як у основній колекції: межа на початку класу і всередині класу
    [("cardiac", MinKey()), ("neuro cns", "8")],
    # як у medicines_bench: межі всередині класів
    [("anti infectives", "29d"), ("anti infectives", "888"), ("anti infectives", "e75"),
     ("cardiac", "653"), ("derma", "f74"), ("gastro intestinal", "9a0"), ("hormones", "7d9"),
     ("neuro cns", "bfc"), ("pain analgesics", "0b8"), ("pain analgesics", "ae8"),
     ("respiratory", "70f")],
])
def test_every_document_belongs_to_exactly_one_shard(inner):
    bounds = [(MinKey(), MinKey())] + inner + [(MaxKey(), MaxKey())]
    partitions = _partitions(bounds, ["shard1rs", "shard2rs", "shard3rs"])
    filters = {p.shard: Adapter.partition_filter(p) for p in partitions}

    rng = random.Random(7)
    classes = ["anti diabetic", "anti infectives", "cardiac", "derma", "gastro intestinal",
               "hormones", "neuro cns", "pain analgesics", "respiratory", "vaccines"]
    docs = [{"therapeutic_class": c, "medicine_id": f"{rng.getrandbits(32):08x}"}
            for c in classes for _ in range(200)]
    docs += [{"therapeutic_class": c, "medicine_id": i} for c, i in inner]   # точно на межах

    for doc in docs:
        owners = [s for s, f in filters.items() if _matches(doc, f)]
        assert len(owners) == 1, (doc, owners)


def test_boundary_documents_go_to_upper_range():
    partitions = _partitions([(MinKey(), MinKey()), ("neuro cns", "8"), (MaxKey(), MaxKey())],
                             ["shard1rs", "shard2rs"])
    low, high = (Adapter.partition_filter(p) for p in partitions)
    doc = {"therapeutic_class": "neuro cns", "medicine_id": "8"}
    assert not _matches(doc, low) and _matches(doc, high)


def test_unsharded_partition_has_no_range_condition():
    assert Adapter.partition_filter(ShardPartition(UNSHARDED)) == {}


def test_pipeline_validates_group_and_filters():
    pipeline = Adapter.group_pipeline({"in_stock": True}, "manufacturer")
    assert pipeline[0] == {"$match": {"in_stock": True}}
    assert pipeline[1]["$group"]["_id"] == "$manufacturer"
    assert set(pipeline[1]["$group"]) >= {"count", "price_sum", "price_min", "price_max"}
    with pytest.raises(ValueError):
        Adapter.group_pipeline({}, "price")
    with pytest.raises(ValueError):
        Adapter._filters({"name": "x"})


# ------------------------------------------------------------ обробник і маршрут

def _fake_analytics(partitions, partials, native_groups):
    by_shard = {p.shard: p for p in partials}
    return SimpleNamespace(
        partitions=AsyncMock(return_value=partitions),
        map_partition=AsyncMock(side_effect=lambda p, g, f: by_shard[p.shard]),
        aggregate=AsyncMock(return_value=NativeAggregation(
            groups=native_groups, elapsed_ms=5.0, shards=["shard1rs", "shard2rs"],
            merger="mongos", shards_part=["$match", "$group"], merger_part=["$mergeCursors", "$group"])),
    )


async def test_handler_runs_map_on_every_shard_and_compares_with_native():
    partials = [ShardPartial("shard1rs", [part("Medisolve", [100])], 3.0, ["shard1rs"]),
                ShardPartial("shard2rs", [part("Medisolve", [200, 200, 200])], 4.0, ["shard2rs"])]
    partitions = [ShardPartition("shard1rs"), ShardPartition("shard2rs")]
    native = StatsReducer().reduce([ShardPartial("native", [part("Medisolve", [100, 200, 200, 200])])])
    analytics = _fake_analytics(partitions, partials, native)

    result = await GetShardStatsHandler(analytics).handle(ShardStatsQuery(in_stock=True))

    assert analytics.map_partition.await_count == 2
    analytics.map_partition.assert_any_await(partitions[0], "manufacturer", {"in_stock": True})
    assert result.sharded and result.total_documents == 4 and result.matches_native
    assert result.groups[0].avg_price == 175.0 and result.groups[0].shards == ["shard1rs", "shard2rs"]
    assert [s.documents for s in result.shards] == [1, 3]
    assert result.native.merger_part == ["$mergeCursors", "$group"]


async def test_handler_reports_unsharded_collection():
    partials = [ShardPartial(UNSHARDED, [part("Medisolve", [100])])]
    analytics = _fake_analytics([ShardPartition(UNSHARDED)], partials,
                                StatsReducer().reduce(partials))
    result = await GetShardStatsHandler(analytics).handle(ShardStatsQuery())
    assert not result.sharded and result.matches_native


async def test_handler_flags_mismatch():
    partials = [ShardPartial("shard1rs", [part("Medisolve", [100])])]
    analytics = _fake_analytics([ShardPartition("shard1rs")], partials,
                                StatsReducer().reduce([ShardPartial("n", [part("Medisolve", [999])])]))
    result = await GetShardStatsHandler(analytics).handle(ShardStatsQuery())
    assert not result.matches_native


def test_query_accepts_only_known_groupings():
    assert ShardStatsQuery(group_by="therapeutic_class").group_by == "therapeutic_class"
    with pytest.raises(ValidationError):
        ShardStatsQuery(group_by="price")


async def test_controller_route_and_parameters():
    handler = SimpleNamespace(handle=AsyncMock(return_value="ok"))
    controller = AnalyticsController(handler)
    assert "/api/analytics/stats" in {route.path for route in controller.router.routes}

    assert await controller.stats(group_by="therapeutic_class", in_stock=None,
                                  is_prescription=True) == "ok"
    query = handler.handle.await_args.args[0]
    assert query.group_by == "therapeutic_class" and query.is_prescription is True
