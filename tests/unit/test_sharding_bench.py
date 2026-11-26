"""Етап 13: допоміжні функції бенчмарку шардінгу (без запущеної БД)."""
import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "bench_sharding.py"
spec = importlib.util.spec_from_file_location("bench_sharding", SCRIPT)
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def test_copy_name_keeps_original_and_marks_copies():
    assert bench.copy_name("augmentin 625 duo tablet", 0) == "augmentin 625 duo tablet"
    assert bench.copy_name("augmentin 625 duo tablet", 3) == "augmentin 625 duo tablet #3"


def test_split_points_give_equal_chunks():
    keys = sorted((cls, f"{i:04d}") for cls in ("a", "b", "c") for i in range(400))
    points = bench.split_points(keys, 12)
    assert len(points) == 11
    bounds = [None] + [tuple(p) for p in points] + [None]
    sizes = [sum(1 for k in keys if (lo is None or k >= lo) and (hi is None or k < hi))
             for lo, hi in zip(bounds, bounds[1:])]
    assert sizes == [100] * 12                       # рівні за кількістю документів
    assert [tuple(p) for p in points] == sorted(tuple(p) for p in points)


def test_chunks_are_spread_round_robin_four_per_shard():
    shards = ["shard1rs", "shard2rs", "shard3rs"]
    targets = bench.chunk_targets(12, shards)
    assert {s: targets.count(s) for s in shards} == {s: 4 for s in shards}
    assert all(a != b for a, b in zip(targets, targets[1:]))   # сусідні чанки на різних шардах
    assert set(bench.chunk_targets(4, ["shard1rs"])) == {"shard1rs"}


@pytest.mark.parametrize("writers", [1, 4, 8])
def test_every_data_file_goes_to_exactly_one_writer(writers):
    parts = bench.assign_parts(8, writers)
    assert len(parts) == writers
    assert sorted(p for group in parts for p in group) == list(range(8))


def test_too_many_writers_rejected():
    with pytest.raises(ValueError):
        bench.assign_parts(8, 9)


def test_quota_is_split_without_loss():
    assert bench.split_quota(10, 4) == [3, 3, 2, 2]
    assert bench.split_quota(None, 2) == [None, None]


def test_percentile_nearest_rank():
    values = list(range(1, 101))
    assert bench.percentile(values, 50) == 50
    assert bench.percentile(values, 95) == 95
    assert bench.percentile([], 95) == 0.0


def test_iter_bson_splits_concatenated_documents():
    def fake_doc(payload: bytes) -> bytes:
        return (len(payload) + 5).to_bytes(4, "little") + payload + b"\x00"
    docs = [fake_doc(b"a"), fake_doc(b"bbbb"), fake_doc(b"")]
    assert list(bench.iter_bson(b"".join(docs))) == docs
    with pytest.raises(ValueError):
        list(bench.iter_bson(docs[0][:-2]))


def test_summary_takes_median_and_speedup_against_one_shard():
    rows = [
        {"shards": 1, "writers": 4, "batch": 5000, "docs": 100, "docs_per_sec": v,
         "batch_p50_ms": 10, "batch_p95_ms": 20} for v in (100, 120, 300)
    ] + [
        {"shards": 3, "writers": 4, "batch": 5000, "docs": 100, "docs_per_sec": v,
         "batch_p50_ms": 5, "batch_p95_ms": 9} for v in (200, 240, 250)
    ]
    summary = {s["shards"]: s for s in bench.summarize(rows)}
    assert summary[1]["docs_per_sec"] == 120 and summary[1]["runs"] == 3
    assert summary[3]["docs_per_sec"] == 240
    assert summary[3]["speedup"] == 2.0
