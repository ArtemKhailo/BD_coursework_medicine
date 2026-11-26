"""Інтеграційні тести MongoDB (вимоги методички, п. «Тестування»).

Потребують запущеного кластера (docker/docker-compose.mongo.yml). Якщо mongos недоступний,
тести пропускаються. Працюють в окремій базі medicine_test, основні дані не чіпають.

Запуск:  python -m pytest tests/integration -v
"""
from __future__ import annotations

import os
from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("pymongo")
from pymongo import AsyncMongoClient  # noqa: E402

from app.domain.entities.medicine import Medicine  # noqa: E402
from app.domain.errors import MedicineAlreadyExistsError  # noqa: E402
from app.infrastructure.observability.mongo_metrics_adapter import MongoMetricsAdapter  # noqa: E402
from app.infrastructure.persistence.mongo_document_adapter import MongoDocumentAdapter  # noqa: E402

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
TEST_DB = "medicine_test"


@pytest.fixture
async def client():
    client = AsyncMongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    try:
        await client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001
        await client.close()
        pytest.skip(f"MongoDB недоступна ({MONGO_URI}): {exc}")
    yield client
    await client.close()


@pytest.fixture
async def repo(client):
    adapter = MongoDocumentAdapter(client, TEST_DB)
    await adapter.ensure_sharding()
    await adapter.ensure_indexes()
    return adapter


@pytest.fixture
def marker() -> str:
    """Унікальний маркер, щоб тести не заважали один одному і повторним запускам."""
    return f"it-marker-{uuid4().hex[:12]}"


@pytest.fixture
async def cleanup(client, marker):
    yield
    await client[TEST_DB]["medicines"].delete_many({"name": {"$regex": f"^{marker}"}})
    await client[TEST_DB]["medicines"].delete_many({"chemical_class": marker})


def make(marker: str, suffix: str = "", **changes: Any) -> Medicine:
    data = dict(medicine_id=uuid4(), source_id=0, name=f"{marker}{suffix}",
                therapeutic_class="cardiac", is_habit_forming=False, is_prescription=False,
                manufacturer="Test Labs", price=100.0)
    data.update(changes)
    return Medicine(**data)


def _shard_hits(plan: dict[str, Any]) -> dict[str, int]:
    """nReturned кожного шарда з explain('executionStats') через mongos."""
    stages = plan.get("executionStats", {}).get("executionStages", {})
    hits = {}
    for shard in stages.get("shards", []):
        n = shard.get("nReturned")
        if n is None:
            n = shard.get("executionStages", {}).get("nReturned", 0)
        hits[shard.get("shardName", "?")] = int(n)
    return hits


async def test_write_goes_to_exactly_one_shard_without_loss(client, repo, marker, cleanup):
    """Запис: об'єкт серіалізується, проходить через ключ шардінгу і лежить рівно на одному шарді."""
    medicine = make(marker, therapeutic_class="vitamins minerals nutrients", uses=["fever"], in_stock=False)
    await repo.create(medicine)

    stored = await repo.get_by_id(medicine.medicine_id)
    assert stored == medicine                                   # нічого не втрачено при серіалізації

    collection = client[TEST_DB]["medicines"]
    full_key = {"therapeutic_class": "vitamins minerals nutrients", "medicine_id": str(medicine.medicine_id)}
    targeted = await collection.find(full_key).explain()
    assert targeted["queryPlanner"]["winningPlan"]["stage"] == "SINGLE_SHARD"
    target_shard = targeted["queryPlanner"]["winningPlan"]["shards"][0]["shardName"]

    hits = _shard_hits(await collection.find({"_id": str(medicine.medicine_id)}).explain())
    if hits:   # запит лише за _id іде на всі шарди, документ знаходиться лише на одному
        assert [s for s, n in hits.items() if n == 1] == [target_shard]


async def test_query_by_class_targets_one_shard(client, repo):
    """Межа діапазону стоїть на початку класу cardiac, тому весь клас - на одному шарді."""
    plan = await client[TEST_DB]["medicines"].find({"therapeutic_class": "cardiac"}).explain()
    assert plan["queryPlanner"]["winningPlan"]["stage"] == "SINGLE_SHARD"


async def test_duplicate_create_is_rejected(repo, marker, cleanup):
    medicine = make(marker)
    await repo.create(medicine)
    with pytest.raises(MedicineAlreadyExistsError):
        await repo.create(medicine)


async def test_bulk_insert_is_idempotent(repo, marker, cleanup):
    batch = [make(marker, f"-{i}", therapeutic_class=c)
             for i, c in enumerate(["anti diabetic", "cardiac", "respiratory"] * 3)]
    assert await repo.create_many(batch) == 9
    assert await repo.create_many(batch) == 0                   # повтор пакета не створює дублів
    assert await repo.count({"name": f"{marker}-0"}) == 1
    assert len(await repo.by_ids([m.medicine_id for m in batch])) == 9


async def test_search_positive_by_unique_marker(repo, marker, cleanup):
    medicine = make(marker, price=77.0, in_stock=True)
    await repo.create(medicine)
    found = await repo.find({"name": marker, "price_max": 80, "in_stock": True})
    assert [m.medicine_id for m in found] == [medicine.medicine_id]


async def test_search_negative_returns_empty_not_error(repo, marker):
    assert await repo.find({"name": f"{marker}-does-not-exist"}) == []
    assert await repo.count({"name": f"{marker}-does-not-exist"}) == 0
    assert await repo.get_by_id(uuid4()) is None
    assert await repo.by_ids([uuid4()]) == []


async def test_update_including_shard_key_and_delete(repo, marker, cleanup):
    medicine = make(marker)
    await repo.create(medicine)
    updated = await repo.update(medicine.medicine_id, {"price": 55.5, "therapeutic_class": "derma"})
    assert updated.price == 55.5 and updated.therapeutic_class == "derma"
    assert (await repo.get_by_id(medicine.medicine_id)).therapeutic_class == "derma"
    assert await repo.update(uuid4(), {"price": 60.0}) is None
    assert await repo.delete(medicine.medicine_id) is True
    assert await repo.delete(medicine.medicine_id) is False


async def test_top_side_effects_skips_placeholders(repo, marker, cleanup):
    await repo.create_many([
        make(marker, "-a", chemical_class=marker, side_effects=["nausea", "no common side effects seen"]),
        make(marker, "-b", chemical_class=marker, side_effects=["nausea", "rash"]),
    ])
    assert await repo.top_side_effects(marker, limit=5) == [("nausea", 2), ("rash", 1)]


async def test_name_prefix_search_uses_name_index(client, repo, marker, cleanup):
    await repo.create_many([make(marker, f"-{i} tablet") for i in range(3)])
    found = await repo.find({"name_prefix": marker.upper()})      # регістр не важливий
    assert len(found) == 3 and all(m.name.startswith(marker) for m in found)
    assert await repo.count({"name_prefix": f"{marker}-1"}) == 1

    flt = MongoDocumentAdapter._build_filter({"name_prefix": marker})
    plan = await client[TEST_DB]["medicines"].find(flt).explain()
    assert "COLLSCAN" not in str(plan["queryPlanner"]["winningPlan"])
    assert "'indexName': 'name'" in str(plan["queryPlanner"]["winningPlan"])


async def test_facets_counts_classes_and_prices(client, repo, marker, cleanup):
    await repo.create_many([
        make(marker, "-a", chemical_class=marker, price=61.0),
        make(marker, "-b", chemical_class=marker, price=62.0),
        make(marker, "-c", chemical_class=marker, therapeutic_class="derma", price=63.0),
    ])

    everything = await repo.facets()
    therapeutic = {f.value for f in everything.therapeutic_classes}
    assert {"cardiac", "derma"} <= therapeutic
    assert (marker, 3) in [(f.value, f.count) for f in everything.chemical_classes]

    cardiac = await repo.facets("cardiac")
    assert (marker, 2) in [(f.value, f.count) for f in cardiac.chemical_classes]
    assert cardiac.price_min <= 61.0 and cardiac.price_max >= 62.0
    assert {f.value for f in cardiac.therapeutic_classes} == therapeutic   # список класів повний

    # агрегація з префіксом ключа шардінгу йде лише на один шард
    plan = await client[TEST_DB].command(
        "aggregate", "medicines",
        pipeline=[{"$match": {"therapeutic_class": "cardiac"}},
                  {"$group": {"_id": "$chemical_class", "count": {"$sum": 1}}}],
        explain=True,
    )
    if "shards" in plan:
        assert len(plan["shards"]) == 1


async def test_search_log_roundtrip(client):
    metrics = MongoMetricsAdapter(client, TEST_DB)
    await metrics.ensure_indexes()
    raw = f"Query {uuid4().hex[:8]}"
    await metrics.record_query(raw, raw.lower(), results_count=0, cache_hit=False, latency_ms=3)
    latest = await metrics.recent_queries(1)
    assert latest[0].raw_query == raw and latest[0].status == "Пусто"
