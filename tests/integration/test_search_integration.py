"""Інтеграційні тести етапу 15. Лише локальні Docker-сервіси."""
import asyncio
from uuid import uuid4

import pytest
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pymongo import AsyncMongoClient
from redis.asyncio import Redis

from app.application.queries.search_medicines_handler import SearchMedicinesHandler
from app.domain.entities.medicine import Medicine
from app.domain.entities.search_document import SearchDocument
from app.domain.services.query_normalizer import QueryNormalizer
from app.infrastructure.observability.mongo_metrics_adapter import MongoMetricsAdapter
from app.infrastructure.persistence.mongo_document_adapter import MongoDocumentAdapter
from app.infrastructure.persistence.redis_cache_adapter import RedisCacheAdapter
from app.infrastructure.persistence.search_index_adapter import SearchIndexAdapter
from app.presentation.rest.search_controller import SearchController


@pytest.fixture
async def elastic():
    name = f"medicine_it_{uuid4().hex}"
    async with AsyncElasticsearch("http://127.0.0.1:9200", request_timeout=30) as client:
        adapter = SearchIndexAdapter(client, name)
        try:
            await adapter.ensure_index()
            yield client, adapter, name
        finally:
            await client.options(ignore_status=404).indices.delete(index=name)


@pytest.fixture
async def redis_client():
    client = Redis.from_url("redis://127.0.0.1:6379", decode_responses=True,
                            socket_connect_timeout=5, socket_timeout=5)
    try:
        assert await client.ping()
        yield client
    finally:
        await client.aclose()


@pytest.fixture
async def mongo():
    name = f"medicine_it_{uuid4().hex}"
    client = AsyncMongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
    try:
        await client.admin.command("ping")
        try:
            yield client, name
        finally:
            await client.drop_database(name)
    finally:
        await client.close()


async def test_elastic_exact_and_fuzzy(elastic):
    client, adapter, name = elastic
    doc = SearchDocument(uuid4(), "paracetamol", "test description")
    assert await adapter.index_many([doc]) == 1
    await client.indices.refresh(index=name)

    exact = await adapter.search("paracetamol", limit=1)
    assert [hit.medicine_id for hit in exact] == [doc.medicine_id]
    assert any("<em>" in text for text in exact[0].snippets)
    fuzzy = await adapter.search("paracetmol", limit=1)
    assert [hit.medicine_id for hit in fuzzy] == [doc.medicine_id]


async def test_elastic_batches_and_delete(elastic):
    client, adapter, name = elastic
    docs = [SearchDocument(uuid4(), f"sample {i}", "headache") for i in range(5)]
    assert await adapter.index_many(docs) == 5
    await client.indices.refresh(index=name)
    batches = [batch async for batch in adapter.search_batches("headache", batch_size=2)]
    assert [len(batch) for batch in batches] == [2, 2, 1]
    ids = [hit.medicine_id for batch in batches for hit in batch]
    assert len(ids) == len(set(ids)) == 5
    assert set(ids) == {doc.medicine_id for doc in docs}

    await adapter.delete(docs[0].medicine_id)
    await client.indices.refresh(index=name)
    found = await adapter.search("headache", limit=10)
    assert {hit.medicine_id for hit in found} == set(ids) - {docs[0].medicine_id}
    assert await adapter.search("zzzxqwertyyy") == []


async def test_redis_set_get_delete(redis_client):
    adapter = RedisCacheAdapter(redis_client)
    key = f"medicine:it:{uuid4().hex}"
    try:
        assert await adapter.get(key) is None
        await adapter.set(key, '{"items": []}', ttl_seconds=60)
        assert await adapter.get(key) == '{"items": []}'
        assert 0 < await redis_client.ttl(key) <= 60
        await adapter.delete(key)
        assert await adapter.get(key) is None
    finally:
        await redis_client.delete(key)


async def test_redis_ttl_expiration(redis_client):
    adapter = RedisCacheAdapter(redis_client)
    key = f"medicine:it:{uuid4().hex}"
    try:
        await adapter.set(key, "temporary", ttl_seconds=3)
        assert await adapter.get(key) == "temporary"
        # Опитування замість вимоги завершення TTL точно в задану мілісекунду.
        for _ in range(40):
            if await adapter.get(key) is None:
                break
            await asyncio.sleep(0.25)
        assert await adapter.get(key) is None
        assert await redis_client.ttl(key) == -2
    finally:
        await redis_client.delete(key)


async def test_search_api_miss_hit_and_filters(elastic, redis_client, mongo):
    es_client, index, index_name = elastic
    mongo_client, db_name = mongo
    documents = MongoDocumentAdapter(mongo_client, db_name)
    metrics = MongoMetricsAdapter(mongo_client, db_name)
    cache = RedisCacheAdapter(redis_client)
    normalizer = QueryNormalizer()
    handler = SearchMedicinesHandler(index, cache, documents, metrics, normalizer,
                                     cache_ttl_seconds=60)
    app = FastAPI()
    app.include_router(SearchController(search_medicines=handler).router)

    # Унікальний запит: не зачіпає ключі звичайного застосунку.
    marker = f"medtest{uuid4().hex}"
    filters = {"is_prescription": False, "price_max": 150.0}
    key = handler._cache_key(normalizer.normalize(marker), filters, 10)
    medicines = []
    for suffix, price, prescription in [("a", 100.0, False), ("b", 300.0, False),
                                        ("c", 80.0, True)]:
        medicine = Medicine(medicine_id=uuid4(), source_id=0,
                            name=f"{marker} {suffix}", therapeutic_class="analgesics",
                            is_habit_forming=False, is_prescription=prescription,
                            manufacturer="Test Labs", price=price)
        await documents.create(medicine)
        medicines.append(medicine)
    assert await index.index_many([
        SearchDocument(m.medicine_id, m.name, marker) for m in medicines
    ]) == 3
    await es_client.indices.refresh(index=index_name)
    params = {"q": marker, "price_max": 150, "is_prescription": "false", "limit": 10}
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as http:
            first = await http.get("/api/search", params=params)
            second = await http.get("/api/search", params=params)
            assert first.status_code == second.status_code == 200
            miss, hit = first.json(), second.json()
            assert miss["cache_hit"] is False
            assert hit["cache_hit"] is True
            assert miss["total"] == hit["total"] == 1
            assert miss["items"] == hit["items"]
            assert miss["items"][0]["medicine_id"] == str(medicines[0].medicine_id)
            assert miss["items"][0]["price"] == 100.0
            assert miss["items"][0]["is_prescription"] is False
            assert await redis_client.exists(key) == 1
        logs = await mongo_client[db_name]["search_log"].find({"raw_query": marker}).to_list(length=None)
        assert len(logs) == 2
        assert sorted(log["cache_hit"] for log in logs) == [False, True]
        assert all(log["results_count"] == 1 for log in logs)
    finally:
        await redis_client.delete(key)