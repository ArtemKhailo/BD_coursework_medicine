"""Модульні тести прикладного сценарію повнотекстового пошуку."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from app.application.dto.search import SearchItem, SearchQuery, SearchResult
from app.application.queries.search_medicines_handler import SearchMedicinesHandler
from app.domain.entities.medicine import Medicine
from app.domain.entities.search_document import SearchHit
from app.domain.services.query_normalizer import QueryNormalizer


def make_medicine(name="test medicine", price=100.0, prescription=False):
    return Medicine(medicine_id=uuid4(), source_id=1, name=name, therapeutic_class="analgesics", is_habit_forming=False, is_prescription=prescription, manufacturer="Test Labs", price=price,)


def make_hit(medicine, score=10.0):
    return SearchHit(medicine.medicine_id, score, ["<em>test</em> medicine"])


@pytest.fixture
def env():
    cache = SimpleNamespace(get=AsyncMock(return_value=None), set=AsyncMock())
    documents = SimpleNamespace(by_ids=AsyncMock(return_value=[]))
    metrics = SimpleNamespace(record_query=AsyncMock())
    index = SimpleNamespace(search_batches=MagicMock())
    handler = SearchMedicinesHandler(index, cache, documents, metrics, QueryNormalizer(), cache_ttl_seconds=60)
    return SimpleNamespace(handler=handler, cache=cache, documents=documents,
                           metrics=metrics, index=index)


def batches(env, *pages):
    async def generate(*args, **kwargs):
        for page in pages:
            yield page
    env.index.search_batches.side_effect = generate


async def test_cache_miss_search_and_save(env):
    medicine = make_medicine()
    batches(env, [make_hit(medicine)])
    env.documents.by_ids.return_value = [medicine]
    result = await env.handler.handle(SearchQuery(raw=" TEST Medicine ", limit=1))
    assert result.cache_hit is False
    assert result.total == 1
    assert result.items[0].medicine_id == medicine.medicine_id
    assert result.items[0].snippets == ["<em>test</em> medicine"]
    assert env.index.search_batches.call_args.args[0] == "medicine test"
    env.documents.by_ids.assert_awaited_once_with([medicine.medicine_id])
    env.cache.set.assert_awaited_once()
    args = env.cache.set.call_args.args
    assert args[0] == env.cache.get.call_args.args[0]
    assert SearchResult.model_validate_json(args[1]).items == result.items
    assert env.cache.set.call_args.kwargs["ttl_seconds"] == 60
    env.metrics.record_query.assert_awaited_once()


async def test_cache_hit_skips_elasticsearch_and_mongo(env):
    medicine = make_medicine()
    item = SearchItem(medicine_id=medicine.medicine_id, name=medicine.name, therapeutic_class=medicine.therapeutic_class, is_prescription=False, price=medicine.price, score=10.0)
    cached = SearchResult(items=[item], total=1)
    env.cache.get.return_value = cached.model_dump_json()
    result = await env.handler.handle(SearchQuery(raw="test"))
    assert result.cache_hit is True
    assert result.items == cached.items
    env.index.search_batches.assert_not_called()
    env.documents.by_ids.assert_not_awaited()
    env.cache.set.assert_not_awaited()
    env.metrics.record_query.assert_awaited_once()


async def test_filters_continue_into_next_batch(env):
    expensive = make_medicine("expensive", 500.0)
    prescribed = make_medicine("prescribed", 80.0, True)
    suitable = make_medicine("suitable", 150.0)
    batches(env, [make_hit(expensive), make_hit(prescribed)], [make_hit(suitable)])
    env.documents.by_ids.side_effect = [[expensive, prescribed], [suitable]]
    result = await env.handler.handle(SearchQuery(raw="test", filters={"price_max": 150, "is_prescription": False}, limit=1))
    assert result.total == 1
    assert result.items[0].medicine_id == suitable.medicine_id
    assert env.documents.by_ids.await_count == 2


async def test_preserves_ranking_and_skips_missing_documents(env):
    first, second, missing = [make_medicine(name) for name in ("first", "second", "missing")]
    batches(env, [make_hit(first, 30), make_hit(missing, 20), make_hit(second, 10)])

async def test_no_matches_returns_empty_result(env):
    batches(env)

    result = await env.handler.handle(
        SearchQuery(raw="notfound")
    )

    assert result.total == 0
    assert result.items == []
    assert result.cache_hit is False

    env.documents.by_ids.assert_not_awaited()


async def test_redis_unavailable_does_not_break_search(env):
    medicine = make_medicine()

    batches(env, [make_hit(medicine)])

    env.documents.by_ids.return_value = [medicine]

    env.cache.get.side_effect = TimeoutError("Redis unavailable")
    env.cache.set.side_effect = TimeoutError("Redis unavailable")

    result = await env.handler.handle(SearchQuery(raw="test"))

    assert result.total == 1
    assert result.cache_hit is False

    env.metrics.record_query.assert_awaited_once()


async def test_metrics_failure_does_not_break_search(env):
    medicine = make_medicine()

    batches(env, [make_hit(medicine)])

    env.documents.by_ids.return_value = [medicine]

    env.metrics.record_query.side_effect = RuntimeError("Metrics unavailable")

    result = await env.handler.handle(SearchQuery(raw="test"))

    assert result.total == 1


def test_cache_key_includes_filters_and_limit(env):
    key = env.handler._cache_key

    assert key("test",{"price_max": 200, "is_prescription": False}, 10) == key("test", {"is_prescription": False, "price_max": 200}, 10)

    assert key("test", {}, 10) != key("test", {}, 20)

    assert key("test", {}, 10) != key("test", {"is_prescription": False}, 10)

    assert key("test", {}, 10) != key("other", {}, 10)