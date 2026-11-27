"""Модульні тести адаптера Elasticsearch без реального індексу."""
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.infrastructure.persistence.search_index_adapter import SearchIndexAdapter


def make_hit(score=10.0):
    medicine_id = str(uuid4())
    return {
        "_source": {"medicine_id": medicine_id}, "_score": score,
        "sort": [score, medicine_id],
        "highlight": {"name": ["<em>test</em>"], "full_description": ["test description"]},
    }


def response(*hits):
    return {"timed_out": False, "hits": {"hits": list(hits)}}


@pytest.fixture
def client():
    return SimpleNamespace(search=AsyncMock())


async def test_exact_search_first(client):
    hit = make_hit()
    client.search.return_value = response(hit)
    result = await SearchIndexAdapter(client, "test_index").search("test", limit=1)
    assert len(result) == 1
    assert str(result[0].medicine_id) == hit["_source"]["medicine_id"]
    assert result[0].score == 10.0
    assert result[0].snippets == ["<em>test</em>", "test description"]
    client.search.assert_awaited_once()
    query = client.search.call_args.kwargs["query"]
    assert "fuzziness" not in query["multi_match"]


async def test_fuzzy_search_when_exact_is_empty(client):
    hit = make_hit()
    client.search.side_effect = [response(), response(hit)]
    result = await SearchIndexAdapter(client).search("tset", limit=1)
    assert len(result) == 1
    assert client.search.await_count == 2
    exact = client.search.call_args_list[0].kwargs["query"]
    fuzzy = client.search.call_args_list[1].kwargs["query"]["bool"]
    assert fuzzy["must"][0]["multi_match"]["fuzziness"] == "AUTO"
    assert fuzzy["must_not"] == [exact]


async def test_search_after_uses_last_sort_value(client):
    first, second = make_hit(20), make_hit(10)
    client.search.side_effect = [response(first), response(second), response(), response()]
    adapter = SearchIndexAdapter(client)
    pages = [page async for page in adapter.search_batches("test", batch_size=1)]
    assert len(pages) == 2
    assert [str(p[0].medicine_id) for p in pages] == [first["_source"]["medicine_id"], second["_source"]["medicine_id"]]
    calls = client.search.call_args_list
    assert "search_after" not in calls[0].kwargs
    assert calls[1].kwargs["search_after"] == first["sort"]
    assert calls[2].kwargs["search_after"] == second["sort"]
    assert "search_after" not in calls[3].kwargs


async def test_empty_query_does_not_call_server(client):
    assert await SearchIndexAdapter(client).search("  ") == []
    client.search.assert_not_awaited()


async def test_timeout_is_not_treated_as_empty_result(client):
    client.search.return_value = {"timed_out": True, "hits": {"hits": []}}
    with pytest.raises(TimeoutError):
        await SearchIndexAdapter(client).search("test")