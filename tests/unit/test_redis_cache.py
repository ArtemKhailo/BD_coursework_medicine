"""Модульні тести адаптера Redis без реального сервера."""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.infrastructure.persistence.redis_cache_adapter import RedisCacheAdapter


@pytest.fixture
def client():
    return SimpleNamespace(get=AsyncMock(), set=AsyncMock(), delete=AsyncMock())


@pytest.mark.parametrize("stored, expected", [(None, None), ("value", "value"), (b"value", "value")])
async def test_cache_get(client, stored, expected):
    client.get.return_value = stored
    adapter = RedisCacheAdapter(client)
    assert await adapter.get("test:key") == expected
    client.get.assert_awaited_once_with("test:key")


async def test_cache_set_with_ttl(client):
    adapter = RedisCacheAdapter(client)
    await adapter.set("test:key", "value", ttl_seconds=60)
    client.set.assert_awaited_once_with("test:key", "value", ex=60)


async def test_cache_delete(client):
    adapter = RedisCacheAdapter(client)
    await adapter.delete("test:key")
    client.delete.assert_awaited_once_with("test:key")