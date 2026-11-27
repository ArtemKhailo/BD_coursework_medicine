"""Адаптер Redis для CacheStore (кеш search:<ключ> і конфігурація config:rate)."""
from __future__ import annotations

from typing import TYPE_CHECKING

from app.domain.ports.outbound.cache_store import CacheStore

if TYPE_CHECKING:
    from redis.asyncio import Redis


class RedisCacheAdapter(CacheStore):
    def __init__(self, client: Redis) -> None:
        self._client = client

    async def get(self, key: str) -> str | None:
        """Отримання значення за ключем."""

        value = await self._client.get(key)

        if value is None:
            return None

        if isinstance(value, bytes):
            return value.decode("utf-8")

        return value

    async def set(self, key: str, value: str, ttl_seconds: int | None = None) -> None:
        """Збереження значення з необов'язковим TTL."""

        if ttl_seconds is not None:

            if ttl_seconds <= 0:
                raise ValueError("TTL має бути більшим за 0")

            await self._client.set(key, value, ex = ttl_seconds)

        else:
            await self._client.set(key, value)

    async def delete(self, key: str) -> None:
        """Видалення значення за ключем."""

        await self._client.delete(key)