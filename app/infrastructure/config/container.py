"""Композиційний корінь: єдине місце, що знає і порти, і їхні реалізації.

Створює клієнти БД, адаптери та підставляє їх у обробники (інверсія залежностей).
Решта коду працює лише з інтерфейсами.
"""

from __future__ import annotations

from pymongo import AsyncMongoClient
from elasticsearch import AsyncElasticsearch
from redis.asyncio import Redis

from app.application.commands import (
    ConfigureRateHandler,
    CreateMedicineHandler,
    DeleteMedicineHandler,
    IngestDocumentHandler,
    UpdateMedicineHandler,
)

from app.application.queries import (
    GetAnaloguesHandler,
    GetMedicineCardHandler,
    GetMedicineFacetsHandler,
    GetShardStatsHandler,
    GetStatsHandler,
    ListMedicinesHandler,
    SearchMedicinesHandler,
)

from app.domain.services.query_normalizer import QueryNormalizer
from app.domain.services.stats_reducer import StatsReducer
from app.infrastructure.settings import Settings
from app.infrastructure.messaging import QueueAdapter
from app.infrastructure.observability import MongoMetricsAdapter

from app.infrastructure.persistence import (
    MongoDocumentAdapter,
    MongoShardAnalyticsAdapter,
    Neo4jGraphAdapter,
    RedisCacheAdapter,
    SearchIndexAdapter,
)

class Container:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

        # MongoDB
        self._mongo_client = AsyncMongoClient(settings.mongo.uri, serverSelectionTimeoutMS=3000)

        self.documents: MongoDocumentAdapter = MongoDocumentAdapter(self._mongo_client, settings.mongo.database,)

        # Аналітика по шардах
        self.shard_analytics: MongoShardAnalyticsAdapter = (MongoShardAnalyticsAdapter(self._mongo_client,settings.mongo.database,))

        # Elasticsearch
        self._elastic_client = AsyncElasticsearch(settings.elastic.url,request_timeout=30)

        self.search_index = SearchIndexAdapter(self._elastic_client, settings.elastic.index)

        # Redis
        self._redis_client = Redis.from_url(settings.redis.url, decode_responses=True, socket_connect_timeout=2, socket_timeout=2)

        self.cache = RedisCacheAdapter(self._redis_client)

        # Метрики пошукових запитів
        self.metrics = MongoMetricsAdapter(self._mongo_client, settings.mongo.database)

        # Компоненти, які ще не реалізовано
        self.graph: Neo4jGraphAdapter | None = None
        self.queue: QueueAdapter | None = None

    async def start(self) -> None:
        """Підготовка застосунку до роботи.

        MongoDB та Elasticsearch наповнюються окремими
        скриптами завантаження даних.

        Клієнти відкривають з'єднання під час звернення.
        """

    async def close(self) -> None:
        """Закриває всі з'єднання зі сховищами."""

        try:
            await self._elastic_client.close()
        finally:
            try:
                await self._redis_client.aclose()
            finally:
                await self._mongo_client.close()

    # Фабрики обробників (шлях запитів)

    def search_medicines_handler(self) -> SearchMedicinesHandler:
        return SearchMedicinesHandler(
            search_index=self.search_index,
            cache=self.cache,
            documents=self.documents,
            metrics=self.metrics,
            normalizer=QueryNormalizer(),
            cache_ttl_seconds=self.settings.redis.search_ttl_seconds
        )

    def get_medicine_card_handler(self) -> GetMedicineCardHandler:
        return GetMedicineCardHandler(self.documents, cache=self.cache)

    def get_medicine_facets_handler(self) -> GetMedicineFacetsHandler:
        return GetMedicineFacetsHandler(self.documents)

    def get_analogues_handler(self) -> GetAnaloguesHandler:
        raise NotImplementedError

    def list_medicines_handler(self) -> ListMedicinesHandler:
        return ListMedicinesHandler(self.documents)

    def get_shard_stats_handler(self) -> GetShardStatsHandler:
        return GetShardStatsHandler(self.shard_analytics, StatsReducer())

    def get_stats_handler(self) -> GetStatsHandler:
        raise NotImplementedError

    # Фабрики обробників (шлях команд)

    def ingest_document_handler(self) -> IngestDocumentHandler:
        raise NotImplementedError

    def configure_rate_handler(self) -> ConfigureRateHandler:
        raise NotImplementedError

    def create_medicine_handler(self) -> CreateMedicineHandler:
        raise NotImplementedError

    def update_medicine_handler(self) -> UpdateMedicineHandler:
        raise NotImplementedError

    def delete_medicine_handler(self) -> DeleteMedicineHandler:
        raise NotImplementedError