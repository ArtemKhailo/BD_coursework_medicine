"""Композиційний корінь: єдине місце, що знає і порти, і їхні реалізації.

Створює клієнти БД, адаптери та підставляє їх у обробники (інверсія залежностей).
Решта коду працює лише з інтерфейсами.
"""
from __future__ import annotations

from pymongo import AsyncMongoClient

from app.application.commands import (
    ConfigureRateHandler, CreateMedicineHandler, DeleteMedicineHandler,
    IngestDocumentHandler, UpdateMedicineHandler,
)
from app.application.queries import (
    GetAnaloguesHandler, GetMedicineCardHandler, GetMedicineFacetsHandler, GetShardStatsHandler,
    GetStatsHandler, ListMedicinesHandler, SearchMedicinesHandler,
)
from app.domain.services.stats_reducer import StatsReducer
from app.infrastructure.settings import Settings
from app.infrastructure.messaging import QueueAdapter
from app.infrastructure.observability import MongoMetricsAdapter
from app.infrastructure.persistence import (
    MongoDocumentAdapter, MongoShardAnalyticsAdapter, Neo4jGraphAdapter, RedisCacheAdapter,
    SearchIndexAdapter,
)


class Container:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        # Клієнт MongoDB не відкриває з'єднання в конструкторі (лише при першому запиті),
        # тому адаптер можна створити одразу, ще до запуску застосунку.
        self._mongo_client = AsyncMongoClient(settings.mongo.uri, serverSelectionTimeoutMS=3000)
        self.documents: MongoDocumentAdapter = MongoDocumentAdapter(
            self._mongo_client, settings.mongo.database,
        )
        self.shard_analytics: MongoShardAnalyticsAdapter = MongoShardAnalyticsAdapter(
            self._mongo_client, settings.mongo.database,
        )
        self.search_index: SearchIndexAdapter | None = None
        self.graph: Neo4jGraphAdapter | None = None
        self.cache: RedisCacheAdapter | None = None
        self.metrics: MongoMetricsAdapter | None = None
        self.queue: QueueAdapter | None = None

    async def start(self) -> None:
        """Підключення решти сховищ (ES, Neo4j, Redis, RabbitMQ) - на наступних етапах.

        MongoDB уже готова: адаптер створено в конструкторі, а індекси й шардінг
        налаштовує scripts/load_mongo.py під час завантаження даних.
        """

    async def close(self) -> None:
        """Закриває всі з'єднання."""
        await self._mongo_client.close()

    # --- фабрики обробників (шлях запитів) ---
    def search_medicines_handler(self) -> SearchMedicinesHandler:
        raise NotImplementedError

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

    # --- фабрики обробників (шлях команд) ---
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
