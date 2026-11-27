"""Сценарій «Пошук препаратів» (рис. 3.5): нормалізація -> кеш -> ES -> MongoDB -> кеш -> метрика."""
from __future__ import annotations

import hashlib
import json
import logging
import time

from app.application.dto.medicine import MedicineListQuery
from app.application.dto.search import SearchQuery, SearchResult, SearchItem
from app.application.ports.inbound.search_medicines import SearchMedicines
from app.domain.ports.outbound import CacheStore, DocumentRepository, MetricsSink, SearchIndex
from app.domain.services.query_normalizer import QueryNormalizer

logger = logging.getLogger(__name__)

# Верхня межа переглянутих збігів ES при пост-фільтрації (захист від повного проходу індексу).
MAX_SCANNED_HITS = 2000


class SearchMedicinesHandler(SearchMedicines):
    def __init__(self, search_index: SearchIndex, cache: CacheStore,
                 documents: DocumentRepository, metrics: MetricsSink,
                 normalizer: QueryNormalizer, cache_ttl_seconds: int = 60) -> None:
        self._search_index = search_index
        self._cache = cache
        self._documents = documents
        self._metrics = metrics
        self._normalizer = normalizer
        self._cache_ttl = cache_ttl_seconds

    @staticmethod
    def _cache_key(normalized_query: str, filters: dict, limit: int) -> str:
        """Формування стабільного ключа кешу."""

        payload = json.dumps(
            {
                "query": normalized_query,
                "filters": filters,
                "limit": limit
            },
            sort_keys = True,
            ensure_ascii = False
        )

        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()

        return f"search:v3:{digest}"

    async def _finish(
        self,result: SearchResult, raw_query: str, normalized_query: str, cache_hit: bool, started: float) -> SearchResult:
        """Формування метрик та завершення пошуку."""

        result.cache_hit = cache_hit

        result.latency_ms = round((time.perf_counter() - started) * 1000)

        try:
            await self._metrics.record_query(
                raw_query = raw_query,
                normalized_query = normalized_query,
                results_count = result.total,
                cache_hit = result.cache_hit,
                latency_ms = result.latency_ms
            )

        except Exception:
            logger.warning("Не вдалося записати метрики пошуку", exc_info = True)

        return result

    async def handle(self, query: SearchQuery) -> SearchResult:
        started = time.perf_counter()

        # Нормалізація пошукового запиту
        normalized = self._normalizer.normalize(query.raw)

        if not normalized:
            return await self._finish(SearchResult(), query.raw, normalized, False, started)

        # Перевірка підтримуваних фільтрів
        supported = {"is_prescription", "price_max"}

        if set(query.filters) - supported:
            raise ValueError("Непідтримувані фільтри пошуку")

        validated = MedicineListQuery(**query.filters)

        filters = validated.model_dump(include = supported, exclude_none = True)

        # Формування ключа кешу
        key = self._cache_key(normalized, filters, query.limit)

        # Перевірка Redis
        try:
            cached = await self._cache.get(key)

            if cached is not None:
                result = SearchResult.model_validate_json(cached)

                return await self._finish(result, query.raw, normalized, True, started)

        except Exception:
            logger.warning("Не вдалося прочитати кеш Redis", exc_info = True)



        # Пакетний пошук в Elasticsearch
        items = []

        batch_size = 200 if filters else query.limit
        scanned = 0

        async for hits in self._search_index.search_batches(normalized, batch_size=batch_size):

            # Отримання документів із MongoDB
            ids = [hit.medicine_id for hit in hits]

            medicines = await self._documents.by_ids(ids)
            scanned += len(hits)

            medicines_by_id = {medicine.medicine_id: medicine for medicine in medicines}

            #  Фільтрація та формування результатів
            for hit in hits:
                medicine = medicines_by_id.get(hit.medicine_id)

                if medicine is None:
                    continue

                if "is_prescription" in filters:
                    if (medicine.is_prescription != filters["is_prescription"]):
                        continue

                if "price_max" in filters:
                    if medicine.price > filters["price_max"]:
                        continue

                items.append(
                    SearchItem(
                        medicine_id=medicine.medicine_id,
                        name=medicine.name,
                        therapeutic_class=medicine.therapeutic_class,
                        is_prescription=medicine.is_prescription,
                        price=medicine.price,
                        score=hit.score,
                        snippets=hit.snippets
                    )
                )

                if len(items) >= query.limit:
                    break

            if len(items) >= query.limit or scanned >= MAX_SCANNED_HITS:
                break

        # Формування підсумкового результату
        result = SearchResult(items=items, total=len(items), cache_hit=False)

        # Збереження результатів у Redis
        try:
            await self._cache.set(key, result.model_dump_json(), ttl_seconds=self._cache_ttl)

        except Exception:
            logger.warning("Не вдалося зберегти результат у Redis", exc_info=True)

        # Запис метрик та повернення результатів
        return await self._finish(result, query.raw, normalized, False, started)