"""Адаптер Elasticsearch для SearchIndex (індекс medicines: medicine_id, name, full_description)."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from elasticsearch.helpers import async_bulk

from app.domain.entities.search_document import SearchDocument, SearchHit
from app.domain.ports.outbound.search_index import SearchIndex

if TYPE_CHECKING:
    from elasticsearch import AsyncElasticsearch


class SearchIndexAdapter(SearchIndex):
    def __init__(self, client: AsyncElasticsearch, index: str = "medicines") -> None:
        self._client = client
        self._index = index

    async def ensure_index(self) -> None:
        """Створення пошукового індексу та визначення mapping."""

        if await self._client.indices.exists(index = self._index):
            return

        await self._client.indices.create(
            index = self._index,
            settings = {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            mappings = {
                "dynamic": "strict",
                "properties": {
                    "medicine_id": {
                        "type": "keyword"
                    },
                    "name": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "full_description": {
                        "type": "text",
                        "analyzer": "standard"
                    }
                }
            }
        )


    async def search(self, query: str, filters: dict[str, Any] | None = None,
                     limit: int = 20) -> list[SearchHit]:
        """Повнотекстовий пошук із ранжуванням та highlight."""

        if not query.strip():
            return []

        if limit < 1:
            return []

        if filters:
            raise ValueError("Фільтрація за цими параметрами не підтримується пошуковим індексом")

        response = await self._client.search(
            index = self._index,
            size = limit,
            query = {
                "multi_match": {
                    "query": query,
                    "fields": [
                        "name^3",
                        "full_description"
                    ],
                    "fuzziness": "AUTO"
                }
            },
            highlight = {
                "encoder": "html",
                "pre_tags": ["<em>"],
                "post_tags": ["</em>"],
                "fields": {
                    "name": {
                        "number_of_fragments": 0
                    },
                    "full_description": {
                        "fragment_size": 150,
                        "number_of_fragments": 3
                    }
                }
            }
        )

        results = []

        for hit in response["hits"]["hits"]:
            highlights = hit.get("highlight", {})

            snippets = (
                highlights.get("name", [])
                + highlights.get("full_description", [])
            )

            results.append(
                SearchHit(
                    medicine_id = UUID(
                        hit["_source"]["medicine_id"]
                    ),
                    score = float(hit["_score"] or 0.0),
                    snippets = snippets
                )
            )

        return results


    async def index_many(self, documents: list[SearchDocument]) -> int:
        """Пакетна індексація препаратів."""

        if not documents:
            return 0

        actions = (
            {
                "_op_type": "index",
                "_index": self._index,
                "_id": str(document.medicine_id),
                "_source": {
                    "medicine_id": str(document.medicine_id),
                    "name": document.name,
                    "full_description": document.full_description
                }
            }
            for document in documents
        )

        success, _ = await async_bulk(
            self._client,
            actions,
            chunk_size = 500,
            stats_only = True,
            max_retries = 3,
            request_timeout = 120
        )

        return success

    async def delete(self, medicine_id: UUID) -> bool:
        """Видалення препарату з пошукового індексу."""

        response = await self._client.options(
            ignore_status = 404
        ).delete(
            index = self._index,
            id = str(medicine_id)
        )

        return response.get("result") == "deleted"