
"""Вихідний порт: повнотекстовий пошук."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, AsyncIterator
from uuid import UUID

from app.domain.entities.search_document import SearchDocument, SearchHit

class SearchIndex(ABC):
    @abstractmethod
    async def search(self, query: str, filters: dict[str, Any] | None = None, limit: int = 20) -> list[SearchHit]:
        """Повнотекстовий пошук із підсвічуванням."""
    @abstractmethod
    def search_batches(self, query: str, batch_size: int = 200) -> AsyncIterator[list[SearchHit]]:
        """Послідовне отримання пакетів результатів пошуку."""
    @abstractmethod
    async def index_many(self, documents: list[SearchDocument]) -> int:
        """Пакетна індексація документів."""
    @abstractmethod
    async def delete(self, medicine_id: UUID) -> bool:
        """Видалення документа з індексу."""