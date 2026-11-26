"""Вихідний порт: CRUD-репозиторій документів (адаптер: MongoDB)."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any
from uuid import UUID

from app.domain.entities.catalog_facets import CatalogFacets
from app.domain.entities.medicine import Medicine


class DocumentRepository(ABC):
    # --- Create ---
    @abstractmethod
    async def create(self, medicine: Medicine) -> Medicine: ...

    @abstractmethod
    async def create_many(self, medicines: list[Medicine]) -> int:
        """Пакетний запис (bulk insert). Повертає кількість записаних."""

    # --- Read ---
    @abstractmethod
    async def get_by_id(self, medicine_id: UUID) -> Medicine | None: ...

    @abstractmethod
    async def by_ids(self, ids: list[UUID]) -> list[Medicine]:
        """Картки за списком id (після пошуку в Elasticsearch)."""

    @abstractmethod
    async def find(self, filters: dict[str, Any], limit: int = 20, offset: int = 0) -> list[Medicine]:
        """Фільтрація: is_prescription, in_stock, price_min/price_max, therapeutic_class,
        chemical_class, name (точна назва), name_prefix (початок назви)."""

    @abstractmethod
    async def count(self, filters: dict[str, Any]) -> int: ...

    @abstractmethod
    async def top_side_effects(self, chemical_class: str, limit: int = 10) -> list[tuple[str, int]]:
        """Агрегація: найпопулярніші побічні ефекти класу (запит №6)."""

    @abstractmethod
    async def facets(self, therapeutic_class: str | None = None) -> CatalogFacets:
        """Довідник для фільтрів: усі терапевтичні класи з кількістю препаратів, а хімічні
        класи й діапазон цін - у межах therapeutic_class (якщо задано) або всього каталогу."""

    # --- Update / Delete ---
    @abstractmethod
    async def update(self, medicine_id: UUID, changes: dict[str, Any]) -> Medicine | None: ...

    @abstractmethod
    async def delete(self, medicine_id: UUID) -> bool: ...
