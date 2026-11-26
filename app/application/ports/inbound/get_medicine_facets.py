"""Вхідний порт: довідник значень для фільтрів каталогу."""
from __future__ import annotations

from abc import ABC, abstractmethod

from app.application.dto.facets import MedicineFacetsDto, MedicineFacetsQuery


class GetMedicineFacets(ABC):
    @abstractmethod
    async def handle(self, query: MedicineFacetsQuery) -> MedicineFacetsDto: ...
