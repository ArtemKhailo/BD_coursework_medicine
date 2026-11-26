"""Довідник фільтрів: які класи існують у базі та скільки препаратів у кожному."""
from __future__ import annotations

from app.application.dto.facets import (
    FacetValueDto,
    MedicineFacetsDto,
    MedicineFacetsQuery,
    PriceRangeDto,
)
from app.application.ports.inbound.get_medicine_facets import GetMedicineFacets
from app.domain.ports.outbound import DocumentRepository


class GetMedicineFacetsHandler(GetMedicineFacets):
    def __init__(self, documents: DocumentRepository) -> None:
        self._documents = documents

    async def handle(self, query: MedicineFacetsQuery) -> MedicineFacetsDto:
        facets = await self._documents.facets(query.therapeutic_class)
        return MedicineFacetsDto(
            therapeutic_classes=[
                FacetValueDto(value=f.value, count=f.count) for f in facets.therapeutic_classes
            ],
            chemical_classes=[
                FacetValueDto(value=f.value, count=f.count) for f in facets.chemical_classes
            ],
            price=PriceRangeDto(min=facets.price_min, max=facets.price_max),
        )
