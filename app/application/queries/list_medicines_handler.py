"""Фільтрація каталогу за атрибутами (MongoDB)."""
from __future__ import annotations

from app.application.dto.medicine import MedicineListQuery, MedicineListResult
from app.application.mappers import medicine_to_card
from app.application.ports.inbound.list_medicines import ListMedicines
from app.domain.ports.outbound import DocumentRepository


class ListMedicinesHandler(ListMedicines):
    def __init__(self, documents: DocumentRepository) -> None:
        self._documents = documents

    async def handle(self, query: MedicineListQuery) -> MedicineListResult:
        filters = query.model_dump(
            exclude_none=True,
            exclude={"limit", "offset"}
        )

        medicines = await self._documents.find(
            filters=filters,
            limit=query.limit,
            offset=query.offset
        )

        total = await self._documents.count(filters)

        return MedicineListResult(
            items=[medicine_to_card(medicine) for medicine in medicines],
            total=total
        )

