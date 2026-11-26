"""Читання картки препарату за ключем."""
from __future__ import annotations

from uuid import UUID

from app.application.dto.medicine import MedicineCard
from app.application.mappers import medicine_to_card
from app.application.ports.inbound.get_medicine_card import GetMedicineCard
from app.domain.errors import MedicineNotFoundError
from app.domain.ports.outbound import CacheStore, DocumentRepository


class GetMedicineCardHandler(GetMedicineCard):
    def __init__(self, documents: DocumentRepository, cache: CacheStore) -> None:
        self._documents = documents
        self._cache = cache

    async def handle(self, medicine_id: UUID) -> MedicineCard:
        medicine = await self._documents.get_by_id(medicine_id)

        if medicine is None:
            raise MedicineNotFoundError(
                f"Medicine with ID {medicine_id} not found"
            )

        return medicine_to_card(medicine)

