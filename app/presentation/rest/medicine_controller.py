"""REST: CRUD препаратів + фільтрація (запити №2, №3) + довідник фільтрів (facets)."""
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, Query

from app.application.dto.facets import MedicineFacetsDto, MedicineFacetsQuery
from app.application.dto.medicine import (
    MedicineCard,
    MedicineCreateDto,
    MedicineListQuery,
    MedicineListResult,
    MedicineUpdateDto,
)
from app.application.ports.inbound import (
    CreateMedicine,
    DeleteMedicine,
    GetMedicineCard,
    GetMedicineFacets,
    ListMedicines,
    UpdateMedicine,
)
from app.domain.errors import MedicineNotFoundError


class MedicineController:
    def __init__(
        self,
        list_medicines: ListMedicines,
        get_card: GetMedicineCard,
        create_medicine: CreateMedicine,
        update_medicine: UpdateMedicine,
        delete_medicine: DeleteMedicine,
        get_facets: GetMedicineFacets | None = None,
    ) -> None:
        self._list = list_medicines
        self._get_card = get_card
        self._create = create_medicine
        self._update = update_medicine
        self._delete = delete_medicine
        self._facets = get_facets

        self.router = APIRouter(
            prefix="/api/medicines",
            tags=["medicines"],
        )

        r = self.router

        r.add_api_route(
            "",
            self.list_medicines,
            methods=["GET"],
            response_model=MedicineListResult,
        )

        # Має бути зареєстрований ДО "/{medicine_id}": інакше "facets"
        # сприйметься як medicine_id і FastAPI поверне 422.
        r.add_api_route(
            "/facets",
            self.get_facets,
            methods=["GET"],
            response_model=MedicineFacetsDto,
        )

        r.add_api_route(
            "/{medicine_id}",
            self.get_card,
            methods=["GET"],
            response_model=MedicineCard,
        )

        r.add_api_route(
            "",
            self.create,
            methods=["POST"],
            response_model=MedicineCard,
            status_code=201,
        )

        r.add_api_route(
            "/{medicine_id}",
            self.update,
            methods=["PATCH"],
            response_model=MedicineCard,
        )

        r.add_api_route(
            "/{medicine_id}",
            self.delete,
            methods=["DELETE"],
            status_code=204,
        )

    async def list_medicines(
        self,
        is_prescription: bool | None = None,
        price_min: float | None = Query(None, ge=0),
        price_max: float | None = Query(None, ge=0),
        in_stock: bool | None = None,
        therapeutic_class: str | None = None,
        chemical_class: str | None = None,
        name: str | None = Query(None, description="Початок назви препарату"),
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0),
    ) -> MedicineListResult:
        query = MedicineListQuery(
            is_prescription=is_prescription,
            price_min=price_min,
            price_max=price_max,
            in_stock=in_stock,
            therapeutic_class=therapeutic_class,
            chemical_class=chemical_class,
            name_prefix=name,
            limit=limit,
            offset=offset,
        )

        return await self._list.handle(query)

    async def get_facets(
        self,
        therapeutic_class: str | None = None,
    ) -> MedicineFacetsDto:
        if self._facets is None:
            raise HTTPException(
                status_code=501,
                detail="Facets are not configured",
            )

        return await self._facets.handle(
            MedicineFacetsQuery(therapeutic_class=therapeutic_class)
        )

    async def get_card(self, medicine_id: UUID) -> MedicineCard:
        try:
            return await self._get_card.handle(medicine_id)
        except MedicineNotFoundError:
            raise HTTPException(
                status_code=404,
                detail="Medicine not found",
            )

    async def create(self, dto: MedicineCreateDto) -> MedicineCard:
        return await self._create.handle(dto)

    async def update(
        self,
        medicine_id: UUID,
        dto: MedicineUpdateDto,
    ) -> MedicineCard:
        try:
            return await self._update.handle(medicine_id, dto)
        except MedicineNotFoundError:
            raise HTTPException(
                status_code=404,
                detail="Medicine not found",
            )

    async def delete(self, medicine_id: UUID) -> None:
        try:
            deleted = await self._delete.handle(medicine_id)
        except MedicineNotFoundError:
            raise HTTPException(
                status_code=404,
                detail="Medicine not found",
            )

        if not deleted:
            raise HTTPException(
                status_code=404,
                detail="Medicine not found",
            )

        return None
