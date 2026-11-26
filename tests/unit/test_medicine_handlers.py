from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.application.dto.medicine import MedicineListQuery
from app.application.queries.list_medicines_handler import ListMedicinesHandler
from app.application.queries.get_medicine_card_handler import GetMedicineCardHandler
from app.domain.entities.medicine import Medicine
from app.domain.errors import MedicineNotFoundError


@pytest.mark.asyncio
async def test_list_medicines_with_filters():
    repository = SimpleNamespace(
        find=AsyncMock(return_value=[]),
        count=AsyncMock(return_value=0),
    )

    handler = ListMedicinesHandler(repository)
    query = MedicineListQuery(in_stock=True, price_max=500, limit=10)

    result = await handler.handle(query)

    assert result.total == 0
    assert result.items == []

    filters = {"in_stock": True, "price_max": 500}

    repository.find.assert_awaited_once_with(
        filters=filters,
        limit=10,
        offset=0,
    )
    repository.count.assert_awaited_once_with(filters)


@pytest.mark.asyncio
async def test_get_medicine_card_found():
    medicine_id = uuid4()
    medicine = Medicine(
        medicine_id=medicine_id,
        source_id=1,
        name="test medicine",
        therapeutic_class="anti infectives",
        is_habit_forming=False,
        is_prescription=True,
        manufacturer="Test Pharma",
        price=250.0,
        in_stock=True,
    )

    repository = SimpleNamespace(
        get_by_id=AsyncMock(return_value=medicine)
    )

    handler = GetMedicineCardHandler(repository, cache=None)
    result = await handler.handle(medicine_id)

    assert result.medicine_id == medicine_id
    assert result.name == "test medicine"
    assert result.price == 250.0

    repository.get_by_id.assert_awaited_once_with(medicine_id)


@pytest.mark.asyncio
async def test_get_medicine_card_not_found():
    medicine_id = uuid4()

    repository = SimpleNamespace(
        get_by_id=AsyncMock(return_value=None)
    )

    handler = GetMedicineCardHandler(repository, cache=None)

    with pytest.raises(MedicineNotFoundError):
        await handler.handle(medicine_id)
