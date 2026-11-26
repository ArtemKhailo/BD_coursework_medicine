"""Етап 12: довідник фільтрів (facets), пошук за назвою, нормалізація значень фільтрів."""
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.application.dto.facets import MedicineFacetsQuery
from app.application.dto.medicine import MedicineListQuery
from app.application.queries.get_medicine_facets_handler import GetMedicineFacetsHandler
from app.application.queries.list_medicines_handler import ListMedicinesHandler
from app.domain.entities.catalog_facets import CatalogFacets, FacetCount
from app.infrastructure.persistence.mongo_document_adapter import MongoDocumentAdapter
from app.presentation.rest.medicine_controller import MedicineController


# --- нормалізація: користувач може ввести будь-який регістр і пробіли по краях ---

def test_list_query_normalizes_text_filters():
    query = MedicineListQuery(
        therapeutic_class="  Anti Infectives ",
        chemical_class="",
        name_prefix="AUGM",
    )
    assert query.therapeutic_class == "anti infectives"
    assert query.chemical_class is None          # порожнє поле = фільтр не задано
    assert query.name_prefix == "augm"


def test_normalization_keeps_inner_spaces():
    # у даних є хімічні класи з подвійним пробілом - їх не можна «виправляти»
    value = "carbonylimidazoles  derivatives"
    assert MedicineListQuery(chemical_class=value).chemical_class == value


def test_facets_query_normalizes_class():
    assert MedicineFacetsQuery(therapeutic_class=" Cardiac").therapeutic_class == "cardiac"
    assert MedicineFacetsQuery(therapeutic_class="").therapeutic_class is None


# --- фільтр за початком назви -> regex із ^ (використовує індекс name) ---

def test_name_prefix_filter_is_anchored_regex():
    flt = MongoDocumentAdapter._build_filter({"name_prefix": " Augm"})
    assert flt == {"name": {"$regex": "^augm"}}


def test_name_prefix_escapes_regex_symbols():
    regex = MongoDocumentAdapter._build_filter({"name_prefix": "a+b (5"})["name"]["$regex"]
    assert re.match(regex, "a+b (5 mg)")       # символи сприймаються буквально
    assert not re.match(regex, "aab (5 mg)")   # "+" не став квантифікатором


def test_empty_name_prefix_adds_no_condition():
    assert MongoDocumentAdapter._build_filter({"name_prefix": "  "}) == {}


def test_price_range_filter():
    flt = MongoDocumentAdapter._build_filter({"price_min": 100, "price_max": 300})
    assert flt == {"price": {"$gte": 100.0, "$lte": 300.0}}


async def test_list_handler_passes_name_prefix_and_price_min():
    repository = SimpleNamespace(
        find=AsyncMock(return_value=[]),
        count=AsyncMock(return_value=0),
    )
    handler = ListMedicinesHandler(repository)

    await handler.handle(MedicineListQuery(name_prefix="Augm", price_min=100))

    filters = {"name_prefix": "augm", "price_min": 100}
    repository.find.assert_awaited_once_with(filters=filters, limit=20, offset=0)
    repository.count.assert_awaited_once_with(filters)


# --- обробник довідника ---

async def test_facets_handler_maps_domain_to_dto():
    facets = CatalogFacets(
        therapeutic_classes=[FacetCount("cardiac", 3), FacetCount("derma", 1)],
        chemical_classes=[FacetCount("macrolides", 2)],
        price_min=55.5,
        price_max=999.0,
    )
    repository = SimpleNamespace(facets=AsyncMock(return_value=facets))
    handler = GetMedicineFacetsHandler(repository)

    result = await handler.handle(MedicineFacetsQuery(therapeutic_class="Cardiac"))

    repository.facets.assert_awaited_once_with("cardiac")
    assert [(f.value, f.count) for f in result.therapeutic_classes] == [("cardiac", 3), ("derma", 1)]
    assert result.chemical_classes[0].value == "macrolides"
    assert (result.price.min, result.price.max) == (55.5, 999.0)


# --- контролер ---

def test_facets_route_is_registered_before_card_route():
    controller = MedicineController(None, None, None, None, None, get_facets=None)
    paths = [route.path for route in controller.router.routes]
    assert "/api/medicines/facets" in paths
    # інакше "facets" сприйметься як medicine_id і FastAPI поверне 422
    assert paths.index("/api/medicines/facets") < paths.index("/api/medicines/{medicine_id}")


async def test_controller_passes_name_as_prefix():
    list_handler = SimpleNamespace(handle=AsyncMock(return_value=None))
    controller = MedicineController(list_handler, None, None, None, None)

    await controller.list_medicines(
        is_prescription=None, price_min=None, price_max=None, in_stock=None,
        therapeutic_class="Cardiac", chemical_class=None, name="Augm", limit=20, offset=0,
    )

    query = list_handler.handle.await_args.args[0]
    assert query.name_prefix == "augm"
    assert query.therapeutic_class == "cardiac"


async def test_controller_facets_endpoint_calls_handler():
    facets_handler = SimpleNamespace(handle=AsyncMock(return_value="ok"))
    controller = MedicineController(None, None, None, None, None, get_facets=facets_handler)

    assert await controller.get_facets(therapeutic_class="Neuro CNS") == "ok"
    query = facets_handler.handle.await_args.args[0]
    assert query.therapeutic_class == "neuro cns"
