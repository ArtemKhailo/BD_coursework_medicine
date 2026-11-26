"""DTO довідника фільтрів: GET /api/medicines/facets."""
from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

from app.application.dto.medicine import normalize_filter_value


class MedicineFacetsQuery(BaseModel):
    """Якщо клас задано, хімічні класи й ціни повертаються лише для нього."""
    therapeutic_class: str | None = None

    @field_validator("therapeutic_class")
    @classmethod
    def _normalize(cls, value: str | None) -> str | None:
        return normalize_filter_value(value)


class FacetValueDto(BaseModel):
    value: str
    count: int


class PriceRangeDto(BaseModel):
    min: float | None = None
    max: float | None = None


class MedicineFacetsDto(BaseModel):
    therapeutic_classes: list[FacetValueDto] = Field(default_factory=list)
    chemical_classes: list[FacetValueDto] = Field(default_factory=list)
    price: PriceRangeDto = Field(default_factory=PriceRangeDto)
