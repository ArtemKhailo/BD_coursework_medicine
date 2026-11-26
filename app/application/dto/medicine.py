"""DTO препарату: картка, створення, оновлення, список."""
from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class MedicineCard(BaseModel):
    """Повна картка препарату (читання за ключем)."""
    medicine_id: UUID
    name: str
    chemical_class: str | None = None
    therapeutic_class: str
    action_class: str | None = None
    is_habit_forming: bool
    is_prescription: bool
    manufacturer: str
    price: float
    in_stock: bool = True
    uses: list[str] = Field(default_factory=list)
    side_effects: list[str] = Field(default_factory=list)
    substitutes: list[str] = Field(default_factory=list)


class MedicineCreateDto(BaseModel):
    name: str = Field(min_length=1)
    therapeutic_class: str = "unknown"
    chemical_class: str | None = None
    action_class: str | None = None
    is_habit_forming: bool = False
    is_prescription: bool = False
    manufacturer: str
    price: float = Field(ge=50.0, le=1500.0)
    in_stock: bool = True
    uses: list[str] = Field(default_factory=list)
    side_effects: list[str] = Field(default_factory=list)
    substitutes: list[str] = Field(default_factory=list)


class MedicineUpdateDto(BaseModel):
    """Часткове оновлення: всі поля необов'язкові."""
    therapeutic_class: str | None = None
    chemical_class: str | None = None
    action_class: str | None = None
    is_prescription: bool | None = None
    manufacturer: str | None = None
    price: float | None = Field(default=None, ge=50.0, le=1500.0)
    in_stock: bool | None = None
    uses: list[str] | None = None
    side_effects: list[str] | None = None
    substitutes: list[str] | None = None


def normalize_filter_value(value: str | None) -> str | None:
    """Значення текстового фільтра -> вигляд, у якому воно лежить у базі.

    Класи й назви в базі записані малими літерами (так їх готує конвеєр даних),
    тому "Anti Infectives " знайде "anti infectives". Порожній рядок = фільтр не задано.
    Пробіли всередині не чіпаємо: кілька хімічних класів у даних містять подвійні пробіли.
    """
    if value is None:
        return None
    value = value.strip().lower()
    return value or None


class MedicineListQuery(BaseModel):
    """Фільтрація за параметрами (запит №2)."""
    is_prescription: bool | None = None
    price_min: float | None = Field(default=None, ge=0)
    price_max: float | None = Field(default=None, ge=0)
    in_stock: bool | None = None
    therapeutic_class: str | None = None
    chemical_class: str | None = None
    name_prefix: str | None = None   # пошук за початком назви: "augm" -> "augmentin 625 duo tablet"
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)

    @field_validator("therapeutic_class", "chemical_class", "name_prefix")
    @classmethod
    def _normalize(cls, value: str | None) -> str | None:
        return normalize_filter_value(value)


class MedicineListResult(BaseModel):
    items: list[MedicineCard] = Field(default_factory=list)
    total: int = 0
