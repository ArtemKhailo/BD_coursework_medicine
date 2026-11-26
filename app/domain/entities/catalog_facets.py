"""Довідник значень для фільтрів каталогу (facets).

Відповідає на питання «які значення фільтрів реально існують у базі і скільки
препаратів має кожне», щоб користувач обирав клас зі списку, а не вгадував його.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FacetCount:
    value: str      # значення поля, як воно лежить у базі (напр. "anti infectives")
    count: int      # кількість препаратів із цим значенням


@dataclass
class CatalogFacets:
    therapeutic_classes: list[FacetCount] = field(default_factory=list)  # усі класи каталогу
    chemical_classes: list[FacetCount] = field(default_factory=list)     # у межах обраного класу
    price_min: float | None = None                                       # у межах обраного класу
    price_max: float | None = None
