"""Доменна служба: обчислення ключа шардінгу MongoDB."""
from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from app.domain.entities.medicine import Medicine

UNKNOWN_CLASS = "unknown"   # те саме значення, що й у scripts/process_dataset.py


@dataclass(frozen=True)
class ShardKey:
    """Складений ключ { therapeutic_class: 1, medicine_id: 1 }."""
    therapeutic_class: str
    medicine_id: UUID


class ShardKeyService:
    def compute(self, medicine: Medicine) -> ShardKey:
        """Будує ShardKey; порожній therapeutic_class замінює на 'unknown'.

        Ключ шардінгу в MongoDB не може бути відсутнім або null, інакше всі такі
        документи потрапили б в один діапазон. Тому порожній клас нормалізується.
        """
        therapeutic_class = (medicine.therapeutic_class or "").strip() or UNKNOWN_CLASS
        return ShardKey(therapeutic_class=therapeutic_class, medicine_id=medicine.medicine_id)
