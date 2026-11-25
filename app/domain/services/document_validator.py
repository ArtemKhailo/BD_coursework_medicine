"""Доменна служба: валідація документа перед записом."""
from __future__ import annotations

from uuid import UUID

from app.domain.entities.medicine import Medicine
from app.domain.errors import ValidationError

PRICE_MIN = 50.0
PRICE_MAX = 1500.0


class DocumentValidator:
    def validate(self, medicine: Medicine) -> list[str]:
        """Повертає список помилок (порожній = валідно).

        Перевірки: непорожнє name, therapeutic_class не порожній (ключ шардінгу),
        price у межах 50..1500, medicine_id є UUID.
        """
        errors: list[str] = []
        if not isinstance(medicine.medicine_id, UUID):
            errors.append(f"medicine_id має бути UUID, отримано {type(medicine.medicine_id).__name__}")
        if not (medicine.name or "").strip():
            errors.append("name не може бути порожнім")
        if not (medicine.therapeutic_class or "").strip():
            errors.append("therapeutic_class не може бути порожнім (частина ключа шардінгу)")
        price = medicine.price
        if isinstance(price, bool) or not isinstance(price, (int, float)):
            errors.append("price має бути числом")
        elif not PRICE_MIN <= price <= PRICE_MAX:
            errors.append(f"price має бути в межах {PRICE_MIN:g}..{PRICE_MAX:g}, отримано {price}")
        return errors

    def ensure_valid(self, medicine: Medicine) -> None:
        """Кидає ValidationError, якщо validate() повернув помилки."""
        errors = self.validate(medicine)
        if errors:
            raise ValidationError(errors)
