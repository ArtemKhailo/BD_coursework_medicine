"""Доменні винятки. Не залежать від жодного іншого шару."""


class DomainError(Exception):
    """Базовий виняток доменного шару."""


class MedicineNotFoundError(DomainError):
    """Препарат з указаним medicine_id не знайдено."""


class MedicineAlreadyExistsError(DomainError):
    """Препарат з таким medicine_id уже існує (повторне створення)."""


class ValidationError(DomainError):
    """Документ не пройшов валідацію (DocumentValidator)."""

    def __init__(self, errors: list[str]) -> None:
        super().__init__("; ".join(errors))
        self.errors = errors
