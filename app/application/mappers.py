"""Перетворення Domain <-> DTO. Єдине місце, що знає про обидва формати."""
from __future__ import annotations

from uuid import UUID, uuid5

from app.application.constants import MEDICINE_NAMESPACE
from app.application.dto.ingest import IngestDocumentDto
from app.application.dto.medicine import MedicineCard, MedicineCreateDto
from app.domain.entities.medicine import Medicine
from app.domain.entities.search_document import SearchDocument

API_SOURCE_ID = 0   # препарат створено через API, а не завантажено з датасету


def normalize_name(name: str) -> str:
    """Як у конвеєрі даних: trim + нижній регістр."""
    return name.strip().lower()


def medicine_id_for(name: str) -> UUID:
    """UUID5 від нормалізованої назви: та сама назва -> той самий id (і в датасеті, і в API)."""
    return uuid5(MEDICINE_NAMESPACE, normalize_name(name))


def medicine_to_card(medicine: Medicine) -> MedicineCard:
    return MedicineCard(
        medicine_id=medicine.medicine_id,
        name=medicine.name,
        chemical_class=medicine.chemical_class,
        therapeutic_class=medicine.therapeutic_class,
        action_class=medicine.action_class,
        is_habit_forming=medicine.is_habit_forming,
        is_prescription=medicine.is_prescription,
        manufacturer=medicine.manufacturer,
        price=medicine.price,
        in_stock=medicine.in_stock,
        uses=list(medicine.uses),
        side_effects=list(medicine.side_effects),
        substitutes=list(medicine.substitutes),
    )


def create_dto_to_medicine(dto: MedicineCreateDto) -> Medicine:
    """Генерує medicine_id як UUID5 від name (детерміновано, як у конвеєрі даних)."""
    name = normalize_name(dto.name)
    return Medicine(
        medicine_id=medicine_id_for(name),
        source_id=API_SOURCE_ID,
        name=name,
        therapeutic_class=dto.therapeutic_class,
        is_habit_forming=dto.is_habit_forming,
        is_prescription=dto.is_prescription,
        manufacturer=dto.manufacturer,
        price=dto.price,
        chemical_class=dto.chemical_class,
        action_class=dto.action_class,
        uses=list(dto.uses),
        side_effects=list(dto.side_effects),
        substitutes=list(dto.substitutes),
        in_stock=dto.in_stock,
    )


def ingest_dto_to_medicine(dto: IngestDocumentDto) -> Medicine:
    """Документ із генератора: id і source_id вже пораховані конвеєром даних."""
    return Medicine(
        medicine_id=dto.medicine_id,
        source_id=dto.source_id,
        name=dto.name,
        therapeutic_class=dto.therapeutic_class,
        is_habit_forming=dto.is_habit_forming,
        is_prescription=dto.is_prescription,
        manufacturer=dto.manufacturer,
        price=dto.price,
        chemical_class=dto.chemical_class,
        action_class=dto.action_class,
        uses=list(dto.uses),
        side_effects=list(dto.side_effects),
        substitutes=list(dto.substitutes),
        in_stock=dto.in_stock,
    )


def ingest_dto_to_search_document(dto: IngestDocumentDto) -> SearchDocument:
    return SearchDocument(
        medicine_id=dto.medicine_id,
        name=dto.name,
        full_description=dto.full_description,
    )
