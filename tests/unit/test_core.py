"""Крок 2: доменні служби, маппери та завантаження налаштувань (без баз даних)."""
from uuid import UUID, uuid4

import pytest

from app.application.dto import IngestDocumentDto, MedicineCreateDto
from app.application.mappers import (
    create_dto_to_medicine, ingest_dto_to_medicine, ingest_dto_to_search_document,
    medicine_id_for, medicine_to_card,
)
from app.domain.entities import Medicine
from app.domain.errors import ValidationError
from app.domain.services import DocumentValidator, QueryNormalizer, ShardKeyService
from app.infrastructure.settings import load_settings


def _medicine(**changes) -> Medicine:
    data = dict(medicine_id=uuid4(), source_id=1, name="azithral 500 tablet",
                therapeutic_class="anti infectives", is_habit_forming=False,
                is_prescription=False, manufacturer="Vitalis Group", price=686.37)
    data.update(changes)
    return Medicine(**data)


# --- ShardKeyService ---

def test_shard_key_uses_class_and_id():
    m = _medicine()
    key = ShardKeyService().compute(m)
    assert key.therapeutic_class == "anti infectives" and key.medicine_id == m.medicine_id


@pytest.mark.parametrize("empty", ["", "   ", None])
def test_shard_key_empty_class_becomes_unknown(empty):
    assert ShardKeyService().compute(_medicine(therapeutic_class=empty)).therapeutic_class == "unknown"


# --- DocumentValidator ---

def test_valid_medicine_has_no_errors():
    assert DocumentValidator().validate(_medicine()) == []


def test_validator_collects_all_errors():
    bad = _medicine(medicine_id="not-a-uuid", name=" ", therapeutic_class="", price=10)
    errors = DocumentValidator().validate(bad)
    assert len(errors) == 4


def test_ensure_valid_raises_domain_error():
    with pytest.raises(ValidationError) as exc:
        DocumentValidator().ensure_valid(_medicine(price=2000))
    assert "price" in str(exc.value)


# --- QueryNormalizer ---

def test_normalizer_ignores_case_and_word_order():
    n = QueryNormalizer()
    assert n.normalize("Bacterial Infections") == n.normalize("infections  bacterial") == "bacterial infections"


# --- Маппери ---

def test_id_matches_dataset_pipeline():
    # id цього препарату в prepared/medicines.jsonl (UUID5 від назви в конвеєрі даних)
    assert medicine_id_for("Augmentin 625 Duo Tablet ") == UUID("a26eaced-e5cd-57b3-91a8-37a25953fcf1")


def test_create_dto_to_medicine_and_card():
    dto = MedicineCreateDto(name=" Test Drug XQ1 ", manufacturer="Acme", price=120.0,
                            therapeutic_class="cardiac", uses=["fever"], in_stock=False)
    m = create_dto_to_medicine(dto)
    assert m.name == "test drug xq1" and m.medicine_id == medicine_id_for("test drug xq1")
    assert m.source_id == 0 and m.in_stock is False
    card = medicine_to_card(m)
    assert card.medicine_id == m.medicine_id and card.uses == ["fever"] and card.in_stock is False


def test_ingest_dto_mapping():
    mid = uuid4()
    dto = IngestDocumentDto(medicine_id=mid, source_id=7, name="x", manufacturer="Acme",
                            price=60, full_description="Uses: fever.", in_stock=True)
    assert ingest_dto_to_medicine(dto).medicine_id == mid
    doc = ingest_dto_to_search_document(dto)
    assert doc.full_description == "Uses: fever." and doc.medicine_id == mid


# --- Налаштування ---

def test_settings_defaults_without_file(tmp_path, monkeypatch):
    monkeypatch.delenv("MONGO_URI", raising=False)
    s = load_settings(tmp_path / "missing.yaml")
    assert s.mongo.uri == "mongodb://localhost:27017" and s.ingest.batch_size == 5000


def test_settings_yaml_then_dotenv_then_environment(tmp_path, monkeypatch):
    (tmp_path / "config.yaml").write_text(
        "mongo:\n  uri: mongodb://yaml:27017\n  database: med\ningest:\n  batch_size: 100\n",
        encoding="utf-8")
    (tmp_path / ".env").write_text("MONGO_URI=mongodb://dotenv:27017\nREDIS_URL=redis://dotenv\n",
                                   encoding="utf-8")
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setenv("MONGO_URI", "mongodb://env:27017")
    s = load_settings(tmp_path / "config.yaml")
    assert s.mongo.uri == "mongodb://env:27017"        # змінна середовища важливіша за .env
    assert s.redis.url == "redis://dotenv"              # .env важливіший за значення за замовчуванням
    assert s.mongo.database == "med" and s.ingest.batch_size == 100   # з YAML, int


def test_settings_reject_unknown_keys(tmp_path):
    (tmp_path / "config.yaml").write_text("mongo:\n  urii: x\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_settings(tmp_path / "config.yaml")
