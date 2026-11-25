"""Завантаження prepared/medicines.jsonl у шардований кластер MongoDB (етап 10-11).

Шлях кожного документа той самий, що й у Ingest Worker:
JSON -> IngestDocumentDto (валідація типів) -> Medicine -> DocumentValidator ->
MongoDocumentAdapter.create_many (пакетний insert_many) -> MongoMetricsAdapter.record_ingest.

Запуск із кореня репозиторію (кластер має бути запущений):
    python scripts/load_mongo.py                 # усі 222 825 документів
    python scripts/load_mongo.py --limit 10000   # перші 10 000 (швидка перевірка)
    python scripts/load_mongo.py --drop          # спершу видалити колекцію і створити наново

Повторний запуск безпечний: наявні документи пропускаються (див. create_many).
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pymongo import AsyncMongoClient  # noqa: E402

from app.application.dto.ingest import IngestDocumentDto  # noqa: E402
from app.application.mappers import ingest_dto_to_medicine  # noqa: E402
from app.domain.services.document_validator import DocumentValidator  # noqa: E402
from app.infrastructure.observability.mongo_metrics_adapter import MongoMetricsAdapter  # noqa: E402
from app.infrastructure.persistence.mongo_document_adapter import MongoDocumentAdapter  # noqa: E402
from app.infrastructure.settings import load_settings  # noqa: E402


def read_batches(path: Path, batch_size: int, limit: int | None, stats: dict[str, int]):
    """Читає JSONL пакетами; рядки з помилками відкидає з поясненням (stats["rejected"])."""
    validator = DocumentValidator()
    batch, total = [], 0
    with path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            if limit is not None and total >= limit:
                break
            if not line.strip():
                continue
            total += 1
            try:
                medicine = ingest_dto_to_medicine(IngestDocumentDto.model_validate_json(line))
                validator.ensure_valid(medicine)
            except Exception as exc:  # noqa: BLE001 - некоректний рядок не має зупиняти завантаження
                stats["rejected"] += 1
                print(f"  рядок {line_no} відкинуто: {exc}")
                continue
            batch.append(medicine)
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if batch:
        yield batch


async def main() -> None:
    parser = argparse.ArgumentParser(description="Завантаження препаратів у MongoDB")
    parser.add_argument("--file", default=str(ROOT / "prepared" / "medicines.jsonl"))
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    parser.add_argument("--batch", type=int, default=None, help="розмір пакета (за замовчуванням з config.yaml)")
    parser.add_argument("--limit", type=int, default=None, help="завантажити лише перші N документів")
    parser.add_argument("--drop", action="store_true", help="видалити колекцію перед завантаженням")
    args = parser.parse_args()

    settings = load_settings(args.config)
    batch_size = args.batch or settings.ingest.batch_size
    path = Path(args.file)
    if not path.is_file():
        sys.exit(f"Файл не знайдено: {path}. Спершу запустіть scripts/process_dataset.py")

    client = AsyncMongoClient(settings.mongo.uri, serverSelectionTimeoutMS=5000)
    try:
        documents = MongoDocumentAdapter(client, settings.mongo.database)
        metrics = MongoMetricsAdapter(client, settings.mongo.database)
        print(f"MongoDB: {settings.mongo.uri}, колекція {documents.namespace}")

        if args.drop:
            await client[settings.mongo.database]["medicines"].drop()
            print("Колекцію medicines видалено")
        if await documents.ensure_sharding():
            print("Колекцію зашардовано і розбито на 3 діапазони за ключем {therapeutic_class, medicine_id}")
        await documents.ensure_indexes()
        await metrics.ensure_indexes()

        started = time.perf_counter()
        inserted_total = processed = 0
        stats = {"rejected": 0}
        for batch in read_batches(path, batch_size, args.limit, stats):
            t0 = time.perf_counter()
            inserted = await documents.create_many(batch)
            duration_ms = int((time.perf_counter() - t0) * 1000)
            await metrics.record_ingest(inserted, duration_ms)
            inserted_total += inserted
            processed += len(batch)
            elapsed = time.perf_counter() - started
            print(f"  пакет {len(batch):5d}: нових {inserted:5d} за {duration_ms:5d} мс | "
                  f"всього {processed:7d}, {processed / elapsed:7.0f} док/с")

        elapsed = time.perf_counter() - started
        print(f"\nОброблено {processed} документів за {elapsed:.1f} с "
              f"({processed / max(elapsed, 1e-9):.0f} док/с); нових записано: {inserted_total}; "
              f"вже були: {processed - inserted_total}; відкинуто: {stats['rejected']}")
        print(f"Документів у колекції: {await documents.count({})}")
        print("Розподіл по шардах:")
        distribution = await documents.shard_distribution()
        total = sum(distribution.values()) or 1
        for shard, count in sorted(distribution.items()):
            print(f"  {shard}: {count:7d} ({100 * count / total:.1f}%)")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
