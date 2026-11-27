"""Завантаження prepared/search_docs.jsonl в Elasticsearch (етап 15)"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time

from pathlib import Path
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from elasticsearch import AsyncElasticsearch

from app.domain.entities.search_document import SearchDocument
from app.infrastructure.persistence.search_index_adapter import (
    SearchIndexAdapter,
)
from app.infrastructure.settings import load_settings


def read_batches(
    path: Path,
    batch_size: int,
    limit: int | None
):
    """Читання пошукових документів пакетами."""

    batch = []
    total = 0

    with path.open(encoding = "utf-8") as file:

        for line_no, line in enumerate(file, start = 1):

            if limit is not None and total >= limit:
                break

            if not line.strip():
                continue

            try:
                data = json.loads(line)

                document = SearchDocument(
                    medicine_id = UUID(data["medicine_id"]),
                    name = data["name"],
                    full_description = data["full_description"]
                )

            except (ValueError, KeyError, TypeError) as exc:
                raise ValueError(f"Помилка у рядку {line_no}: {exc}") from exc

            batch.append(document)
            total += 1

            if len(batch) >= batch_size:
                yield batch
                batch = []

    if batch:
        yield batch


async def main() -> None:

    parser = argparse.ArgumentParser(description = "Індексація препаратів в Elasticsearch")

    parser.add_argument("--file", default = str(ROOT / "prepared" / "search_docs.jsonl"))

    parser.add_argument("--config", default = str(ROOT / "config.yaml"))

    parser.add_argument("--batch", type = int, default = None)

    parser.add_argument("--limit", type = int, default = None)

    args = parser.parse_args()

    settings = load_settings(args.config)

    path = Path(args.file)

    batch_size = (
        args.batch
        if args.batch is not None
        else settings.ingest.batch_size
    )

    if batch_size <= 0:
        parser.error("Розмір пакета має бути більшим за 0")

    if args.limit is not None and args.limit <= 0:
        parser.error("Ліміт має бути більшим за 0")

    if not path.is_file():
        sys.exit(
            f"Файл не знайдено: {path}. "
            "Спочатку запустіть scripts/process_dataset.py"
        )

    client = AsyncElasticsearch(
        settings.elastic.url,
        request_timeout = 120
    )

    try:
        if not await client.ping():
            raise ConnectionError("Не вдалося підключитися до Elasticsearch")

        adapter = SearchIndexAdapter(client, settings.elastic.index)

        print("Підключення до Elasticsearch успішне")
        print(f"Індекс: {settings.elastic.index}")

        await adapter.ensure_index()

        print("Пошуковий індекс готовий")

        started = time.perf_counter()

        indexed_total = 0

        for batch in read_batches(
            path,
            batch_size,
            args.limit
        ):

            batch_started = time.perf_counter()

            indexed = await adapter.index_many(batch)

            indexed_total += indexed

            duration_ms = int((time.perf_counter() - batch_started) * 1000)

            print(
                f"Пакет: {len(batch)} | "
                f"Проіндексовано: {indexed} | "
                f"Час: {duration_ms} мс | "
                f"Всього: {indexed_total}"
            )

        await client.indices.refresh(index = settings.elastic.index)

        elapsed = time.perf_counter() - started

        count = await client.count(index = settings.elastic.index)

        print("\nРезультати індексації:")
        print(f"Оброблено документів: {indexed_total}")
        print(f"Документів в індексі: {count['count']}")
        print(f"Загальний час: {elapsed:.2f} с")

        if elapsed > 0:
            print(
                f"Швидкість: "
                f"{indexed_total / elapsed:.0f} док/с"
            )

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
