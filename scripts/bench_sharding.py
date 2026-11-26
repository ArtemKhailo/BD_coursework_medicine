"""Етап 13: порівняння швидкості запису в MongoDB на 1 шард і на кілька (3+).

Бенчмарк пише в окрему колекцію medicine.medicines_bench, основну колекцію medicines
не чіпає. Ключ шардінгу той самий, що в основній колекції: {therapeutic_class, medicine_id}.

Команди (запуск із кореня репозиторію, кластер має працювати):

1) Підготовка даних (один раз, ~1-3 хв, ~0,9 ГБ на диску в prepared/bench/):
    python scripts/bench_sharding.py prepare
   Бере prepared/medicines.jsonl (222 825 документів), робить 5 копій кожного препарату
   з новими назвами та id (разом 1 114 125 документів), перемішує їх і зберігає готовими
   BSON-документами у 8 файлів. Окремо рахує межі чанків для 1-5 шард.

2) Прогони (результати дописуються в results/bench/bench_results.csv):
    python scripts/bench_sharding.py run --shards 1
    python scripts/bench_sharding.py run --shards 3
    python scripts/bench_sharding.py run --shards 4 5     # потрібен docker-compose.mongo.scale.yml
   За замовчуванням для кожної кількості шард: записувачі 1, 4, 8; пакет 1000 і 5000;
   по 3 прогони. Перерваний запуск можна просто повторити: виконані прогони пропускаються.
   Швидка перевірка, що все працює:  run --shards 3 --writers 4 --batch 5000 --repeats 1 --docs 100000

3) Підсумок (медіана з прогонів, таблиця і графік у results/bench/):
    python scripts/bench_sharding.py report

4) Прибирання:  python scripts/bench_sharding.py clean   (видаляє колекцію medicines_bench)

Як влаштовано один прогін:
  * колекцію medicines_bench створюють наново: ті самі індекси, що в medicines, шардінг
    за {therapeutic_class, medicine_id}, балансувальник для неї вимкнено;
  * колекцію заздалегідь ділять на 4 x N чанків за квантилями ключа (рівні за кількістю
    документів) і розносять по N шардах по колу, тож кожен шард отримує 4 чанки;
    для N = 1 усі чанки лежать на shard1rs, а інші шарди просто не використовуються
    («ніби інших немає»);
  * W записувачів - окремі процеси Python, кожен зі своїм підключенням до mongos;
    документи заздалегідь закодовані в BSON і завантажені в пам'ять, тому вимірюється
    робота кластера, а не підготовка документів у Python;
  * час рахується від одночасного старту всіх записувачів до завершення останнього;
  * після прогону перевіряється, що в колекції рівно стільки документів, скільки записано,
    і як вони розподілилися по шардах.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import dataclasses
import json
import multiprocessing as mp
import queue as queue_module
import random
import shutil
import statistics
import subprocess
import sys
import threading
import time
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

BENCH_DIR = ROOT / "prepared" / "bench"
RESULTS_DIR = ROOT / "results" / "bench"
RESULTS_CSV = RESULTS_DIR / "bench_results.csv"
META_FILE = BENCH_DIR / "meta.json"

COLLECTION = "medicines_bench"
PARTS = 8                    # стільки файлів даних; записувачів може бути до 8
CHUNKS_PER_SHARD = 4
MAX_SHARDS = 5
SHARD_KEY = {"therapeutic_class": 1, "medicine_id": 1}
DUPLICATE_KEY = 11000

CSV_FIELDS = [
    "timestamp", "shards", "writers", "batch", "repeat", "docs", "seconds",
    "docs_per_sec", "batch_p50_ms", "batch_p95_ms", "distribution", "chunks", "cpu",
]


# ============================================================ допоміжні функції (без БД)

def copy_name(name: str, copy: int) -> str:
    """Копія 0 - оригінальний препарат, решта отримують суфікс: «augmentin 625 duo tablet #3»."""
    return name if copy == 0 else f"{name} #{copy}"


def split_points(sorted_keys: list[tuple[str, str]], chunks: int) -> list[list[str]]:
    """Межі між чанками за квантилями: кожен із chunks чанків отримує однакову кількість ключів."""
    if chunks < 1:
        raise ValueError("chunks має бути >= 1")
    n = len(sorted_keys)
    return [list(sorted_keys[n * i // chunks]) for i in range(1, chunks)]


def chunk_targets(chunks: int, shards: list[str]) -> list[str]:
    """Чанк j -> шард j mod N (по колу): сусідні діапазони ключа лежать на різних шардах."""
    return [shards[j % len(shards)] for j in range(chunks)]


def assign_parts(parts: int, writers: int) -> list[list[int]]:
    """Файли даних між записувачами: файл p дістається записувачу p mod W."""
    if not 1 <= writers <= parts:
        raise ValueError(f"записувачів має бути від 1 до {parts}")
    return [[p for p in range(parts) if p % writers == w] for w in range(writers)]


def split_quota(total: int | None, writers: int) -> list[int | None]:
    """Обмеження --docs ділиться між записувачами порівну (залишок - першим)."""
    if total is None:
        return [None] * writers
    base, extra = divmod(total, writers)
    return [base + (1 if w < extra else 0) for w in range(writers)]


def percentile(values: list[float], q: float) -> float:
    """Перцентиль методом найближчого рангу (p50 - медіана, p95 - 95 % пакетів не довші)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(1, -(-len(ordered) * q // 100))          # ceil(n * q / 100)
    return ordered[int(rank) - 1]


def iter_bson(data: bytes):
    """Розбиває вміст файлу на окремі BSON-документи (перші 4 байти документа - його довжина)."""
    offset, size = 0, len(data)
    while offset < size:
        length = int.from_bytes(data[offset:offset + 4], "little")
        if length < 5 or offset + length > size:
            raise ValueError(f"пошкоджений BSON на позиції {offset}")
        yield data[offset:offset + length]
        offset += length


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Медіана прогонів для кожної конфігурації + прискорення відносно 1 шарда."""
    groups: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (int(row["shards"]), int(row["writers"]), int(row["batch"]), int(row["docs"]))
        groups[key].append(row)
    summary = []
    for (shards, writers, batch, docs), items in sorted(groups.items()):
        summary.append({
            "shards": shards, "writers": writers, "batch": batch, "docs": docs,
            "runs": len(items),
            "docs_per_sec": round(statistics.median(float(r["docs_per_sec"]) for r in items)),
            "batch_p50_ms": round(statistics.median(float(r["batch_p50_ms"]) for r in items), 1),
            "batch_p95_ms": round(statistics.median(float(r["batch_p95_ms"]) for r in items), 1),
        })
    baseline = {(s["writers"], s["batch"], s["docs"]): s["docs_per_sec"]
                for s in summary if s["shards"] == 1}
    for s in summary:
        base = baseline.get((s["writers"], s["batch"], s["docs"]))
        s["speedup"] = round(s["docs_per_sec"] / base, 2) if base else None
    return summary


def read_results(path: Path = RESULTS_CSV) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def append_result(row: dict[str, Any], path: Path = RESULTS_CSV) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerow(row)


def load_meta() -> dict[str, Any]:
    if not META_FILE.exists():
        sys.exit("Немає підготовлених даних. Спершу виконайте: python scripts/bench_sharding.py prepare")
    return json.loads(META_FILE.read_text(encoding="utf-8"))


# ============================================================ prepare

def prepare(args: argparse.Namespace) -> None:
    import bson

    from app.application.dto.ingest import IngestDocumentDto
    from app.application.mappers import ingest_dto_to_medicine, medicine_id_for
    from app.domain.services.document_validator import DocumentValidator
    from app.infrastructure.persistence.mongo_document_adapter import MongoDocumentAdapter

    source = Path(args.file)
    if not source.exists():
        sys.exit(f"Немає файлу {source}. Його створює scripts/process_dataset.py (див. README).")

    started = time.perf_counter()
    validator = DocumentValidator()
    base = []
    with source.open(encoding="utf-8") as f:
        for line in f:
            if line.strip():
                medicine = ingest_dto_to_medicine(IngestDocumentDto.model_validate_json(line))
                validator.ensure_valid(medicine)
                base.append(medicine)
    total = len(base) * args.copies
    print(f"Базових документів: {len(base):,}; копій: {args.copies}; разом: {total:,}")

    order = list(range(total))
    random.Random(args.seed).shuffle(order)       # перемішування: класи йдуть урозкид

    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    files = [(BENCH_DIR / f"part-{p}.bson").open("wb") for p in range(PARTS)]
    counts, keys, size = [0] * PARTS, [], 0
    try:
        for position, index in enumerate(order):
            medicine = base[index % len(base)]
            copy = index // len(base)
            if copy:
                name = copy_name(medicine.name, copy)
                medicine = dataclasses.replace(medicine, name=name, medicine_id=medicine_id_for(name))
            document = MongoDocumentAdapter._to_document(medicine)
            data = bson.encode(document)
            files[position % PARTS].write(data)
            counts[position % PARTS] += 1
            size += len(data)
            keys.append((document["therapeutic_class"], document["medicine_id"]))
            if (position + 1) % 100_000 == 0:
                print(f"  закодовано {position + 1:,} / {total:,}")
    finally:
        for f in files:
            f.close()

    keys.sort()
    duplicates = sum(1 for a, b in zip(keys, keys[1:]) if a == b)
    meta = {
        "created": datetime.now().isoformat(timespec="seconds"),
        "source": str(source.relative_to(ROOT) if source.is_relative_to(ROOT) else source),
        "base_docs": len(base), "copies": args.copies, "seed": args.seed,
        "docs": total, "duplicates": duplicates, "bytes": size,
        "parts": counts,
        "split_points": {str(n * CHUNKS_PER_SHARD): split_points(keys, n * CHUNKS_PER_SHARD)
                         for n in range(1, MAX_SHARDS + 1)},
    }
    META_FILE.write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Готово за {time.perf_counter() - started:.0f} с: {total:,} документів, "
          f"{size / 2**20:.0f} МіБ, дублікатів ключа: {duplicates}")
    print(f"Файли: {BENCH_DIR}")


# ============================================================ підготовка колекції

def create_indexes(uri: str, database: str) -> None:
    """Ті самі вторинні індекси, що й в основній колекції (вони теж коштують часу запису)."""
    from pymongo import AsyncMongoClient

    from app.infrastructure.persistence.mongo_document_adapter import MongoDocumentAdapter

    async def _run() -> None:
        client = AsyncMongoClient(uri, serverSelectionTimeoutMS=5000)
        try:
            await MongoDocumentAdapter(client, database, COLLECTION).ensure_indexes()
        finally:
            await client.close()

    asyncio.run(_run())


def setup_collection(client, uri: str, database: str, shards: list[str], meta: dict[str, Any]) -> None:
    """Порожня шардована колекція, поділена на 4 x N чанків, по 4 на кожен із N шардів."""
    from pymongo import WriteConcern
    from pymongo.errors import OperationFailure

    ns = f"{database}.{COLLECTION}"
    admin = client.admin
    client[database][COLLECTION].drop()
    admin.command("enableSharding", database)
    client[database][COLLECTION].create_index(list(SHARD_KEY.items()), name="shard_key")
    admin.command("shardCollection", ns, key=SHARD_KEY)

    # Балансувальник і автоматичне злиття чанків для цієї колекції вимкнено:
    # розміщення чанків задаємо самі, і воно не повинно змінюватися під час запису.
    config_collections = client.config.get_collection(
        "collections", write_concern=WriteConcern("majority", wtimeout=60000))
    config_collections.update_one({"_id": ns}, {"$set": {"noBalance": True}})   # = sh.disableBalancing
    try:
        admin.command("configureCollectionBalancing", ns, enableAutoMerger=False)
    except OperationFailure:
        pass                     # старіші версії MongoDB не мають цього параметра

    chunks = len(shards) * CHUNKS_PER_SHARD
    for point in meta["split_points"][str(chunks)]:
        admin.command("split", ns, middle={"therapeutic_class": point[0], "medicine_id": point[1]})

    uuid = client.config.collections.find_one({"_id": ns})["uuid"]
    existing = list(client.config.chunks.find({"uuid": uuid}).sort("min", 1))
    for chunk, target in zip(existing, chunk_targets(len(existing), shards)):
        if chunk["shard"] != target:
            admin.command("moveChunk", ns, bounds=[chunk["min"], chunk["max"]], to=target)

    create_indexes(uri, database)


def collection_state(client, database: str) -> tuple[dict[str, int], dict[str, int]]:
    """Документи та чанки колекції на кожному шарді."""
    ns = f"{database}.{COLLECTION}"
    rows = client[database][COLLECTION].aggregate([{"$collStats": {"count": {}}}])
    docs = {row["shard"]: int(row["count"]) for row in rows}
    uuid = client.config.collections.find_one({"_id": ns})["uuid"]
    chunks: dict[str, int] = defaultdict(int)
    for chunk in client.config.chunks.find({"uuid": uuid}, {"shard": 1}):
        chunks[chunk["shard"]] += 1
    return dict(sorted(docs.items())), dict(sorted(chunks.items()))


# ============================================================ записувачі

def load_documents(paths: list[str], quota: int | None) -> list:
    from bson.raw_bson import RawBSONDocument

    documents = []
    for path in paths:
        for raw in iter_bson(Path(path).read_bytes()):
            if quota is not None and len(documents) >= quota:
                return documents
            documents.append(RawBSONDocument(raw))
    return documents


def writer_main(uri: str, database: str, paths: list[str], quota: int | None, batch_size: int,
                barrier, results) -> None:
    """Окремий процес-записувач: завантажує свою частину даних, чекає спільного старту, пише."""
    started = False
    try:
        from pymongo import MongoClient
        from pymongo.errors import BulkWriteError

        documents = load_documents(paths, quota)
        client = MongoClient(uri, maxPoolSize=2, serverSelectionTimeoutMS=5000)
        collection = client[database][COLLECTION]
        client.admin.command("ping")
        barrier.wait(timeout=600)
        started = True

        durations, inserted, duplicates = [], 0, 0
        for start in range(0, len(documents), batch_size):
            batch = documents[start:start + batch_size]
            t = time.perf_counter()
            try:
                inserted += len(collection.insert_many(batch, ordered=False).inserted_ids)
            except BulkWriteError as exc:
                errors = exc.details.get("writeErrors", [])
                if any(e.get("code") != DUPLICATE_KEY for e in errors):
                    raise
                duplicates += len(errors)
                inserted += int(exc.details.get("nInserted", 0))
            durations.append((time.perf_counter() - t) * 1000)
        client.close()
        results.put({"inserted": inserted, "duplicates": duplicates, "batches_ms": durations})
    except BaseException:  # noqa: BLE001 - помилку треба передати батьківському процесу
        if not started:
            barrier.abort()
        results.put({"error": traceback.format_exc()})


class DockerStatsSampler(threading.Thread):
    """Під час прогону періодично знімає `docker stats` контейнерів MongoDB (CPU, %)."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self._stop_event = threading.Event()
        self.samples: dict[str, list[float]] = defaultdict(list)
        self.available = shutil.which("docker") is not None

    def run(self) -> None:
        fmt = "{{.Name}};{{.CPUPerc}}"
        while self.available and not self._stop_event.is_set():
            try:
                out = subprocess.run(["docker", "stats", "--no-stream", "--format", fmt],
                                     capture_output=True, text=True, timeout=20).stdout
            except (OSError, subprocess.TimeoutExpired):
                return
            for line in out.splitlines():
                name, _, cpu = line.partition(";")
                if name.startswith("mongo") and cpu.endswith("%"):
                    try:
                        self.samples[name].append(float(cpu[:-1]))
                    except ValueError:
                        pass

    def stop(self) -> dict[str, dict[str, float]]:
        self._stop_event.set()
        self.join(timeout=30)
        return {name: {"avg": round(statistics.mean(v), 1), "max": round(max(v), 1)}
                for name, v in sorted(self.samples.items()) if v}


def run_once(uri: str, database: str, writers: int, batch: int, docs: int | None,
             docker_stats: bool) -> dict[str, Any]:
    ctx = mp.get_context("spawn")                  # однаково працює у Windows, macOS і Linux
    barrier = ctx.Barrier(writers + 1)
    results = ctx.Queue()
    parts = assign_parts(PARTS, writers)
    quotas = split_quota(docs, writers)
    processes = [
        ctx.Process(target=writer_main, args=(
            uri, database, [str(BENCH_DIR / f"part-{p}.bson") for p in parts[w]],
            quotas[w], batch, barrier, results))
        for w in range(writers)
    ]
    for p in processes:
        p.start()

    outcome: list[dict[str, Any]] = []
    try:
        try:
            barrier.wait(timeout=900)              # усі записувачі завантажили дані
        except threading.BrokenBarrierError:
            pass                                   # помилку покаже results нижче
        sampler = DockerStatsSampler() if docker_stats else None
        if sampler:
            sampler.start()
        t0 = time.perf_counter()
        while len(outcome) < writers:
            try:
                item = results.get(timeout=5)
            except queue_module.Empty:
                if any(p.exitcode not in (None, 0) for p in processes):
                    raise RuntimeError("процес-записувач аварійно завершився")
                continue
            if "error" in item:
                raise RuntimeError("помилка записувача:\n" + item["error"])
            outcome.append(item)
        seconds = time.perf_counter() - t0
        cpu = sampler.stop() if sampler else {}
    finally:
        for p in processes:
            p.join(timeout=30)
            if p.is_alive():
                p.terminate()

    durations = [d for item in outcome for d in item["batches_ms"]]
    inserted = sum(item["inserted"] for item in outcome)
    return {
        "seconds": seconds, "inserted": inserted,
        "duplicates": sum(item["duplicates"] for item in outcome),
        "docs_per_sec": inserted / seconds if seconds else 0.0,
        "p50": percentile(durations, 50), "p95": percentile(durations, 95), "cpu": cpu,
    }


# ============================================================ run

def run(args: argparse.Namespace) -> None:
    from pymongo import MongoClient

    from app.infrastructure.settings import load_settings

    meta = load_meta()
    settings = load_settings(args.config)
    uri, database = settings.mongo.uri, settings.mongo.database
    docs = min(args.docs, meta["docs"]) if args.docs else meta["docs"]

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    available = sorted(s["_id"] for s in client.admin.command("listShards")["shards"])
    print(f"Шарди кластера: {', '.join(available)}")
    for n in args.shards:
        if n > len(available):
            sys.exit(f"Потрібно {n} шард, а в кластері {len(available)}. Додайте шарди:\n"
                     "  docker compose -f docker/docker-compose.mongo.yml "
                     "-f docker/docker-compose.mongo.scale.yml up -d")
        if str(n * CHUNKS_PER_SHARD) not in meta["split_points"]:
            sys.exit(f"Для {n} шард немає меж чанків; перезапустіть prepare")
    for w in args.writers:
        if not 1 <= w <= PARTS:
            sys.exit(f"Записувачів може бути від 1 до {PARTS}")

    done: dict[tuple, int] = defaultdict(int)
    for row in read_results():
        done[(int(row["shards"]), int(row["writers"]), int(row["batch"]), int(row["docs"]))] += 1
    plan = []
    for n in args.shards:
        for w in args.writers:
            for b in args.batch:
                for r in range(done[(n, w, b, docs)] + 1, args.repeats + 1):
                    plan.append((n, w, b, r))
    skipped = len(args.shards) * len(args.writers) * len(args.batch) * args.repeats - len(plan)
    print(f"Документів на прогін: {docs:,}. Прогонів: {len(plan)}"
          + (f" (ще {skipped} вже виконано раніше, пропускаю)" if skipped else ""))

    for i, (n, w, b, r) in enumerate(plan, start=1):
        shards = available[:n]
        print(f"\n[{i}/{len(plan)}] шард: {n} ({', '.join(shards)}), записувачів: {w}, "
              f"пакет: {b}, прогін {r}/{args.repeats}")
        setup_collection(client, uri, database, shards, meta)
        result = run_once(uri, database, w, b, docs, not args.no_docker_stats)
        distribution, chunks = collection_state(client, database)
        stored = sum(distribution.values())
        if stored != result["inserted"]:
            print(f"  УВАГА: записано {result['inserted']:,}, а в колекції {stored:,}")
        append_result({
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "shards": n, "writers": w, "batch": b, "repeat": r, "docs": docs,
            "seconds": round(result["seconds"], 2),
            "docs_per_sec": round(result["docs_per_sec"]),
            "batch_p50_ms": round(result["p50"], 1), "batch_p95_ms": round(result["p95"], 1),
            "distribution": json.dumps(distribution), "chunks": json.dumps(chunks),
            "cpu": json.dumps(result["cpu"]),
        })
        mongos_cpu = result["cpu"].get("mongos", {}).get("avg")
        print(f"  {result['inserted']:,} документів за {result['seconds']:.1f} с = "
              f"{result['docs_per_sec']:,.0f} док/с; пакет p50 {result['p50']:.0f} мс, "
              f"p95 {result['p95']:.0f} мс"
              + (f"; CPU mongos {mongos_cpu}%" if mongos_cpu is not None else ""))
        print(f"  по шардах: {distribution}; чанків: {chunks}")

    client.close()
    print(f"\nРезультати: {RESULTS_CSV}")
    print("Колекція останнього прогону залишилася. Розподіл для скріншоту:\n"
          '  docker exec -it mongos mongosh medicine --quiet --eval '
          '"db.medicines_bench.getShardDistribution()"')


# ============================================================ report

def report(args: argparse.Namespace) -> None:
    rows = read_results()
    if not rows:
        sys.exit(f"Немає результатів у {RESULTS_CSV}. Спершу виконайте run.")
    docs_values = sorted({int(r["docs"]) for r in rows})
    docs = args.docs or max(docs_values, key=lambda d: sum(int(r["docs"]) == d for r in rows))
    summary = [s for s in summarize(rows) if s["docs"] == docs]

    header = ["shards", "writers", "batch", "runs", "docs_per_sec", "speedup", "batch_p50_ms", "batch_p95_ms"]
    print(f"Документів на прогін: {docs:,} (медіана з прогонів)\n")
    print(" | ".join(f"{h:>12}" for h in header))
    for s in summary:
        print(" | ".join(f"{str(s[h] if s[h] is not None else '-'):>12}" for h in header))

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with (RESULTS_DIR / "summary.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["docs"] + header)
        writer.writeheader()
        for s in summary:
            writer.writerow({k: s[k] for k in ["docs"] + header})
    print(f"\nТаблиця: {RESULTS_DIR / 'summary.csv'}")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("Графік не побудовано: встановіть matplotlib (pip install matplotlib) і повторіть report.")
        return

    batches = sorted({s["batch"] for s in summary})
    fig, axes = plt.subplots(1, len(batches), figsize=(6 * len(batches), 4.5), squeeze=False)
    for ax, batch in zip(axes[0], batches):
        for writers in sorted({s["writers"] for s in summary}):
            points = sorted((s["shards"], s["docs_per_sec"]) for s in summary
                            if s["batch"] == batch and s["writers"] == writers)
            if points:
                ax.plot([p[0] for p in points], [p[1] / 1000 for p in points],
                        marker="o", label=f"записувачів: {writers}")
        ax.set_title(f"Пакет {batch} документів")
        ax.set_xlabel("Кількість шард")
        ax.set_ylabel("Тис. документів за секунду")
        ax.set_xticks(sorted({s["shards"] for s in summary}))
        ax.set_ylim(bottom=0)                      # вісь від нуля, щоб не перебільшувати різницю
        ax.grid(alpha=0.3)
        ax.legend()
    fig.suptitle(f"Швидкість запису в MongoDB ({docs:,} документів, медіана)")
    fig.tight_layout()
    chart = RESULTS_DIR / "throughput.png"
    fig.savefig(chart, dpi=150)
    print(f"Графік: {chart}")


# ============================================================ clean

def clean(args: argparse.Namespace) -> None:
    from pymongo import MongoClient

    from app.infrastructure.settings import load_settings

    settings = load_settings(args.config)
    client = MongoClient(settings.mongo.uri, serverSelectionTimeoutMS=5000)
    client[settings.mongo.database][COLLECTION].drop()
    client.close()
    print(f"Колекцію {settings.mongo.database}.{COLLECTION} видалено")
    if args.files and BENCH_DIR.exists():
        shutil.rmtree(BENCH_DIR)
        print(f"Підготовлені файли {BENCH_DIR} видалено")


def main() -> None:
    parser = argparse.ArgumentParser(description="Бенчмарк запису MongoDB: 1 шард проти 3+")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"))
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("prepare", help="підготувати перемішані дані для бенчмарку")
    p.add_argument("--file", default=str(ROOT / "prepared" / "medicines.jsonl"))
    p.add_argument("--copies", type=int, default=5, help="скільки разів розмножити датасет (5 -> 1,1 млн)")
    p.add_argument("--seed", type=int, default=42)
    p.set_defaults(func=prepare)

    p = sub.add_parser("run", help="виконати прогони")
    p.add_argument("--shards", type=int, nargs="+", default=[1, 3])
    p.add_argument("--writers", type=int, nargs="+", default=[1, 4, 8])
    p.add_argument("--batch", type=int, nargs="+", default=[1000, 5000])
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--docs", type=int, default=None, help="записувати лише N документів (швидкий тест)")
    p.add_argument("--no-docker-stats", action="store_true", help="не знімати docker stats")
    p.set_defaults(func=run)

    p = sub.add_parser("report", help="таблиця медіан і графік")
    p.add_argument("--docs", type=int, default=None)
    p.set_defaults(func=report)

    p = sub.add_parser("clean", help="видалити колекцію medicines_bench")
    p.add_argument("--files", action="store_true", help="також видалити prepared/bench/")
    p.set_defaults(func=clean)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
