"""Етап 13, п. 2: яке сховище тримає найбільше даних (обґрунтування, що шардується MongoDB).

Показує:
  1) MongoDB: для кожної колекції бази - кількість документів, обсяг даних,
     місце на диску (зі стисненням) та розмір індексів, сумарно по всіх шардах;
  2) Elasticsearch: розмір індексів через _cat/indices, якщо ES запущено;
  3) обсяг вихідних файлів prepared/ для кожного сховища - орієнтир, скільки даних
     кожне сховище отримує, навіть якщо деякі БД ще не запущені.
Результат також зберігається в results/storage_sizes.json (для звіту).

Запуск із кореня репозиторію (кластер MongoDB має працювати):
    python scripts/storage_sizes.py
"""
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.infrastructure.settings import load_settings  # noqa: E402

PREPARED = ROOT / "prepared"
SOURCES = {   # які підготовлені файли в яке сховище потрапляють (табл. 2.8 звіту)
    "MongoDB": ["medicines.jsonl"],
    "Elasticsearch": ["search_docs.jsonl"],
    "Neo4j": ["diseases.csv", "symptoms.csv", "treats.csv", "causes.csv", "analogs.csv"],
}
MIB = 2 ** 20


def mib(value: float) -> str:
    return f"{value / MIB:,.1f}"


def mongo_sizes(uri: str, database: str) -> list[dict]:
    from pymongo import MongoClient

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client[database]
    rows = []
    for name in sorted(db.list_collection_names()):
        if name.startswith("system."):
            continue
        totals = {"count": 0, "size": 0, "storageSize": 0, "totalIndexSize": 0}
        shards = []
        try:
            stats = db[name].aggregate([{"$collStats": {"storageStats": {}}}])
            for row in stats:
                storage = row.get("storageStats", {})
                shards.append(row.get("shard", "?"))
                for key in totals:
                    totals[key] += int(storage.get(key, 0) or 0)
        except Exception as exc:  # noqa: BLE001 - напр., колекції часових рядів у старих версіях
            print(f"  {name}: не вдалося отримати статистику ({exc})")
            continue
        rows.append({"collection": name, "shards": sorted(set(shards)), **totals})
    client.close()
    return rows


def elastic_sizes(url: str) -> list[dict] | None:
    try:
        with urllib.request.urlopen(f"{url.rstrip('/')}/_cat/indices?format=json&bytes=b",
                                    timeout=3) as response:
            data = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError):
        return None
    return [{"index": i["index"], "count": int(i.get("docs.count") or 0),
             "storageSize": int(i.get("store.size") or 0)}
            for i in data if not i["index"].startswith(".")]


def count_records(path: Path) -> int:
    """Кількість записів: рядки JSONL або рядки CSV без заголовка."""
    with path.open("rb") as f:
        lines = sum(1 for line in f if line.strip())
    return lines - 1 if path.suffix == ".csv" else lines


def source_sizes() -> dict[str, dict[str, int]]:
    result = {}
    for store, files in SOURCES.items():
        present = [PREPARED / f for f in files if (PREPARED / f).exists()]
        result[store] = {
            "bytes": sum(p.stat().st_size for p in present),
            "records": sum(count_records(p) for p in present),
        }
    return result


def main() -> None:
    settings = load_settings(str(ROOT / "config.yaml"))
    result: dict = {"created": datetime.now().isoformat(timespec="seconds")}

    print(f"MongoDB, база {settings.mongo.database} (сумарно по всіх шардах), МіБ")
    print(f"{'колекція':<18}{'документів':>12}{'дані':>10}{'на диску':>10}{'індекси':>10}  шарди")
    mongo = mongo_sizes(settings.mongo.uri, settings.mongo.database)
    for r in mongo:
        print(f"{r['collection']:<18}{r['count']:>12,}{mib(r['size']):>10}{mib(r['storageSize']):>10}"
              f"{mib(r['totalIndexSize']):>10}  {', '.join(r['shards'])}")
    main_rows = [r for r in mongo if r["collection"] != "medicines_bench"]
    mongo_disk = sum(r["storageSize"] + r["totalIndexSize"] for r in main_rows)
    print(f"Разом без medicines_bench: {mib(mongo_disk)} МіБ на диску (дані + індекси)")
    result["mongodb"] = mongo

    print("\nElasticsearch")
    elastic = elastic_sizes(settings.elastic.url)
    if elastic is None:
        print(f"  недоступний за адресою {settings.elastic.url} - порівняння нижче за вихідними файлами")
    else:
        for i in elastic:
            print(f"  індекс {i['index']}: {i['count']:,} документів, {mib(i['storageSize'])} МіБ")
    result["elasticsearch"] = elastic

    print("\nВихідні файли prepared/ за сховищами")
    print(f"  {'сховище':<14}{'МіБ':>8}{'записів':>12}{'байт/запис':>12}   файли")
    sources = source_sizes()
    for store, info in sources.items():
        files = ", ".join(SOURCES[store])
        if info["bytes"]:
            per_record = info["bytes"] / info["records"] if info["records"] else 0
            print(f"  {store:<14}{mib(info['bytes']):>8}{info['records']:>12,}{per_record:>12.0f}   {files}")
        else:
            print(f"  {store:<14}{'—':>8}{'—':>12}{'—':>12}   файлів немає")
    result["sources"] = sources

    print("\nЯк читати порівняння файлів:")
    print("  * файли Neo4j - це переважно списки ребер: кожен рядок містить два текстові UUID")
    print("    (~75 байт), тоді як у самій Neo4j зв'язок - запис фіксованого розміру, а id вузла")
    print("    зберігається один раз; тому розмір CSV не дорівнює обсягу даних у графі;")
    print("  * усі ребра Neo4j і текст full_description для Elasticsearch виведені з полів")
    print("    uses, side_effects, substitutes, action_class документів MongoDB;")
    print("  * повний документ препарату (усі поля) зберігається лише в MongoDB: кожен документ")
    print("    потоку запису потрапляє туди цілком, і лише в ній ростуть журнали search_log / ingest_log.")
    out = ROOT / "results" / "storage_sizes.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Збережено: {out}")


if __name__ == "__main__":
    main()
