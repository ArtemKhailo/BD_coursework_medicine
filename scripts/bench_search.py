"""Порівняння продуктивності пошуку з Redis та без кешу."""

import csv
import json
import sys
import time
import statistics

from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from redis import Redis

from app.application.queries.search_medicines_handler import SearchMedicinesHandler
from app.domain.services.query_normalizer import QueryNormalizer


API_URL = "http://127.0.0.1:8000/api/search"
REDIS_URL = "redis://127.0.0.1:6379"

QUERIES = [
    "paracetamol",
    "headache",
    "nausea",
    "bacterial infection",
    "paracetmol"
]

REPEATS = 10
LIMIT = 10

OUTPUT = ROOT / "results" / "bench_search.csv"


def execute_search(query: str):
    params = urlencode({"q": query, "limit": LIMIT})

    started = time.perf_counter()

    with urlopen(f"{API_URL}?{params}", timeout=120) as response:

        data = json.loads(response.read())

    elapsed_ms = (time.perf_counter() - started) * 1000

    return data, elapsed_ms


def main():
    redis = Redis.from_url(REDIS_URL, decode_responses=True)

    normalizer = QueryNormalizer()

    rows = []

    try:
        redis.ping()
        print("Warming up...")

        for query in QUERIES:
            execute_search(query)

        print("\nBenchmark started\n")

        for query in QUERIES:
            normalized = normalizer.normalize(query)

            key = SearchMedicinesHandler._cache_key(normalized,{}, LIMIT)

            miss_times = []
            hit_times = []

            for run in range(1, REPEATS + 1):
                # Видаляємо лише кеш поточного запиту
                redis.delete(key)

                # Пошук без кешу
                miss, miss_ms = execute_search(query)

                # Повторний пошук із кешем
                hit, hit_ms = execute_search(query)

                # Перевірка коректності
                assert miss["cache_hit"] is False
                assert hit["cache_hit"] is True
                assert miss["items"] == hit["items"]

                miss_times.append(miss_ms)
                hit_times.append(hit_ms)

                rows.append({
                    "query": query,
                    "run": run,
                    "results_count": miss["total"],
                    "miss_ms": round(miss_ms, 3),
                    "hit_ms": round(hit_ms, 3),
                    "handler_miss_ms": miss["latency_ms"],
                    "handler_hit_ms": hit["latency_ms"],
                    "speedup": round(miss_ms / hit_ms, 3)
                })

            miss_mean = statistics.mean(miss_times)
            hit_mean = statistics.mean(hit_times)

            miss_median = statistics.median(miss_times)
            hit_median = statistics.median(hit_times)

            speedup = miss_median / hit_median

            print(f"Query: {query}")
            print(f"Results: {miss['total']}")
            print(f"Mean MISS: {miss_mean:.2f} ms")
            print(f"Mean HIT: {hit_mean:.2f} ms")
            print(f"Median MISS: {miss_median:.2f} ms")
            print(f"Median HIT: {hit_median:.2f} ms")
            print(f"Speedup: {speedup:.2f}x")
            print("-" * 50)

            redis.delete(key)

        OUTPUT.parent.mkdir(parents=True, exist_ok=True)

        with OUTPUT.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))

            writer.writeheader()
            writer.writerows(rows)

        print(f"\nResults saved to: {OUTPUT}")
        print("Benchmark completed successfully")

    finally:
        redis.close()


if __name__ == "__main__":
    main()
