
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"

df = pd.read_csv(RESULTS / "bench_search.csv")

summary = df.groupby("query", sort=False).median(numeric_only=True)

# Порівняння повного часу HTTP-запитів
ax = summary[["miss_ms", "hit_ms"]].plot(kind="bar", figsize=(11, 6))

ax.set_title("Порівняння часу HTTP-запитів")
ax.set_xlabel("Пошуковий запит")
ax.set_ylabel("Медіанний час, мс")
ax.legend(["Cache MISS", "Cache HIT"])
ax.tick_params(axis="x", rotation=15)
ax.grid(axis="y", alpha=0.3)

plt.tight_layout()
plt.savefig(RESULTS / "search_http_latency.png", dpi=300)
plt.close()

# Порівняння часу виконання обробника
ax = summary[
    ["handler_miss_ms", "handler_hit_ms"]
].plot(
    kind="bar",
    figsize=(11, 6)
)

ax.set_title("Порівняння часу виконання SearchMedicinesHandler")
ax.set_xlabel("Пошуковий запит")
ax.set_ylabel("Медіанний час, мс")
ax.legend(["Cache MISS", "Cache HIT"])
ax.tick_params(axis="x", rotation=15)
ax.grid(axis="y", alpha=0.3)

plt.tight_layout()
plt.savefig(RESULTS / "search_handler_latency.png", dpi=300)
plt.close()


# Коефіцієнт прискорення HTTP-запитів
speedup = summary["miss_ms"] / summary["hit_ms"]

ax = speedup.plot(kind="bar", figsize=(11, 6))

ax.set_title("Коефіцієнт прискорення пошуку з Redis")
ax.set_xlabel("Пошуковий запит")
ax.set_ylabel("Прискорення, разів")
ax.tick_params(axis="x", rotation=15)
ax.grid(axis="y", alpha=0.3)
ax.set_ylim(0, speedup.max() * 1.15)

for container in ax.containers:
    ax.bar_label(container, fmt="%.2f", padding=3)

plt.tight_layout()
plt.savefig(RESULTS / "search_speedup.png", dpi=300)
plt.close()

print("All benchmark plots saved successfully")
