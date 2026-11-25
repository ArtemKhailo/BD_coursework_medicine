"""Налаштування застосунку: адреси БД, логування, параметри пакетів і черги.

Читаються з config.yaml; змінні середовища (.env) мають вищий пріоритет.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass
class MongoSettings:
    uri: str = "mongodb://localhost:27017"   # адреса mongos
    database: str = "medicine"


@dataclass
class ElasticSettings:
    url: str = "http://localhost:9200"
    index: str = "medicines"


@dataclass
class Neo4jSettings:
    uri: str = "bolt://localhost:7687"
    user: str = "neo4j"
    password: str = "change_me"


@dataclass
class RedisSettings:
    url: str = "redis://localhost:6379"
    search_ttl_seconds: int = 60


@dataclass
class RabbitSettings:
    url: str = "amqp://guest:guest@localhost:5672/"
    queue: str = "medicine.ingest"
    prefetch: int = 10
    max_length: int = 1000          # обмеження довжини черги


@dataclass
class IngestSettings:
    batch_size: int = 5000


@dataclass
class LoggingSettings:
    level: str = "INFO"             # ERROR / WARNING / INFO / DEBUG


@dataclass
class Settings:
    mongo: MongoSettings = field(default_factory=MongoSettings)
    elastic: ElasticSettings = field(default_factory=ElasticSettings)
    neo4j: Neo4jSettings = field(default_factory=Neo4jSettings)
    redis: RedisSettings = field(default_factory=RedisSettings)
    rabbit: RabbitSettings = field(default_factory=RabbitSettings)
    ingest: IngestSettings = field(default_factory=IngestSettings)
    logging: LoggingSettings = field(default_factory=LoggingSettings)


# Змінна середовища -> (секція, поле). Імена збігаються з .env.example.
ENV_OVERRIDES: dict[str, tuple[str, str]] = {
    "MONGO_URI": ("mongo", "uri"),
    "MONGO_DATABASE": ("mongo", "database"),
    "ELASTIC_URL": ("elastic", "url"),
    "NEO4J_URI": ("neo4j", "uri"),
    "NEO4J_USER": ("neo4j", "user"),
    "NEO4J_PASSWORD": ("neo4j", "password"),
    "REDIS_URL": ("redis", "url"),
    "RABBITMQ_URL": ("rabbit", "url"),
    "INGEST_BATCH_SIZE": ("ingest", "batch_size"),
    "LOG_LEVEL": ("logging", "level"),
}


def _read_dotenv(path: Path) -> dict[str, str]:
    """Простий розбір .env: рядки KEY=VALUE, коментарі # і порожні рядки пропускаються."""
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _convert(value: Any, current: Any, where: str) -> Any:
    """Приводить значення до типу поля за замовчуванням (int, bool або str)."""
    if isinstance(current, bool):
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in ("1", "true", "yes", "on")
    if isinstance(current, int):
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{where}: очікується ціле число, отримано {value!r}") from exc
    return str(value)


def _apply_section(section: Any, values: dict[str, Any], name: str) -> None:
    known = {f.name for f in fields(section)}
    for key, value in values.items():
        if key not in known:
            raise ValueError(f"config: невідомий параметр {name}.{key}")
        setattr(section, key, _convert(value, getattr(section, key), f"{name}.{key}"))


def load_settings(path: str | Path = "config.yaml") -> Settings:
    """Читає YAML, накладає змінні середовища, повертає Settings.

    Пріоритет (від нижчого до вищого): значення за замовчуванням -> config.yaml ->
    файл .env поруч із config.yaml -> справжні змінні середовища.
    Якщо config.yaml немає, використовуються значення за замовчуванням.
    """
    path = Path(path)
    settings = Settings()

    if path.is_file():
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(data, dict):
            raise ValueError(f"{path}: очікується словник секцій")
        for name, values in data.items():
            section = getattr(settings, name, None)
            if section is None or not is_dataclass(section):
                raise ValueError(f"config: невідома секція {name}")
            if values:
                _apply_section(section, values, name)

    env = _read_dotenv(path.parent / ".env")
    env.update({k: v for k, v in os.environ.items() if k in ENV_OVERRIDES})
    for var, (name, attr) in ENV_OVERRIDES.items():
        if var in env:
            section = getattr(settings, name)
            setattr(section, attr, _convert(env[var], getattr(section, attr), var))
    return settings
