"""Етап 13: партиціонування журналів за часом (MongoMetricsAdapter.ensure_indexes)."""
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("pymongo")
from pymongo.errors import OperationFailure  # noqa: E402

from app.infrastructure.observability import mongo_metrics_adapter as module  # noqa: E402
from app.infrastructure.observability.mongo_metrics_adapter import MongoMetricsAdapter  # noqa: E402


class FakeDb:
    def __init__(self, names, ingest_type="collection"):
        self.collections = {n: MagicMock(name=n) for n in ("search_log", "ingest_log")}
        for name, coll in self.collections.items():
            coll.name = name
            coll.create_index = AsyncMock()
        self.list_collection_names = AsyncMock(return_value=names)
        self.create_collection = AsyncMock()
        self.command = AsyncMock()
        cursor = MagicMock()
        cursor.to_list = AsyncMock(return_value=[{"name": "ingest_log", "type": ingest_type}])
        self.list_collections = AsyncMock(return_value=cursor)

    def __getitem__(self, name):
        return self.collections[name]


def make(db):
    return MongoMetricsAdapter({"medicine": db}, "medicine")


async def test_new_ingest_log_is_time_series_with_expiry():
    db = FakeDb(names=[])
    await make(db).ensure_indexes()
    db.create_collection.assert_awaited_once()
    args, kwargs = db.create_collection.await_args
    assert args == ("ingest_log",)
    assert kwargs["timeseries"]["timeField"] == "created_at"
    assert kwargs["expireAfterSeconds"] == module.INGEST_LOG_TTL_DAYS * module.DAY_SECONDS


async def test_search_log_gets_ttl_index():
    db = FakeDb(names=["ingest_log", "search_log"], ingest_type="timeseries")
    await make(db).ensure_indexes()
    db.create_collection.assert_not_awaited()
    kwargs = db.collections["search_log"].create_index.await_args.kwargs
    assert kwargs["expireAfterSeconds"] == module.SEARCH_LOG_TTL_DAYS * module.DAY_SECONDS


async def test_existing_plain_index_is_converted_to_ttl_with_collmod():
    db = FakeDb(names=["ingest_log", "search_log"], ingest_type="timeseries")
    db.collections["search_log"].create_index = AsyncMock(
        side_effect=OperationFailure("Index already exists with different options", code=85))
    await make(db).ensure_indexes()
    args, kwargs = db.command.await_args
    assert args == ("collMod", "search_log")
    assert kwargs["index"]["expireAfterSeconds"] == module.SEARCH_LOG_TTL_DAYS * module.DAY_SECONDS


async def test_other_index_errors_are_not_hidden():
    db = FakeDb(names=["ingest_log", "search_log"], ingest_type="timeseries")
    db.collections["search_log"].create_index = AsyncMock(side_effect=OperationFailure("boom", code=2))
    with pytest.raises(OperationFailure):
        await make(db).ensure_indexes()
