"""Точка входу FastAPI для каталогу препаратів."""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pymongo import AsyncMongoClient

from app.application.queries.get_medicine_card_handler import (
    GetMedicineCardHandler,
)
from app.application.queries.list_medicines_handler import (
    ListMedicinesHandler,
)
from app.infrastructure.persistence.mongo_document_adapter import (
    MongoDocumentAdapter,
)
from app.infrastructure.settings import load_settings
from app.presentation.rest.medicine_controller import MedicineController


class UnavailableHandler:
    """Заглушка для команд, які ще не підключені."""

    async def handle(self, *args, **kwargs):
        raise HTTPException(
            status_code=501,
            detail="This operation is not implemented yet",
        )


def create_app(config_path: str = "config.yaml") -> FastAPI:
    settings = load_settings(config_path)

    mongo_client = AsyncMongoClient(
        settings.mongo.uri,
        serverSelectionTimeoutMS=3000,
    )

    # Infrastructure layer
    documents = MongoDocumentAdapter(
        mongo_client,
        settings.mongo.database,
    )

    # Application layer
    list_handler = ListMedicinesHandler(documents)
    card_handler = GetMedicineCardHandler(
        documents,
        cache=None,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        await mongo_client.close()

    # Presentation layer
    app = FastAPI(
        title="Medicine Catalog API",
        description="API for medicine catalog and filtering",
        version="1.0.0",
        lifespan=lifespan,
    )

    unavailable = UnavailableHandler()

    controller = MedicineController(
        list_medicines=list_handler,
        get_card=card_handler,
        create_medicine=unavailable,
        update_medicine=unavailable,
        delete_medicine=unavailable,
    )

    app.include_router(controller.router)

    # Головна сторінка каталогу
    @app.get("/", include_in_schema=False)
    async def home():
        html_path = (
            Path(__file__).resolve().parent
            / "presentation"
            / "web"
            / "index.html"
        )
        return FileResponse(html_path)

    # Перевірка роботи сервера
    @app.get("/health", tags=["system"])
    async def health():
        return {"status": "running"}

    return app
