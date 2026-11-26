"""Точка входу FastAPI для каталогу препаратів.

Об'єкти збирає Container (композиційний корінь): він створює адаптери БД
і підставляє їх в обробники. main.py лише вмикає контролери в застосунок.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from app.infrastructure.config.container import Container
from app.infrastructure.settings import load_settings
from app.presentation.rest.medicine_controller import MedicineController

WEB_DIR = Path(__file__).resolve().parent / "presentation" / "web"


class UnavailableHandler:
    """Заглушка для команд (create/update/delete), які ще не підключені до UI."""

    async def handle(self, *args, **kwargs):
        raise HTTPException(
            status_code=501,
            detail="This operation is not implemented yet",
        )


def create_app(config_path: str = "config.yaml") -> FastAPI:
    settings = load_settings(config_path)
    container = Container(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await container.start()
        yield
        await container.close()

    app = FastAPI(
        title="Medicine Catalog API",
        description="API for medicine catalog and filtering",
        version="1.1.0",
        lifespan=lifespan,
    )

    unavailable = UnavailableHandler()

    controller = MedicineController(
        list_medicines=container.list_medicines_handler(),
        get_card=container.get_medicine_card_handler(),
        create_medicine=unavailable,
        update_medicine=unavailable,
        delete_medicine=unavailable,
        get_facets=container.get_medicine_facets_handler(),
    )

    app.include_router(controller.router)

    # Головна сторінка каталогу
    @app.get("/", include_in_schema=False)
    async def home():
        return FileResponse(WEB_DIR / "index.html")

    # Перевірка роботи сервера
    @app.get("/health", tags=["system"])
    async def health():
        return {"status": "running"}

    return app
