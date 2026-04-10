from __future__ import annotations

from fastapi import FastAPI

from . import downloads, files, maintenance, system, telegram_api, telegrams


def register_routers(app: FastAPI) -> None:
    app.include_router(system.router)
    app.include_router(telegram_api.router)
    app.include_router(telegrams.router)
    app.include_router(maintenance.router)
    app.include_router(files.router)
    app.include_router(downloads.router)
