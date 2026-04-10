from __future__ import annotations

import asyncio
import sqlite3
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from ..app_state import _tdlib_error_hint, _tdlib_manager_from_app
from ..config import AppConfig
from ..db import get_telegram_account
from ..deps import get_db
from ..maintenance import run_maintenance_backfills
from ..route_utils import _bool_or_none, _to_telegram_id

router = APIRouter()


def _int_or_default(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


@router.post("/telegram/{telegramId}/maintenance/run")
async def telegram_maintenance_run(
    telegramId: str,
    request: Request,
    payload: dict[str, Any] | None = None,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    td_manager = _tdlib_manager_from_app(request.app)
    if td_manager is None:
        raise HTTPException(status_code=503, detail=_tdlib_error_hint(request.app))

    telegram_id = _to_telegram_id(telegramId)
    config: AppConfig = request.app.state.config
    account = get_telegram_account(
        db,
        telegram_id=telegram_id,
        app_root=str(config.app_root),
    )
    if account is None:
        raise HTTPException(status_code=404, detail="Telegram account not found.")

    body = payload or {}
    limit = _int_or_default(body.get("limit"), 100)
    if limit <= 0:
        limit = 100
    if limit > 500:
        limit = 500

    run_album = _bool_or_none(body.get("album"))
    run_thumbnail = _bool_or_none(body.get("thumbnail"))
    include_album = True if run_album is None else run_album
    include_thumbnail = True if run_thumbnail is None else run_thumbnail
    if not include_album and not include_thumbnail:
        raise HTTPException(
            status_code=400,
            detail="At least one maintenance task must be enabled.",
        )

    result = await asyncio.to_thread(
        run_maintenance_backfills,
        db,
        td_manager,
        telegram_id=telegram_id,
        root_path=str(account.get("rootPath") or ""),
        limit=limit,
        run_album=include_album,
        run_thumbnail=include_thumbnail,
    )
    return {
        "telegramId": telegram_id,
        "limit": limit,
        **result,
    }
