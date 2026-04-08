from __future__ import annotations

import asyncio
import logging
import sqlite3
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse

from ..app_state import _tdlib_error_hint, _tdlib_manager_from_app
from ..config import AppConfig
from ..db import (
    count_files_by_type,
    get_chat_group,
    get_file_preview_info,
    get_files_count,
    get_telegram_account,
    list_files,
    update_file_tags,
    update_files_tags,
)
from ..deps import get_db
from ..download_runtime import _tdlib_account_root_path
from ..file_record_ops import (
    upsert_tdlib_file_record as _db_upsert_tdlib_file_record,
)
from ..route_utils import _bool_or_none, _get_filters, _int_or_default
from ..tdlib_downloads import (
    cached_tdlib_file_preview as _cached_tdlib_file_preview,
    enrich_tdlib_thumbnails_for_files as _enrich_tdlib_thumbnails_for_files,
    media_type_for_path as _media_type_for_path,
    resolve_tdlib_preview_info as _resolve_tdlib_preview_info,
)
from ..tdlib_queries import (
    load_tdlib_chat_files as _load_tdlib_chat_files,
    load_tdlib_chat_files_count as _load_tdlib_chat_files_count,
    parse_link_files as _parse_link_files,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/files/count")
def files_count(db: sqlite3.Connection = Depends(get_db)) -> dict[str, int]:
    return get_files_count(db)


@router.get("/files")
def files(request: Request, db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    return list_files(db, telegram_id=None, chat_id=0, filters=_get_filters(request))


@router.get("/telegram/{telegramId}/chat/{chatId}/files")
async def telegram_files(
    telegramId: int,
    chatId: int,
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    filters = _get_filters(request)
    link = str(filters.get("link") or "").strip()
    if link:
        td_manager = _tdlib_manager_from_app(request.app)
        if td_manager is None:
            return JSONResponse(
                status_code=503,
                content={"error": _tdlib_error_hint(request.app)},
            )

        config: AppConfig = request.app.state.config
        account = get_telegram_account(
            db,
            telegram_id=telegramId,
            app_root=str(config.app_root),
        )
        if account is None:
            raise HTTPException(status_code=404, detail="Telegram account not found.")

        try:
            return await asyncio.to_thread(
                _parse_link_files,
                td_manager,
                db=db,
                telegram_id=telegramId,
                root_path=str(account.get("rootPath") or ""),
                link=link,
            )
        except Exception as exc:
            return JSONResponse(
                status_code=400,
                content={"error": str(exc)},
            )

    db_result = list_files(db, telegram_id=telegramId, chat_id=chatId, filters=filters)
    offline_requested = _bool_or_none(filters.get("offline")) is True
    if offline_requested:
        return db_result

    if _int_or_default(db_result.get("size"), 0) > 0:
        has_search = bool((filters.get("search") or "").strip())
        can_try_enrichment = (
            _int_or_default(filters.get("fromMessageId"), 0) == 0 and not has_search
        )
        if can_try_enrichment:
            td_manager = _tdlib_manager_from_app(request.app)
            if td_manager is not None:
                config: AppConfig = request.app.state.config
                account = get_telegram_account(
                    db,
                    telegram_id=telegramId,
                    app_root=str(config.app_root),
                )
                if account is not None:
                    try:
                        changed = await asyncio.to_thread(
                            _enrich_tdlib_thumbnails_for_files,
                            db,
                            td_manager,
                            telegram_id=telegramId,
                            root_path=str(account.get("rootPath") or ""),
                            files=[
                                item
                                for item in (
                                    db_result.get("files")
                                    if isinstance(db_result.get("files"), list)
                                    else []
                                )
                                if isinstance(item, dict)
                            ],
                            upsert_file_record=_db_upsert_tdlib_file_record,
                        )
                        if changed:
                            return list_files(
                                db,
                                telegram_id=telegramId,
                                chat_id=chatId,
                                filters=filters,
                            )
                    except Exception as exc:
                        logger.warning(
                            "Failed to enrich thumbnails for telegram=%s chat=%s: %s",
                            telegramId,
                            chatId,
                            exc,
                        )

        return db_result

    td_manager = _tdlib_manager_from_app(request.app)
    if td_manager is None:
        return db_result

    config: AppConfig = request.app.state.config
    account = get_telegram_account(
        db,
        telegram_id=telegramId,
        app_root=str(config.app_root),
    )
    if account is None:
        return db_result

    try:
        return await asyncio.to_thread(
            _load_tdlib_chat_files,
            td_manager,
            db=db,
            telegram_id=telegramId,
            root_path=str(account.get("rootPath") or ""),
            chat_id=chatId,
            filters=filters,
        )
    except Exception as exc:
        logger.warning(
            "Failed to fetch chat files from TDLib for telegram=%s chat=%s: %s",
            telegramId,
            chatId,
            exc,
        )
        return db_result


@router.get("/telegram/{telegramId}/chat-group/{groupId}/files")
def telegram_chat_group_files(
    telegramId: int,
    groupId: str,
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    group = get_chat_group(db, telegram_id=telegramId, group_id=groupId)
    if group is None:
        raise HTTPException(status_code=404, detail="Group chat not found.")

    raw_chat_ids = group.get("chatIds")
    chat_ids = (
        [int(item) for item in raw_chat_ids if str(item).strip()]
        if isinstance(raw_chat_ids, list)
        else []
    )
    return list_files(
        db,
        telegram_id=telegramId,
        chat_id=0,
        chat_ids=chat_ids,
        filters=_get_filters(request),
    )


@router.get("/telegram/{telegramId}/chat/{chatId}/files/count")
async def telegram_files_count(
    telegramId: int,
    chatId: int,
    request: Request,
    offline: bool = Query(default=False),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, int]:
    db_result = count_files_by_type(db, telegram_id=telegramId, chat_id=chatId)
    if offline:
        return db_result

    td_manager = _tdlib_manager_from_app(request.app)
    if td_manager is None:
        return db_result

    root_path = _tdlib_account_root_path(request.app, db, telegramId)
    if root_path is None:
        return db_result

    try:
        return await asyncio.to_thread(
            _load_tdlib_chat_files_count,
            td_manager,
            telegram_id=telegramId,
            root_path=root_path,
            chat_id=chatId,
        )
    except Exception as exc:
        logger.warning(
            "Failed to fetch live chat counts for telegram=%s chat=%s: %s",
            telegramId,
            chatId,
            exc,
        )
        return db_result


@router.get("/telegram/{telegramId}/chat-group/{groupId}/files/count")
def telegram_chat_group_files_count(
    telegramId: int,
    groupId: str,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, int]:
    group = get_chat_group(db, telegram_id=telegramId, group_id=groupId)
    if group is None:
        raise HTTPException(status_code=404, detail="Group chat not found.")

    raw_chat_ids = group.get("chatIds")
    chat_ids = (
        [int(item) for item in raw_chat_ids if str(item).strip()]
        if isinstance(raw_chat_ids, list)
        else []
    )
    return count_files_by_type(
        db,
        telegram_id=telegramId,
        chat_id=0,
        chat_ids=chat_ids,
    )


@router.post("/files/update-tags")
def files_update_tags(
    payload: dict[str, Any],
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    files = payload.get("files")
    if not isinstance(files, list):
        raise HTTPException(status_code=400, detail="'files' must be an array.")

    tags = payload.get("tags")
    normalized_tags = "" if tags is None else str(tags)

    unique_ids: list[str] = []
    for file_item in files:
        if not isinstance(file_item, dict):
            continue
        unique_id = str(file_item.get("uniqueId") or "").strip()
        if unique_id:
            unique_ids.append(unique_id)

    update_files_tags(db, unique_ids, normalized_tags)
    return Response(status_code=200)


@router.post("/file/{uniqueId}/update-tags")
def file_update_tags(
    uniqueId: str,
    payload: dict[str, Any],
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    normalized_unique_id = uniqueId.strip()
    if not normalized_unique_id:
        raise HTTPException(
            status_code=400, detail="Path parameter 'uniqueId' is required."
        )

    tags = payload.get("tags")
    normalized_tags = "" if tags is None else str(tags)
    update_file_tags(db, normalized_unique_id, normalized_tags)
    return Response(status_code=200)


@router.get("/{telegramId}/file/{uniqueId}")
async def file_preview(
    telegramId: int,
    uniqueId: str,
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> FileResponse:
    info = get_file_preview_info(
        db,
        telegram_id=telegramId,
        unique_id=uniqueId,
    )
    if info is None:
        cached = _cached_tdlib_file_preview(telegram_id=telegramId, unique_id=uniqueId)
        if cached is not None:
            path_raw = str(cached.get("path") or "").strip()
            if path_raw:
                path_obj = Path(path_raw)
                if path_obj.exists() and path_obj.is_file():
                    return FileResponse(
                        path=str(path_obj),
                        media_type=_media_type_for_path(
                            str(path_obj), str(cached.get("mimeType") or "")
                        ),
                    )

        td_manager = _tdlib_manager_from_app(request.app)
        if td_manager is not None:
            config: AppConfig = request.app.state.config
            account = get_telegram_account(
                db,
                telegram_id=telegramId,
                app_root=str(config.app_root),
            )
            if account is not None:
                try:
                    info = await asyncio.to_thread(
                        _resolve_tdlib_preview_info,
                        td_manager,
                        telegram_id=telegramId,
                        root_path=str(account.get("rootPath") or ""),
                        unique_id=uniqueId,
                    )
                except Exception:
                    info = None

        if info is None:
            raise HTTPException(status_code=404, detail="File not found")

    path = Path(str(info["path"]))
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        path=str(path),
        media_type=str(info.get("mimeType") or "application/octet-stream"),
    )
