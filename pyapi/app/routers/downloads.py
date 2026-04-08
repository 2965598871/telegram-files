from __future__ import annotations

import asyncio
import logging
import sqlite3
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from ..app_state import (
    EVENT_TYPE_FILE_STATUS,
    _build_ws_payload,
    _emit_ws_payload,
    _is_pending_account,
    _session_id_from_request,
    _tdlib_manager_from_app,
)
from ..config import AppConfig
from ..db import (
    cancel_file_download,
    get_telegram_account,
    remove_file_download,
    start_file_download,
    toggle_pause_file_download,
    update_auto_settings,
    update_chat_group_auto_settings,
)
from ..deps import get_db
from ..download_runtime import (
    _db_update_tdlib_file_status,
    _emit_tdlib_download_aggregate,
    _ensure_tdlib_download_monitor,
    _stop_tdlib_download_monitor,
    _td_file_status_payload,
    _tdlib_account_root_path,
)
from ..file_record_ops import (
    upsert_tdlib_file_record as _db_upsert_tdlib_file_record,
)
from ..route_utils import (
    _bool_or_none,
    _file_status_from_file_record,
    _int_or_default,
    _parse_batch_files,
)
from ..tdlib_downloads import (
    start_tdlib_download_for_message as _start_tdlib_download_for_message,
    tdlib_cancel_download_fallback as _tdlib_cancel_download_fallback,
    tdlib_remove_file_fallback as _tdlib_remove_file_fallback,
    tdlib_toggle_pause_download_fallback as _tdlib_toggle_pause_download_fallback,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/{telegramId}/file/start-download")
async def file_start_download_route(
    telegramId: int,
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    chat_id = _int_or_default(payload.get("chatId"), 0)
    message_id = _int_or_default(payload.get("messageId"), 0)
    file_id = _int_or_default(payload.get("fileId"), 0)
    if chat_id == 0 or message_id == 0 or file_id == 0:
        raise HTTPException(
            status_code=400, detail="chatId, messageId and fileId are required."
        )

    started_via_tdlib = False
    file_record: dict[str, Any] | None = None
    tdlib_start_error: str | None = None

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
                file_record = await asyncio.to_thread(
                    _start_tdlib_download_for_message,
                    td_manager,
                    db=db,
                    telegram_id=telegramId,
                    root_path=str(account.get("rootPath") or ""),
                    chat_id=chat_id,
                    message_id=message_id,
                    file_id=file_id,
                )
                started_via_tdlib = True
                _db_upsert_tdlib_file_record(db, file_payload=file_record)
            except Exception as exc:
                tdlib_start_error = str(exc)
                logger.warning(
                    "TDLib start download failed telegram=%s chat=%s message=%s file=%s: %s",
                    telegramId,
                    chat_id,
                    message_id,
                    file_id,
                    exc,
                )

    if file_record is None:
        if tdlib_start_error is not None:
            raise HTTPException(status_code=400, detail=tdlib_start_error)

        file_record = start_file_download(
            db,
            telegram_id=telegramId,
            chat_id=chat_id,
            message_id=message_id,
            file_id=file_id,
        )
        if file_record is None:
            raise HTTPException(status_code=404, detail="File not found")

    session_id = _session_id_from_request(request)
    status_payload = (
        _file_status_from_file_record(file_record)
        if "messageId" in file_record
        else _td_file_status_payload(file_record)
    )
    await _emit_ws_payload(
        _build_ws_payload(
            EVENT_TYPE_FILE_STATUS,
            status_payload,
        ),
        session_id=session_id,
    )

    if (
        started_via_tdlib
        and str(file_record.get("downloadStatus") or "").strip() == "downloading"
    ):
        unique_id = str(file_record.get("uniqueId") or "").strip()
        monitor_file_id = _int_or_default(file_record.get("id"), file_id)
        _ensure_tdlib_download_monitor(
            request.app,
            session_id=session_id,
            telegram_id=telegramId,
            file_id=monitor_file_id,
            unique_id=unique_id,
        )

    return file_record


@router.post("/{telegramId}/file/cancel-download")
async def file_cancel_download_route(
    telegramId: int,
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    file_id = _int_or_default(payload.get("fileId"), 0)
    if file_id == 0:
        raise HTTPException(status_code=400, detail="fileId is required.")

    result = cancel_file_download(
        db,
        telegram_id=telegramId,
        file_id=file_id,
        unique_id=str(payload.get("uniqueId") or "").strip() or None,
    )
    if result is None:
        td_manager = _tdlib_manager_from_app(request.app)
        if td_manager is None:
            raise HTTPException(status_code=404, detail="File not found")

        config: AppConfig = request.app.state.config
        account = get_telegram_account(
            db,
            telegram_id=telegramId,
            app_root=str(config.app_root),
        )
        if account is None:
            raise HTTPException(status_code=404, detail="File not found")

        unique_id = str(payload.get("uniqueId") or "").strip()
        try:
            result = await asyncio.to_thread(
                _tdlib_cancel_download_fallback,
                td_manager,
                telegram_id=telegramId,
                root_path=str(account.get("rootPath") or ""),
                file_id=file_id,
                unique_id=unique_id,
            )
            _db_update_tdlib_file_status(
                db,
                telegram_id=telegramId,
                file_id=file_id,
                unique_id=str(result.get("uniqueId") or unique_id),
                status_payload=result,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    session_id = _session_id_from_request(request)
    _stop_tdlib_download_monitor(
        session_id=session_id,
        telegram_id=telegramId,
        file_id=file_id,
    )
    await _emit_tdlib_download_aggregate(session_id=session_id, telegram_id=telegramId)
    await _emit_ws_payload(
        _build_ws_payload(EVENT_TYPE_FILE_STATUS, result),
        session_id=session_id,
    )
    return Response(status_code=200)


@router.post("/{telegramId}/file/toggle-pause-download")
async def file_toggle_pause_download_route(
    telegramId: int,
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    file_id = _int_or_default(payload.get("fileId"), 0)
    if file_id == 0:
        raise HTTPException(status_code=400, detail="fileId is required.")

    result = toggle_pause_file_download(
        db,
        telegram_id=telegramId,
        file_id=file_id,
        is_paused=_bool_or_none(payload.get("isPaused")),
        unique_id=str(payload.get("uniqueId") or "").strip() or None,
    )
    if result is None:
        td_manager = _tdlib_manager_from_app(request.app)
        if td_manager is None:
            raise HTTPException(status_code=404, detail="File not found")

        config: AppConfig = request.app.state.config
        account = get_telegram_account(
            db,
            telegram_id=telegramId,
            app_root=str(config.app_root),
        )
        if account is None:
            raise HTTPException(status_code=404, detail="File not found")

        unique_id = str(payload.get("uniqueId") or "").strip()
        try:
            result, should_monitor = await asyncio.to_thread(
                _tdlib_toggle_pause_download_fallback,
                td_manager,
                telegram_id=telegramId,
                root_path=str(account.get("rootPath") or ""),
                file_id=file_id,
                unique_id=unique_id,
                is_paused=_bool_or_none(payload.get("isPaused")),
            )
            _db_update_tdlib_file_status(
                db,
                telegram_id=telegramId,
                file_id=file_id,
                unique_id=str(result.get("uniqueId") or unique_id),
                status_payload=result,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        session_id = _session_id_from_request(request)
        if should_monitor:
            _ensure_tdlib_download_monitor(
                request.app,
                session_id=session_id,
                telegram_id=telegramId,
                file_id=file_id,
                unique_id=str(result.get("uniqueId") or unique_id),
            )
        else:
            _stop_tdlib_download_monitor(
                session_id=session_id,
                telegram_id=telegramId,
                file_id=file_id,
            )
            await _emit_tdlib_download_aggregate(
                session_id=session_id,
                telegram_id=telegramId,
            )
        await _emit_ws_payload(
            _build_ws_payload(EVENT_TYPE_FILE_STATUS, result),
            session_id=session_id,
        )
        return Response(status_code=200)

    session_id = _session_id_from_request(request)
    await _emit_ws_payload(
        _build_ws_payload(EVENT_TYPE_FILE_STATUS, result),
        session_id=session_id,
    )
    return Response(status_code=200)


@router.post("/{telegramId}/file/remove")
async def file_remove_route(
    telegramId: int,
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    file_id = _int_or_default(payload.get("fileId"), 0)
    unique_id = str(payload.get("uniqueId") or "").strip()
    if file_id == 0 and not unique_id:
        raise HTTPException(status_code=400, detail="fileId or uniqueId is required.")

    result = remove_file_download(
        db,
        telegram_id=telegramId,
        file_id=file_id,
        unique_id=unique_id or None,
    )
    if result is None:
        td_manager = _tdlib_manager_from_app(request.app)
        if td_manager is None:
            raise HTTPException(status_code=404, detail="File not found")

        config: AppConfig = request.app.state.config
        account = get_telegram_account(
            db,
            telegram_id=telegramId,
            app_root=str(config.app_root),
        )
        if account is None:
            raise HTTPException(status_code=404, detail="File not found")

        try:
            result = await asyncio.to_thread(
                _tdlib_remove_file_fallback,
                td_manager,
                telegram_id=telegramId,
                root_path=str(account.get("rootPath") or ""),
                file_id=file_id,
                unique_id=unique_id,
            )
            _db_update_tdlib_file_status(
                db,
                telegram_id=telegramId,
                file_id=file_id,
                unique_id=str(result.get("uniqueId") or unique_id),
                status_payload=result,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    session_id = _session_id_from_request(request)
    _stop_tdlib_download_monitor(
        session_id=session_id,
        telegram_id=telegramId,
        file_id=file_id,
    )
    await _emit_tdlib_download_aggregate(session_id=session_id, telegram_id=telegramId)
    await _emit_ws_payload(
        _build_ws_payload(EVENT_TYPE_FILE_STATUS, result),
        session_id=session_id,
    )
    return Response(status_code=200)


@router.post("/{telegramId}/file/update-auto-settings")
def file_update_auto_settings_route(
    telegramId: int,
    chatId: int = Query(default=0),
    payload: dict[str, Any] | None = None,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    if chatId == 0:
        raise HTTPException(status_code=400, detail="chatId is required.")

    if _is_pending_account(str(telegramId)):
        raise HTTPException(
            status_code=400,
            detail="Pending account does not support automation settings.",
        )

    auto_payload = payload if isinstance(payload, dict) else {}
    update_auto_settings(
        db,
        telegram_id=telegramId,
        chat_id=chatId,
        auto_payload=auto_payload,
    )
    return Response(status_code=200)


@router.post("/{telegramId}/chat-group/{groupId}/update-auto-settings")
def chat_group_update_auto_settings_route(
    telegramId: int,
    groupId: str,
    payload: dict[str, Any] | None = None,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    if _is_pending_account(str(telegramId)):
        raise HTTPException(
            status_code=400,
            detail="Pending account does not support automation settings.",
        )

    auto_payload = payload if isinstance(payload, dict) else {}
    updated = update_chat_group_auto_settings(
        db,
        telegram_id=telegramId,
        group_id=groupId,
        auto_payload=auto_payload,
    )
    if updated is None:
        raise HTTPException(status_code=404, detail="Group chat not found.")
    return Response(status_code=200)


@router.post("/files/start-download-multiple")
async def files_start_download_multiple(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    normalized_files = _parse_batch_files(payload)
    session_id = _session_id_from_request(request)
    td_manager = _tdlib_manager_from_app(request.app)
    root_path_cache: dict[int, str | None] = {}

    processed = 0
    failed = 0
    for item in normalized_files:
        if (
            item["telegramId"] <= 0
            or item["chatId"] == 0
            or item["messageId"] == 0
            or item["fileId"] == 0
        ):
            failed += 1
            continue

        file_record: dict[str, Any] | None = None
        started_via_tdlib = False
        tdlib_start_error = False
        if td_manager is not None:
            root_path = _tdlib_account_root_path(
                request.app,
                db,
                item["telegramId"],
                root_path_cache,
            )
            if root_path is not None:
                try:
                    file_record = await asyncio.to_thread(
                        _start_tdlib_download_for_message,
                        td_manager,
                        db=db,
                        telegram_id=item["telegramId"],
                        root_path=root_path,
                        chat_id=item["chatId"],
                        message_id=item["messageId"],
                        file_id=item["fileId"],
                    )
                    started_via_tdlib = True
                    _db_upsert_tdlib_file_record(db, file_payload=file_record)
                except Exception as exc:
                    tdlib_start_error = True
                    logger.warning(
                        "TDLib batch start failed telegram=%s chat=%s message=%s file=%s: %s",
                        item["telegramId"],
                        item["chatId"],
                        item["messageId"],
                        item["fileId"],
                        exc,
                    )

        if file_record is None:
            if tdlib_start_error:
                failed += 1
                continue

            file_record = start_file_download(
                db,
                telegram_id=item["telegramId"],
                chat_id=item["chatId"],
                message_id=item["messageId"],
                file_id=item["fileId"],
            )

            if file_record is None:
                failed += 1
                continue

        processed += 1
        status_payload = (
            _file_status_from_file_record(file_record)
            if "messageId" in file_record
            else _td_file_status_payload(file_record)
        )
        await _emit_ws_payload(
            _build_ws_payload(
                EVENT_TYPE_FILE_STATUS,
                status_payload,
            ),
            session_id=session_id,
        )

        if (
            started_via_tdlib
            and str(file_record.get("downloadStatus") or "").strip() == "downloading"
        ):
            _ensure_tdlib_download_monitor(
                request.app,
                session_id=session_id,
                telegram_id=item["telegramId"],
                file_id=_int_or_default(file_record.get("id"), item["fileId"]),
                unique_id=str(file_record.get("uniqueId") or ""),
            )

    if processed == 0 and failed > 0:
        raise HTTPException(
            status_code=400,
            detail="Failed to start download for the selected files.",
        )

    return {
        "processed": processed,
        "failed": failed,
    }


@router.post("/files/cancel-download-multiple")
async def files_cancel_download_multiple(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    normalized_files = _parse_batch_files(payload)
    session_id = _session_id_from_request(request)
    td_manager = _tdlib_manager_from_app(request.app)
    root_path_cache: dict[int, str | None] = {}
    changed_accounts: set[int] = set()

    processed = 0
    failed = 0
    for item in normalized_files:
        if item["telegramId"] <= 0 or item["fileId"] == 0:
            failed += 1
            continue

        result = cancel_file_download(
            db,
            telegram_id=item["telegramId"],
            file_id=item["fileId"],
            unique_id=item["uniqueId"] or None,
        )

        used_tdlib = False
        if result is None:
            if td_manager is not None:
                root_path = _tdlib_account_root_path(
                    request.app,
                    db,
                    item["telegramId"],
                    root_path_cache,
                )
                if root_path is not None:
                    try:
                        result = await asyncio.to_thread(
                            _tdlib_cancel_download_fallback,
                            td_manager,
                            telegram_id=item["telegramId"],
                            root_path=root_path,
                            file_id=item["fileId"],
                            unique_id=item["uniqueId"],
                        )
                        _db_update_tdlib_file_status(
                            db,
                            telegram_id=item["telegramId"],
                            file_id=item["fileId"],
                            unique_id=str(result.get("uniqueId") or item["uniqueId"]),
                            status_payload=result,
                        )
                        used_tdlib = True
                    except Exception:
                        result = None

            if result is None:
                failed += 1
                continue

        processed += 1
        if used_tdlib:
            _stop_tdlib_download_monitor(
                session_id=session_id,
                telegram_id=item["telegramId"],
                file_id=item["fileId"],
            )
            changed_accounts.add(item["telegramId"])

        await _emit_ws_payload(
            _build_ws_payload(EVENT_TYPE_FILE_STATUS, result),
            session_id=session_id,
        )

    for telegram_id in changed_accounts:
        await _emit_tdlib_download_aggregate(
            session_id=session_id,
            telegram_id=telegram_id,
        )

    return {
        "processed": processed,
        "failed": failed,
    }


@router.post("/files/toggle-pause-download-multiple")
async def files_toggle_pause_download_multiple(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    normalized_files = _parse_batch_files(payload)
    is_paused = _bool_or_none(payload.get("isPaused"))
    session_id = _session_id_from_request(request)
    td_manager = _tdlib_manager_from_app(request.app)
    root_path_cache: dict[int, str | None] = {}
    changed_accounts: set[int] = set()

    processed = 0
    failed = 0
    for item in normalized_files:
        if item["telegramId"] <= 0 or item["fileId"] == 0:
            failed += 1
            continue

        result = toggle_pause_file_download(
            db,
            telegram_id=item["telegramId"],
            file_id=item["fileId"],
            is_paused=is_paused,
            unique_id=item["uniqueId"] or None,
        )

        used_tdlib = False
        should_monitor = False
        if result is None:
            if td_manager is not None:
                root_path = _tdlib_account_root_path(
                    request.app,
                    db,
                    item["telegramId"],
                    root_path_cache,
                )
                if root_path is not None:
                    try:
                        result, should_monitor = await asyncio.to_thread(
                            _tdlib_toggle_pause_download_fallback,
                            td_manager,
                            telegram_id=item["telegramId"],
                            root_path=root_path,
                            file_id=item["fileId"],
                            unique_id=item["uniqueId"],
                            is_paused=is_paused,
                        )
                        _db_update_tdlib_file_status(
                            db,
                            telegram_id=item["telegramId"],
                            file_id=item["fileId"],
                            unique_id=str(result.get("uniqueId") or item["uniqueId"]),
                            status_payload=result,
                        )
                        used_tdlib = True
                    except Exception:
                        result = None

            if result is None:
                failed += 1
                continue

        processed += 1
        if used_tdlib:
            if should_monitor:
                _ensure_tdlib_download_monitor(
                    request.app,
                    session_id=session_id,
                    telegram_id=item["telegramId"],
                    file_id=item["fileId"],
                    unique_id=str(result.get("uniqueId") or item["uniqueId"]),
                )
            else:
                _stop_tdlib_download_monitor(
                    session_id=session_id,
                    telegram_id=item["telegramId"],
                    file_id=item["fileId"],
                )
                changed_accounts.add(item["telegramId"])

        await _emit_ws_payload(
            _build_ws_payload(EVENT_TYPE_FILE_STATUS, result),
            session_id=session_id,
        )

    for telegram_id in changed_accounts:
        await _emit_tdlib_download_aggregate(
            session_id=session_id,
            telegram_id=telegram_id,
        )

    return {
        "processed": processed,
        "failed": failed,
    }


@router.post("/files/remove-multiple")
async def files_remove_multiple(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    normalized_files = _parse_batch_files(payload)
    session_id = _session_id_from_request(request)
    td_manager = _tdlib_manager_from_app(request.app)
    root_path_cache: dict[int, str | None] = {}
    changed_accounts: set[int] = set()

    processed = 0
    failed = 0
    for item in normalized_files:
        if item["telegramId"] <= 0 or (item["fileId"] == 0 and not item["uniqueId"]):
            failed += 1
            continue

        result = remove_file_download(
            db,
            telegram_id=item["telegramId"],
            file_id=item["fileId"],
            unique_id=item["uniqueId"] or None,
        )

        used_tdlib = False
        if result is None:
            if td_manager is not None:
                root_path = _tdlib_account_root_path(
                    request.app,
                    db,
                    item["telegramId"],
                    root_path_cache,
                )
                if root_path is not None:
                    try:
                        result = await asyncio.to_thread(
                            _tdlib_remove_file_fallback,
                            td_manager,
                            telegram_id=item["telegramId"],
                            root_path=root_path,
                            file_id=item["fileId"],
                            unique_id=item["uniqueId"],
                        )
                        _db_update_tdlib_file_status(
                            db,
                            telegram_id=item["telegramId"],
                            file_id=item["fileId"],
                            unique_id=str(result.get("uniqueId") or item["uniqueId"]),
                            status_payload=result,
                        )
                        used_tdlib = True
                    except Exception:
                        result = None

            if result is None:
                failed += 1
                continue

        processed += 1
        if used_tdlib:
            _stop_tdlib_download_monitor(
                session_id=session_id,
                telegram_id=item["telegramId"],
                file_id=item["fileId"],
            )
            changed_accounts.add(item["telegramId"])

        await _emit_ws_payload(
            _build_ws_payload(EVENT_TYPE_FILE_STATUS, result),
            session_id=session_id,
        )

    for telegram_id in changed_accounts:
        await _emit_tdlib_download_aggregate(
            session_id=session_id,
            telegram_id=telegram_id,
        )

    return {
        "processed": processed,
        "failed": failed,
    }
