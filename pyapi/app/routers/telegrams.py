from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from ..app_state import (
    EVENT_TYPE_AUTHORIZATION,
    PENDING_TELEGRAMS,
    SESSION_TELEGRAM_SELECTION,
    STATE_LOCK,
    TELEGRAM_CONSTRUCTOR_WAIT_PHONE_NUMBER,
    PendingTelegramAccount,
    _auth_state,
    _build_ws_payload,
    _emit_ws_payload,
    _is_pending_account,
    _pending_account_to_response,
    _remove_pending_account,
    _session_id_from_request,
    _tdlib_error_hint,
    _tdlib_manager_from_app,
    update_session_selection,
)
from ..config import AppConfig
from ..db import (
    create_chat_group,
    delete_chat_group,
    delete_telegram,
    get_automation_map,
    get_chat_group,
    get_telegram_account,
    get_telegram_download_statistics,
    get_telegram_download_statistics_by_phase,
    get_telegram_ping_seconds,
    list_chat_groups,
    list_chats,
    list_telegrams,
    update_chat_group,
    update_telegram_proxy,
)
from ..deps import get_db
from ..download_runtime import (
    _apply_chat_auto_settings,
    _avg_speed_interval,
    _live_speed_stats,
    _tdlib_account_root_path,
)
from ..route_utils import _to_telegram_id
from ..tdlib import TdlibConfigurationError
from ..tdlib_queries import (
    load_tdlib_chats as _load_tdlib_chats,
    load_tdlib_network_statistics as _load_tdlib_network_statistics,
    load_tdlib_ping_seconds as _load_tdlib_ping_seconds,
    tdlib_test_network as _tdlib_test_network,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/telegrams")
def telegrams(
    request: Request,
    authorized: bool | None = Query(default=None),
    db: sqlite3.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    config: AppConfig = request.app.state.config
    active_accounts = list_telegrams(db, str(config.app_root), None)
    with STATE_LOCK:
        pending_accounts = [
            _pending_account_to_response(p) for p in PENDING_TELEGRAMS.values()
        ]

    all_accounts = [*active_accounts, *pending_accounts]
    if authorized is None:
        return all_accounts

    target_status = "active" if authorized else "inactive"
    return [
        account for account in all_accounts if account.get("status") == target_status
    ]


@router.post("/telegrams/change")
def telegrams_change(request: Request) -> Response:
    session_id = _session_id_from_request(request)
    telegram_id = (request.query_params.get("telegramId") or "").strip()
    update_session_selection(session_id, telegram_id or None)
    return Response(status_code=200)


@router.post("/telegram/create")
async def telegram_create(
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    del db
    config: AppConfig = request.app.state.config
    td_manager = _tdlib_manager_from_app(request.app)
    if td_manager is None:
        raise HTTPException(status_code=503, detail=_tdlib_error_hint(request.app))

    session_id = _session_id_from_request(request)
    proxy_name_raw = payload.get("proxyName")
    proxy_name = (
        str(proxy_name_raw).strip()
        if proxy_name_raw is not None and str(proxy_name_raw).strip()
        else None
    )

    with STATE_LOCK:
        selected_id = SESSION_TELEGRAM_SELECTION.get(session_id)
        pending = PENDING_TELEGRAMS.get(selected_id) if selected_id else None
        if pending is None:
            pending_id = f"pending-{uuid4().hex[:8]}"
            pending = PendingTelegramAccount(
                id=pending_id,
                name="Pending Account",
                root_path=str(config.app_root / "account" / pending_id),
                proxy=proxy_name,
                phone_number="",
                last_authorization_state=_auth_state(
                    TELEGRAM_CONSTRUCTOR_WAIT_PHONE_NUMBER
                ),
            )
            PENDING_TELEGRAMS[pending_id] = pending
            SESSION_TELEGRAM_SELECTION[session_id] = pending_id
        elif proxy_name is not None:
            pending.proxy = proxy_name

        last_state = dict(pending.last_authorization_state)
        account_id = pending.id

    try:
        await asyncio.to_thread(
            td_manager.ensure_session, account_id, pending.root_path
        )
    except TdlibConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initialize TDLib session: {exc}",
        ) from exc

    await _emit_ws_payload(
        _build_ws_payload(
            EVENT_TYPE_AUTHORIZATION,
            last_state,
        ),
        session_id=session_id,
    )
    return {
        "id": account_id,
        "lastState": last_state,
    }


@router.post("/telegram/{telegramId}/delete")
def telegram_delete(
    telegramId: str,
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    td_manager = _tdlib_manager_from_app(request.app)
    if _is_pending_account(telegramId):
        _remove_pending_account(telegramId, td_manager=td_manager)
        return Response(status_code=200)
    delete_telegram(db, _to_telegram_id(telegramId))
    return Response(status_code=200)


@router.get("/telegram/{telegramId}/chats")
async def telegram_chats(
    telegramId: str,
    request: Request,
    query: str = Query(default=""),
    archived: bool = Query(default=False),
    chatId: str | None = Query(default=None),
    db: sqlite3.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    if _is_pending_account(telegramId):
        return []

    telegram_id_num = _to_telegram_id(telegramId)
    activated_chat_id = None
    if chatId is not None and chatId.strip() != "":
        try:
            activated_chat_id = int(chatId)
        except ValueError:
            activated_chat_id = None

    db_chats = list_chats(
        db,
        telegram_id=telegram_id_num,
        query=query,
        activated_chat_id=activated_chat_id,
    )
    automation_map = get_automation_map(db, telegram_id=telegram_id_num)
    db_chats = _apply_chat_auto_settings(
        db_chats,
        telegram_id=telegram_id_num,
        automation_map=automation_map,
    )

    td_manager = _tdlib_manager_from_app(request.app)
    if td_manager is None:
        return db_chats

    config: AppConfig = request.app.state.config
    account = get_telegram_account(
        db,
        telegram_id=telegram_id_num,
        app_root=str(config.app_root),
    )
    if account is None:
        return db_chats

    try:
        td_chats = await asyncio.to_thread(
            _load_tdlib_chats,
            td_manager,
            telegram_id=telegram_id_num,
            root_path=str(account.get("rootPath") or ""),
            query=query,
            archived=archived,
            activated_chat_id=activated_chat_id,
        )
    except Exception as exc:
        logger.warning("Failed to fetch chats from TDLib: %s", exc)
        return db_chats

    target_chats = td_chats if td_chats else db_chats
    return _apply_chat_auto_settings(
        target_chats,
        telegram_id=telegram_id_num,
        automation_map=automation_map,
    )


@router.get("/telegram/{telegramId}/chat-groups")
def telegram_chat_groups(
    telegramId: str,
    query: str = Query(default=""),
    chatId: str | None = Query(default=None),
    db: sqlite3.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    if _is_pending_account(telegramId):
        return []

    normalized_telegram_id = _to_telegram_id(telegramId)
    activated_group_id = None
    if chatId and chatId.startswith("group:"):
        activated_group_id = chatId.split(":", 1)[1].strip() or None

    return list_chat_groups(
        db,
        telegram_id=normalized_telegram_id,
        query=query,
        activated_group_id=activated_group_id,
    )


@router.post("/telegram/{telegramId}/chat-groups")
def telegram_chat_group_create(
    telegramId: str,
    payload: dict[str, Any] | None = None,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    if _is_pending_account(telegramId):
        raise HTTPException(
            status_code=400,
            detail="Pending account does not support group chats.",
        )

    normalized_payload = payload if isinstance(payload, dict) else {}
    try:
        return create_chat_group(
            db,
            telegram_id=_to_telegram_id(telegramId),
            group_id=uuid4().hex,
            name=str(normalized_payload.get("name") or ""),
            chat_ids=(
                normalized_payload.get("chatIds")
                if isinstance(normalized_payload.get("chatIds"), list)
                else []
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/telegram/{telegramId}/chat-groups/{groupId}")
def telegram_chat_group_update(
    telegramId: str,
    groupId: str,
    payload: dict[str, Any] | None = None,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    if _is_pending_account(telegramId):
        raise HTTPException(
            status_code=400,
            detail="Pending account does not support group chats.",
        )

    normalized_payload = payload if isinstance(payload, dict) else {}
    try:
        group = update_chat_group(
            db,
            telegram_id=_to_telegram_id(telegramId),
            group_id=groupId,
            name=str(normalized_payload.get("name") or ""),
            chat_ids=(
                normalized_payload.get("chatIds")
                if isinstance(normalized_payload.get("chatIds"), list)
                else []
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if group is None:
        raise HTTPException(status_code=404, detail="Group chat not found.")
    return group


@router.post("/telegram/{telegramId}/chat-groups/{groupId}/delete")
def telegram_chat_group_delete(
    telegramId: str,
    groupId: str,
    db: sqlite3.Connection = Depends(get_db),
) -> Response:
    if _is_pending_account(telegramId):
        raise HTTPException(
            status_code=400,
            detail="Pending account does not support group chats.",
        )

    normalized_telegram_id = _to_telegram_id(telegramId)
    group = get_chat_group(db, telegram_id=normalized_telegram_id, group_id=groupId)
    if group is None:
        raise HTTPException(status_code=404, detail="Group chat not found.")

    delete_chat_group(db, telegram_id=normalized_telegram_id, group_id=groupId)
    return Response(status_code=200)


@router.get("/telegram/{telegramId}/download-statistics")
async def telegram_download_statistics(
    telegramId: str,
    request: Request,
    type: str | None = Query(default=None),
    timeRange: int = Query(default=1),
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    if _is_pending_account(telegramId):
        if type == "phase":
            return {"speedStats": [], "completedStats": []}
        return {
            "total": 0,
            "downloading": 0,
            "paused": 0,
            "completed": 0,
            "error": 0,
            "photo": 0,
            "video": 0,
            "audio": 0,
            "file": 0,
            "networkStatistics": {
                "sinceDate": int(time.time()),
                "sentBytes": 0,
                "receivedBytes": 0,
            },
            "speedStats": {
                "interval": _avg_speed_interval(db),
                "avgSpeed": 0,
                "medianSpeed": 0,
                "maxSpeed": 0,
                "minSpeed": 0,
            },
        }

    normalized_telegram_id = _to_telegram_id(telegramId)
    if type == "phase":
        return get_telegram_download_statistics_by_phase(
            db, normalized_telegram_id, timeRange
        )

    result = get_telegram_download_statistics(db, normalized_telegram_id)
    result["speedStats"] = _live_speed_stats(db, telegram_id=normalized_telegram_id)

    td_manager = _tdlib_manager_from_app(request.app)
    if td_manager is None:
        return result

    root_path = _tdlib_account_root_path(request.app, db, normalized_telegram_id)
    if root_path is None:
        return result

    try:
        result["networkStatistics"] = await asyncio.to_thread(
            _load_tdlib_network_statistics,
            td_manager,
            telegram_id=normalized_telegram_id,
            root_path=root_path,
        )
    except Exception as exc:
        logger.warning(
            "Failed to fetch network statistics for telegram=%s: %s",
            normalized_telegram_id,
            exc,
        )

    return result


@router.post("/telegram/{telegramId}/toggle-proxy")
def telegram_toggle_proxy(
    telegramId: str,
    payload: dict[str, Any],
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, Any]:
    config: AppConfig = request.app.state.config
    raw_proxy_name = payload.get("proxyName")
    proxy_name = None
    if raw_proxy_name is not None and str(raw_proxy_name).strip() != "":
        proxy_name = str(raw_proxy_name).strip()

    if _is_pending_account(telegramId):
        with STATE_LOCK:
            pending = PENDING_TELEGRAMS.get(telegramId)
            if pending is not None:
                pending.proxy = proxy_name
        return {"proxy": proxy_name}

    proxy = update_telegram_proxy(
        db,
        telegram_id=_to_telegram_id(telegramId),
        proxy_name=proxy_name,
        app_root=str(config.app_root),
    )
    return {"proxy": proxy}


@router.get("/telegram/{telegramId}/ping")
async def telegram_ping(
    telegramId: str,
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, float]:
    if _is_pending_account(telegramId):
        with STATE_LOCK:
            pending = PENDING_TELEGRAMS.get(telegramId)
            seconds = 0.08 if pending is not None and pending.proxy else 0.0
        return {"ping": seconds}

    telegram_id_num = _to_telegram_id(telegramId)
    td_manager = _tdlib_manager_from_app(request.app)
    if td_manager is None:
        return {"ping": get_telegram_ping_seconds(db, telegram_id_num)}

    root_path = _tdlib_account_root_path(request.app, db, telegram_id_num)
    if root_path is None:
        raise HTTPException(status_code=404, detail="Telegram account not found.")

    try:
        seconds = await asyncio.to_thread(
            _load_tdlib_ping_seconds,
            td_manager,
            telegram_id=telegram_id_num,
            root_path=root_path,
        )
        return {"ping": seconds}
    except Exception as exc:
        logger.warning(
            "Failed to ping TDLib proxy for telegram=%s: %s",
            telegram_id_num,
            exc,
        )
        return {"ping": get_telegram_ping_seconds(db, telegram_id_num)}


@router.get("/telegram/{telegramId}/test-network")
async def telegram_test_network(
    telegramId: str,
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, bool]:
    if _is_pending_account(telegramId):
        return {"success": True}

    telegram_id_num = _to_telegram_id(telegramId)
    td_manager = _tdlib_manager_from_app(request.app)
    if td_manager is None:
        return {"success": True}

    root_path = _tdlib_account_root_path(request.app, db, telegram_id_num)
    if root_path is None:
        raise HTTPException(status_code=404, detail="Telegram account not found.")

    try:
        success = await asyncio.to_thread(
            _tdlib_test_network,
            td_manager,
            telegram_id=telegram_id_num,
            root_path=root_path,
        )
    except Exception as exc:
        logger.warning(
            "Failed to run testNetwork for telegram=%s: %s",
            telegram_id_num,
            exc,
        )
        success = False

    return {"success": success}
