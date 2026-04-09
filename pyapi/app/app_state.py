from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, Request, WebSocket

from .db import create_telegram_account
from .tdlib import TdlibAuthManager


SESSION_COOKIE_NAME = "tf"

EVENT_TYPE_AUTHORIZATION = 1
EVENT_TYPE_METHOD_RESULT = 2
EVENT_TYPE_FILE_UPDATE = 3
EVENT_TYPE_FILE_DOWNLOAD = 4
EVENT_TYPE_FILE_STATUS = 5

TELEGRAM_CONSTRUCTOR_STATE_READY = -1834871737
TELEGRAM_CONSTRUCTOR_WAIT_PHONE_NUMBER = 306402531
TELEGRAM_CONSTRUCTOR_WAIT_CODE = 52643073
TELEGRAM_CONSTRUCTOR_WAIT_PASSWORD = 112238030
TELEGRAM_CONSTRUCTOR_WAIT_OTHER_DEVICE_CONFIRMATION = 860166378

AUTHENTICATION_METHODS = {
    "SetAuthenticationPhoneNumber",
    "CheckAuthenticationCode",
    "CheckAuthenticationPassword",
    "RequestQrCodeAuthentication",
}

TDLIB_AUTH_STATE_TO_CONSTRUCTOR = {
    "authorizationStateWaitPhoneNumber": TELEGRAM_CONSTRUCTOR_WAIT_PHONE_NUMBER,
    "authorizationStateWaitCode": TELEGRAM_CONSTRUCTOR_WAIT_CODE,
    "authorizationStateWaitPassword": TELEGRAM_CONSTRUCTOR_WAIT_PASSWORD,
    "authorizationStateWaitOtherDeviceConfirmation": TELEGRAM_CONSTRUCTOR_WAIT_OTHER_DEVICE_CONFIRMATION,
    "authorizationStateReady": TELEGRAM_CONSTRUCTOR_STATE_READY,
}

logger = logging.getLogger(__name__)


@dataclass
class PendingTelegramAccount:
    id: str
    name: str
    root_path: str
    proxy: str | None
    phone_number: str
    last_authorization_state: dict[str, Any]


STATE_LOCK = Lock()
PENDING_TELEGRAMS: dict[str, PendingTelegramAccount] = {}
SESSION_TELEGRAM_SELECTION: dict[str, str] = {}
WS_CONNECTIONS: dict[str, set[WebSocket]] = {}


def _auth_state(constructor: int, **extra: Any) -> dict[str, Any]:
    payload = {"constructor": constructor}
    payload.update(extra)
    return payload


def _build_ws_payload(
    event_type: int,
    data: Any,
    code: str | None = None,
) -> dict[str, Any]:
    return {
        "type": event_type,
        "code": code,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }


def _session_id_from_request(request: Request) -> str:
    state_session = getattr(request.state, "session_id", None)
    if state_session:
        return str(state_session)
    cookie_session = request.cookies.get(SESSION_COOKIE_NAME)
    if cookie_session:
        return str(cookie_session)
    return uuid4().hex


def _selected_telegram_id(session_id: str) -> str | None:
    with STATE_LOCK:
        return SESSION_TELEGRAM_SELECTION.get(session_id)


def _recover_auth_selection(session_id: str, method: str) -> str | None:
    if method not in AUTHENTICATION_METHODS:
        return None

    with STATE_LOCK:
        if session_id in SESSION_TELEGRAM_SELECTION:
            return SESSION_TELEGRAM_SELECTION[session_id]

        pending_ids = list(PENDING_TELEGRAMS.keys())
        if len(pending_ids) != 1:
            return None

        recovered_id = pending_ids[0]
        SESSION_TELEGRAM_SELECTION[session_id] = recovered_id
        return recovered_id


def _tdlib_manager_from_app(app: FastAPI) -> TdlibAuthManager | None:
    manager = getattr(app.state, "tdlib_manager", None)
    if isinstance(manager, TdlibAuthManager):
        return manager
    return None


def _tdlib_error_hint(app: FastAPI) -> str:
    reason = str(getattr(app.state, "tdlib_error", "") or "").strip()
    if reason:
        return reason
    return "TDLib auth is not configured."


def _normalize_tdlib_authorization_state(
    td_state: dict[str, Any],
) -> dict[str, Any] | None:
    state_type = str(td_state.get("@type") or "")
    constructor = TDLIB_AUTH_STATE_TO_CONSTRUCTOR.get(state_type)
    if constructor is None:
        return None

    normalized = _auth_state(constructor)
    if state_type == "authorizationStateWaitOtherDeviceConfirmation":
        normalized["link"] = str(td_state.get("link") or "")
    if state_type == "authorizationStateWaitCode":
        code_info = td_state.get("code_info")
        if isinstance(code_info, dict):
            normalized["phoneNumber"] = str(code_info.get("phone_number") or "")
    return normalized


def _display_name_from_td_me(payload: dict[str, Any]) -> str | None:
    first_name = str(payload.get("first_name") or "").strip()
    last_name = str(payload.get("last_name") or "").strip()
    if first_name and last_name:
        return f"{first_name} {last_name}".strip()
    if first_name:
        return first_name
    if last_name:
        return last_name
    return None


def update_session_selection(session_id: str, telegram_id: str | None) -> None:
    with STATE_LOCK:
        if not telegram_id:
            SESSION_TELEGRAM_SELECTION.pop(session_id, None)
            return
        SESSION_TELEGRAM_SELECTION[session_id] = telegram_id


def register_ws_connection(
    session_id: str,
    websocket: WebSocket,
    telegram_id: str | None = None,
) -> PendingTelegramAccount | None:
    with STATE_LOCK:
        WS_CONNECTIONS.setdefault(session_id, set()).add(websocket)
        if telegram_id:
            SESSION_TELEGRAM_SELECTION[session_id] = telegram_id
        selected_id = SESSION_TELEGRAM_SELECTION.get(session_id)
        return PENDING_TELEGRAMS.get(selected_id) if selected_id else None


def unregister_ws_connection(session_id: str, websocket: WebSocket) -> None:
    with STATE_LOCK:
        session_connections = WS_CONNECTIONS.get(session_id)
        if session_connections is None:
            return
        session_connections.discard(websocket)
        if not session_connections:
            del WS_CONNECTIONS[session_id]


async def _emit_ws_payload(
    payload: dict[str, Any], session_id: str | None = None
) -> None:
    with STATE_LOCK:
        if session_id is not None:
            targets = list(WS_CONNECTIONS.get(session_id, set()))
            if not targets:
                targets = [
                    ws
                    for session_connections in WS_CONNECTIONS.values()
                    for ws in session_connections
                ]
        else:
            targets = [
                ws
                for session_connections in WS_CONNECTIONS.values()
                for ws in session_connections
            ]

    dead_connections: list[WebSocket] = []
    for ws in targets:
        try:
            await ws.send_json(payload)
        except Exception:
            dead_connections.append(ws)

    if not dead_connections:
        return

    with STATE_LOCK:
        for dead in dead_connections:
            for session_connections in WS_CONNECTIONS.values():
                if dead in session_connections:
                    session_connections.discard(dead)


def _pending_account_to_response(
    pending: PendingTelegramAccount,
) -> dict[str, Any]:
    return {
        "id": pending.id,
        "name": pending.name,
        "phoneNumber": pending.phone_number,
        "avatar": "",
        "status": "inactive",
        "rootPath": pending.root_path,
        "isPremium": False,
        "lastAuthorizationState": pending.last_authorization_state,
        "proxy": pending.proxy,
    }


def _is_pending_account(telegram_id: str) -> bool:
    with STATE_LOCK:
        return telegram_id in PENDING_TELEGRAMS


def _remove_pending_account(
    telegram_id: str,
    tdlib_manager: TdlibAuthManager | None = None,
) -> None:
    if tdlib_manager is not None:
        try:
            tdlib_manager.remove_session(telegram_id)
        except Exception:
            pass

    with STATE_LOCK:
        if telegram_id in PENDING_TELEGRAMS:
            del PENDING_TELEGRAMS[telegram_id]
        sessions_to_clear = [
            sid
            for sid, selected in SESSION_TELEGRAM_SELECTION.items()
            if selected == telegram_id
        ]
        for sid in sessions_to_clear:
            del SESSION_TELEGRAM_SELECTION[sid]


async def _finalize_pending_login(
    app: FastAPI,
    *,
    pending_id: str,
    display_name: str | None = None,
    phone_number: str | None = None,
) -> str | None:
    with STATE_LOCK:
        pending = PENDING_TELEGRAMS.get(pending_id)
        if pending is None:
            return None
        pending_name = display_name or pending.name
        pending_proxy = pending.proxy
        pending_phone = phone_number or pending.phone_number
        pending_root_path = pending.root_path

    db = app.state.db
    config = app.state.config
    active_account = create_telegram_account(
        db,
        app_root=str(config.app_root),
        first_name=pending_name,
        proxy_name=pending_proxy,
        phone_number=pending_phone,
        root_path=pending_root_path,
    )

    with STATE_LOCK:
        PENDING_TELEGRAMS.pop(pending_id, None)
        sessions_to_update = [
            sid
            for sid, selected in SESSION_TELEGRAM_SELECTION.items()
            if selected == pending_id
        ]
        for sid in sessions_to_update:
            SESSION_TELEGRAM_SELECTION[sid] = active_account["id"]

    ready_payload = _build_ws_payload(
        EVENT_TYPE_AUTHORIZATION,
        _auth_state(TELEGRAM_CONSTRUCTOR_STATE_READY),
    )
    for sid in sessions_to_update:
        await _emit_ws_payload(ready_payload, session_id=sid)

    return str(active_account["id"])


async def _handle_tdlib_authorization_state(
    app: FastAPI,
    telegram_id: str,
    td_state: dict[str, Any],
) -> None:
    normalized_state = _normalize_tdlib_authorization_state(td_state)
    if normalized_state is None:
        return

    with STATE_LOCK:
        pending = PENDING_TELEGRAMS.get(telegram_id)
        if pending is None:
            return
        pending.last_authorization_state = normalized_state
        phone_number = str(normalized_state.get("phoneNumber") or "").strip()
        if phone_number:
            pending.phone_number = phone_number
        session_ids = [
            sid
            for sid, selected in SESSION_TELEGRAM_SELECTION.items()
            if selected == telegram_id
        ]

    ws_payload = _build_ws_payload(EVENT_TYPE_AUTHORIZATION, normalized_state)
    for session_id in session_ids:
        await _emit_ws_payload(ws_payload, session_id=session_id)

    if normalized_state.get("constructor") != TELEGRAM_CONSTRUCTOR_STATE_READY:
        return

    td_manager = _tdlib_manager_from_app(app)
    td_me_payload: dict[str, Any] = {}
    if td_manager is not None:
        try:
            td_me_payload = await asyncio.to_thread(td_manager.get_me, telegram_id)
        except Exception as exc:
            logger.warning("Failed to call getMe for %s: %s", telegram_id, exc)

    resolved_name = _display_name_from_td_me(td_me_payload)
    resolved_phone = str(td_me_payload.get("phone_number") or "").strip() or None
    await _finalize_pending_login(
        app,
        pending_id=telegram_id,
        display_name=resolved_name,
        phone_number=resolved_phone,
    )

    if td_manager is not None:
        await asyncio.to_thread(td_manager.remove_session, telegram_id)
