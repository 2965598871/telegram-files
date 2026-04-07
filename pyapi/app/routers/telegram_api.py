from __future__ import annotations

import asyncio
import secrets
import sqlite3
import time
from copy import deepcopy
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from ..app_state import (
    AUTHENTICATION_METHODS,
    EVENT_TYPE_AUTHORIZATION,
    EVENT_TYPE_METHOD_RESULT,
    PENDING_TELEGRAMS,
    STATE_LOCK,
    TELEGRAM_CONSTRUCTOR_STATE_READY,
    _auth_state,
    _build_ws_payload,
    _emit_ws_payload,
    _recover_auth_selection,
    _selected_telegram_id,
    _session_id_from_request,
    _tdlib_error_hint,
    _tdlib_manager_from_app,
)
from ..db import get_telegram_ping_seconds
from ..deps import get_db
from ..download_runtime import _tdlib_account_root_path
from ..route_utils import _int_or_default, _method_error
from ..tdlib import TdlibRequestTimeout
from ..tdlib_payloads import (
    build_tdlib_generic_request as _build_tdlib_generic_request,
    build_tdlib_method_payload as _build_tdlib_method_payload,
)
from ..tdlib_queries import (
    load_tdlib_session_for_account as _load_tdlib_session_for_account,
)

router = APIRouter()


SUPPORTED_TELEGRAM_METHODS: dict[str, dict[str, Any]] = {
    "SetAuthenticationPhoneNumber": {
        "phoneNumber": "",
        "settings": None,
    },
    "CheckAuthenticationCode": {
        "code": "",
    },
    "CheckAuthenticationPassword": {
        "password": "",
    },
    "RequestQrCodeAuthentication": {
        "otherUserIds": None,
    },
    "GetMessageThread": {
        "chatId": 0,
        "messageId": 0,
    },
    "GetNetworkStatistics": {},
    "PingProxy": {
        "proxyId": 0,
    },
}


@router.get("/telegram/api/methods")
def telegram_api_methods() -> dict[str, Any]:
    return {
        "methods": sorted(SUPPORTED_TELEGRAM_METHODS.keys()),
        "supportsGeneric": True,
    }


@router.get("/telegram/api/{method}/parameters")
def telegram_api_method_parameters(method: str) -> dict[str, Any]:
    return {
        "parameters": deepcopy(SUPPORTED_TELEGRAM_METHODS.get(method, {})),
    }


@router.post("/telegram/api/{method}")
async def telegram_api_method(
    method: str,
    payload: dict[str, Any] | None,
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> JSONResponse:
    session_id = _session_id_from_request(request)
    selected_telegram = _selected_telegram_id(session_id)
    if not selected_telegram:
        selected_telegram = _recover_auth_selection(session_id, method)
    if not selected_telegram:
        return _method_error("Your session not link any telegram!")

    params = payload if isinstance(payload, dict) else {}
    code = secrets.token_hex(5)

    method_result: Any
    authorization_state: dict[str, Any] | None = None

    with STATE_LOCK:
        pending = PENDING_TELEGRAMS.get(selected_telegram)

    if pending is not None:
        if method in AUTHENTICATION_METHODS:
            td_manager = _tdlib_manager_from_app(request.app)
            if td_manager is None:
                return _method_error(_tdlib_error_hint(request.app))

            try:
                td_payload, side_effects = _build_tdlib_method_payload(method, params)
            except ValueError as exc:
                return _method_error(str(exc))

            if method == "SetAuthenticationPhoneNumber":
                normalized_phone = str(side_effects.get("phoneNumber") or "")
                with STATE_LOCK:
                    still_pending = PENDING_TELEGRAMS.get(selected_telegram)
                    if still_pending is not None:
                        still_pending.phone_number = normalized_phone

            try:
                is_ready = await asyncio.to_thread(
                    td_manager.prepare_authorization,
                    selected_telegram,
                    12.0,
                )
            except TdlibRequestTimeout as exc:
                return _method_error(str(exc))
            except Exception as exc:
                return _method_error(f"TDLib init failed: {exc}")

            if not is_ready:
                return _method_error(
                    "TDLib is still initializing. Please retry in a moment."
                )

            try:
                td_result = await asyncio.to_thread(
                    td_manager.request,
                    selected_telegram,
                    td_payload,
                    30.0,
                )
            except TdlibRequestTimeout as exc:
                return _method_error(str(exc))
            except Exception as exc:
                return _method_error(f"TDLib request failed: {exc}")

            if str(td_result.get("@type") or "") == "error":
                error_message = str(td_result.get("message") or "TDLib error")
                if "setTdlibParameters" in error_message:
                    try:
                        retry_ready = await asyncio.to_thread(
                            td_manager.prepare_authorization,
                            selected_telegram,
                            12.0,
                        )
                    except TdlibRequestTimeout as exc:
                        return _method_error(str(exc))
                    except Exception as exc:
                        return _method_error(f"TDLib init failed: {exc}")

                    if not retry_ready:
                        return _method_error(
                            "TDLib is still initializing. Please retry in a moment."
                        )

                    try:
                        td_result = await asyncio.to_thread(
                            td_manager.request,
                            selected_telegram,
                            td_payload,
                            30.0,
                        )
                    except TdlibRequestTimeout as exc:
                        return _method_error(str(exc))
                    except Exception as exc:
                        return _method_error(f"TDLib request failed: {exc}")
                    if str(td_result.get("@type") or "") == "error":
                        error_message = str(td_result.get("message") or "TDLib error")

                if str(td_result.get("@type") or "") == "error":
                    return _method_error(error_message)

            method_result = {"ok": True}
        elif method == "GetMessageThread":
            method_result = {
                "chatId": _int_or_default(params.get("chatId"), 0),
                "messageThreadId": _int_or_default(params.get("messageId"), 0),
            }
        elif method == "GetNetworkStatistics":
            method_result = {
                "sinceDate": int(time.time()),
                "entries": [],
            }
        elif method == "PingProxy":
            method_result = {
                "seconds": 0.08 if pending.proxy else 0.0,
            }
        else:
            return _method_error(f"Unsupported method in pending account: {method}")
    else:
        if method == "GetMessageThread":
            method_result = {
                "chatId": _int_or_default(params.get("chatId"), 0),
                "messageThreadId": _int_or_default(params.get("messageId"), 0),
            }
        elif method == "GetNetworkStatistics":
            method_result = {
                "sinceDate": int(time.time()),
                "entries": [],
            }
        elif method == "PingProxy":
            telegram_id_num = _int_or_default(selected_telegram, 0)
            seconds = (
                get_telegram_ping_seconds(db, telegram_id_num)
                if telegram_id_num > 0
                else 0.0
            )
            method_result = {"seconds": seconds}
        elif method in {
            "SetAuthenticationPhoneNumber",
            "CheckAuthenticationCode",
            "CheckAuthenticationPassword",
            "RequestQrCodeAuthentication",
        }:
            method_result = {"ok": True}
            authorization_state = _auth_state(TELEGRAM_CONSTRUCTOR_STATE_READY)
        else:
            td_manager = _tdlib_manager_from_app(request.app)
            if td_manager is None:
                return _method_error(_tdlib_error_hint(request.app))

            telegram_id_num = _int_or_default(selected_telegram, 0)
            if telegram_id_num <= 0:
                return _method_error("Telegram account not found")

            root_path = _tdlib_account_root_path(
                request.app,
                db,
                telegram_id_num,
            )
            if root_path is None:
                return _method_error("Telegram account not found")

            td_method_payload = _build_tdlib_generic_request(method, params)
            try:
                is_ready = await asyncio.to_thread(
                    _load_tdlib_session_for_account,
                    td_manager,
                    telegram_id_num,
                    root_path,
                )
            except Exception as exc:
                return _method_error(f"TDLib init failed: {exc}")

            if not is_ready:
                return _method_error(
                    "TDLib is still initializing. Please retry in a moment."
                )

            try:
                td_result = await asyncio.to_thread(
                    td_manager.request,
                    str(telegram_id_num),
                    td_method_payload,
                    30.0,
                )
            except TdlibRequestTimeout as exc:
                return _method_error(str(exc))
            except Exception as exc:
                return _method_error(f"TDLib request failed: {exc}")

            if str(td_result.get("@type") or "") == "error":
                return _method_error(str(td_result.get("message") or "TDLib error"))

            method_result = td_result

    await _emit_ws_payload(
        _build_ws_payload(EVENT_TYPE_METHOD_RESULT, method_result, code=code),
        session_id=session_id,
    )
    if authorization_state is not None:
        await _emit_ws_payload(
            _build_ws_payload(EVENT_TYPE_AUTHORIZATION, authorization_state),
            session_id=session_id,
        )

    return JSONResponse(content={"code": code})
