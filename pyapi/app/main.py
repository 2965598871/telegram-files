from __future__ import annotations

import asyncio
import logging
import secrets
import sqlite3
import os
import time
from copy import deepcopy
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

from .automation_workers import (
    WorkerDeps,
    background_workers_loop as _background_workers_loop,
    reset_worker_state as _reset_worker_state,
)
from .app_state import (
    AUTHENTICATION_METHODS,
    EVENT_TYPE_AUTHORIZATION,
    EVENT_TYPE_FILE_STATUS,
    EVENT_TYPE_METHOD_RESULT,
    PENDING_TELEGRAMS,
    SESSION_COOKIE_NAME,
    SESSION_TELEGRAM_SELECTION,
    STATE_LOCK,
    TELEGRAM_CONSTRUCTOR_STATE_READY,
    TELEGRAM_CONSTRUCTOR_WAIT_PHONE_NUMBER,
    PendingTelegramAccount,
    _auth_state,
    _build_ws_payload,
    _emit_ws_payload,
    _handle_tdlib_authorization_state,
    _is_pending_account,
    _pending_account_to_response,
    _recover_auth_selection,
    _remove_pending_account,
    _selected_telegram_id,
    _session_id_from_request,
    _tdlib_error_hint,
    _tdlib_manager_from_app,
    register_ws_connection,
    unregister_ws_connection,
    update_session_selection,
)
from .config import AppConfig
from .download_runtime import (
    _apply_chat_auto_settings,
    _avg_speed_interval,
    _db_update_tdlib_file_status,
    _emit_tdlib_download_aggregate,
    _ensure_tdlib_download_monitor,
    _live_speed_stats,
    _persist_speed_statistics,
    _stop_tdlib_download_monitor,
    _td_file_status_payload,
    _tdlib_account_root_path,
    reset_speed_state,
)
from .file_record_ops import (
    upsert_tdlib_file_record as _db_upsert_tdlib_file_record,
)
from .db import (
    count_files_by_type,
    cancel_file_download,
    create_connection,
    delete_telegram,
    get_file_preview_info,
    get_files_count,
    get_automation_map,
    get_settings_by_keys,
    get_telegram_account,
    get_telegram_download_statistics,
    get_telegram_download_statistics_by_phase,
    get_telegram_ping_seconds,
    init_schema,
    list_files,
    list_chats,
    list_telegrams,
    remove_file_download,
    start_file_download,
    toggle_pause_file_download,
    update_file_tags,
    update_files_tags,
    update_auto_settings,
    update_telegram_proxy,
    upsert_settings,
)
from .route_utils import (
    _bool_or_none,
    _file_status_from_file_record,
    _get_filters,
    _int_or_default,
    _method_error,
    _parse_batch_files,
    _to_telegram_id,
)
from .settings_keys import default_value_for
from .tdlib import (
    TdlibAuthManager,
    TdlibConfigurationError,
    TdlibRequestTimeout,
)
from .tdlib_payloads import (
    build_tdlib_generic_request as _build_tdlib_generic_request,
    build_tdlib_method_payload as _build_tdlib_method_payload,
)
from .tdlib_queries import (
    load_tdlib_chat_files as _load_tdlib_chat_files,
    load_tdlib_chat_files_count as _load_tdlib_chat_files_count,
    load_tdlib_chats as _load_tdlib_chats,
    load_tdlib_network_statistics as _load_tdlib_network_statistics,
    load_tdlib_ping_seconds as _load_tdlib_ping_seconds,
    load_tdlib_session_for_account as _load_tdlib_session_for_account,
    parse_link_files as _parse_link_files,
    tdlib_test_network as _tdlib_test_network,
)
from .tdlib_downloads import (
    cached_tdlib_file_preview as _cached_tdlib_file_preview,
    enrich_tdlib_thumbnails_for_files as _enrich_tdlib_thumbnails_for_files,
    media_type_for_path as _media_type_for_path,
    resolve_tdlib_preview_info as _resolve_tdlib_preview_info,
    start_tdlib_download_for_message as _start_tdlib_download_for_message,
    tdlib_cancel_download_fallback as _tdlib_cancel_download_fallback,
    tdlib_remove_file_fallback as _tdlib_remove_file_fallback,
    tdlib_toggle_pause_download_fallback as _tdlib_toggle_pause_download_fallback,
)
from .tdlib_monitor import reset_tdlib_monitor_state as _reset_tdlib_monitor_state


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

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = AppConfig.from_env()
    conn = create_connection(config)
    init_schema(conn)
    loop = asyncio.get_running_loop()

    tdlib_manager: TdlibAuthManager | None = None
    tdlib_error: str | None = None

    if config.telegram_api_id > 0 and config.telegram_api_hash:
        try:
            tdlib_manager = TdlibAuthManager(
                api_id=config.telegram_api_id,
                api_hash=config.telegram_api_hash,
                application_version=config.version,
                log_level=config.telegram_log_level,
                shared_lib_path=config.tdlib_shared_lib or None,
                on_authorization_state=lambda telegram_id,
                state: loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(
                        _handle_tdlib_authorization_state(app, telegram_id, state)
                    )
                ),
            )
        except Exception as exc:
            tdlib_error = str(exc)
            logger.warning("TDLib auth disabled: %s", tdlib_error)
    else:
        tdlib_error = (
            "Set TELEGRAM_API_ID and TELEGRAM_API_HASH to enable real TDLib login."
        )

    app.state.config = config
    app.state.db = conn
    app.state.tdlib_manager = tdlib_manager
    app.state.tdlib_error = tdlib_error
    _reset_worker_state()
    _reset_tdlib_monitor_state()
    reset_speed_state()

    async def _emit_worker_file_status(payload: dict[str, Any]) -> None:
        await _emit_ws_payload(
            _build_ws_payload(EVENT_TYPE_FILE_STATUS, payload),
        )

    worker_task = asyncio.create_task(
        _background_workers_loop(
            app,
            WorkerDeps(
                tdlib_account_root_path=_tdlib_account_root_path,
                emit_file_status=_emit_worker_file_status,
                td_file_status_payload=_td_file_status_payload,
                ensure_tdlib_download_monitor=lambda worker_app,
                session_id,
                telegram_id,
                file_id,
                unique_id: _ensure_tdlib_download_monitor(
                    worker_app,
                    session_id=session_id,
                    telegram_id=telegram_id,
                    file_id=file_id,
                    unique_id=unique_id,
                ),
                avg_speed_interval=_avg_speed_interval,
                persist_speed_statistics=_persist_speed_statistics,
            ),
        )
    )
    app.state.background_workers = worker_task
    try:
        yield
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        if tdlib_manager is not None:
            tdlib_manager.close()
        conn.close()


app = FastAPI(title="telegram-files python backend", lifespan=lifespan)

if os.getenv("APP_ENV", "prod") != "prod":
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["*"],
    )


@app.middleware("http")
async def ensure_session_cookie(request: Request, call_next):
    session_id = request.cookies.get(SESSION_COOKIE_NAME) or uuid4().hex
    request.state.session_id = session_id
    response = await call_next(request)
    if request.cookies.get(SESSION_COOKIE_NAME) != session_id:
        response.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=session_id,
            httponly=True,
            samesite="strict",
            secure=False,
        )
    return response


def get_db(request: Request) -> sqlite3.Connection:
    return request.app.state.db


def not_implemented() -> JSONResponse:
    return JSONResponse(
        status_code=501,
        content={
            "error": "This endpoint is not implemented in the Python backend yet."
        },
    )


@app.get("/")
def home() -> PlainTextResponse:
    return PlainTextResponse("Hello World!")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "UP"}


@app.get("/version")
def version(request: Request) -> dict[str, str]:
    config: AppConfig = request.app.state.config
    return {"version": config.version}


@app.websocket("/ws")
async def websocket_events(websocket: WebSocket) -> None:
    session_id = (
        websocket.cookies.get(SESSION_COOKIE_NAME)
        or websocket.query_params.get("sessionId")
        or uuid4().hex
    )
    telegram_id = websocket.query_params.get("telegramId")

    await websocket.accept()
    pending = register_ws_connection(session_id, websocket, telegram_id)

    if pending is not None:
        await _emit_ws_payload(
            _build_ws_payload(
                EVENT_TYPE_AUTHORIZATION,
                pending.last_authorization_state,
            ),
            session_id=session_id,
        )

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        unregister_ws_connection(session_id, websocket)


@app.get("/settings")
def settings(
    keys: str = Query(default=""), db: sqlite3.Connection = Depends(get_db)
) -> dict[str, Any]:
    key_list = [key.strip() for key in keys.split(",") if key.strip()]
    if not key_list:
        raise HTTPException(
            status_code=400, detail="Query parameter 'keys' is required."
        )

    data = get_settings_by_keys(db, key_list)
    for key in key_list:
        if key in data:
            continue
        try:
            data[key] = default_value_for(key)
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    return data


@app.post("/settings/create")
def settings_create(
    payload: dict[str, Any], db: sqlite3.Connection = Depends(get_db)
) -> Response:
    if not payload:
        raise HTTPException(
            status_code=400, detail="Request body must be a non-empty JSON object."
        )

    normalized = {
        str(key): "" if value is None else str(value) for key, value in payload.items()
    }
    upsert_settings(db, normalized)
    return Response(status_code=200)


@app.get("/telegram/api/methods")
def telegram_api_methods() -> dict[str, Any]:
    return {
        "methods": sorted(SUPPORTED_TELEGRAM_METHODS.keys()),
        "supportsGeneric": True,
    }


@app.get("/telegram/api/{method}/parameters")
def telegram_api_method_parameters(method: str) -> dict[str, Any]:
    return {
        "parameters": deepcopy(SUPPORTED_TELEGRAM_METHODS.get(method, {})),
    }


@app.post("/telegram/api/{method}")
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


@app.get("/telegrams")
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


@app.post("/telegrams/change")
def telegrams_change(request: Request) -> Response:
    session_id = _session_id_from_request(request)
    telegram_id = (request.query_params.get("telegramId") or "").strip()
    update_session_selection(session_id, telegram_id or None)
    return Response(status_code=200)


@app.post("/telegram/create")
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


@app.post("/telegram/{telegramId}/delete")
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


@app.get("/telegram/{telegramId}/chats")
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


@app.get("/telegram/{telegramId}/download-statistics")
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


@app.post("/telegram/{telegramId}/toggle-proxy")
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


@app.get("/telegram/{telegramId}/ping")
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


@app.get("/telegram/{telegramId}/test-network")
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


@app.get("/files/count")
def files_count(db: sqlite3.Connection = Depends(get_db)) -> dict[str, int]:
    return get_files_count(db)


@app.get("/files")
def files(request: Request, db: sqlite3.Connection = Depends(get_db)) -> dict[str, Any]:
    return list_files(db, telegram_id=None, chat_id=0, filters=_get_filters(request))


@app.get("/telegram/{telegramId}/chat/{chatId}/files")
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
        can_try_enrichment = _int_or_default(filters.get("fromMessageId"), 0) == 0
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


@app.get("/telegram/{telegramId}/chat/{chatId}/files/count")
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


@app.post("/files/update-tags")
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


@app.post("/file/{uniqueId}/update-tags")
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


@app.get("/{telegramId}/file/{uniqueId}")
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


@app.post("/{telegramId}/file/start-download")
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

    if started_via_tdlib:
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


@app.post("/{telegramId}/file/cancel-download")
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


@app.post("/{telegramId}/file/toggle-pause-download")
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


@app.post("/{telegramId}/file/remove")
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


@app.post("/{telegramId}/file/update-auto-settings")
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


@app.post("/files/start-download-multiple")
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

        if started_via_tdlib:
            _ensure_tdlib_download_monitor(
                request.app,
                session_id=session_id,
                telegram_id=item["telegramId"],
                file_id=_int_or_default(file_record.get("id"), item["fileId"]),
                unique_id=str(file_record.get("uniqueId") or ""),
            )

    return {
        "processed": processed,
        "failed": failed,
    }


@app.post("/files/cancel-download-multiple")
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


@app.post("/files/toggle-pause-download-multiple")
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


@app.post("/files/remove-multiple")
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


UNPORTED_ROUTES: list[tuple[str, str]] = []


for idx, (method, route) in enumerate(UNPORTED_ROUTES):
    app.add_api_route(
        route,
        not_implemented,
        methods=[method],
        name=f"not_implemented_{idx}",
    )
