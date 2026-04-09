from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

from fastapi import (
    FastAPI,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse

from .app_state import (
    EVENT_TYPE_AUTHORIZATION,
    EVENT_TYPE_FILE_STATUS,
    SESSION_COOKIE_NAME,
    _build_ws_payload,
    _emit_ws_payload,
    _handle_tdlib_authorization_state,
    register_ws_connection,
    unregister_ws_connection,
)
from .automation_workers import (
    WorkerDeps,
    background_workers_loop as _background_workers_loop,
    reset_worker_state as _reset_worker_state,
)
from .config import AppConfig, _load_dotenv_if_present
from .db import create_connection, init_schema
from .download_runtime import (
    _avg_speed_interval,
    _ensure_tdlib_download_monitor,
    _persist_speed_statistics,
    _td_file_status_payload,
    _tdlib_account_root_path,
    reset_speed_state,
)
from .routers import register_routers
from .tdlib import TdlibAuthManager
from .tdlib_monitor import reset_tdlib_monitor_state as _reset_tdlib_monitor_state

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
                on_authorization_state=lambda telegram_id, state: (
                    loop.call_soon_threadsafe(
                        lambda: asyncio.create_task(
                            _handle_tdlib_authorization_state(app, telegram_id, state)
                        )
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
                ensure_tdlib_download_monitor=lambda worker_app, session_id, telegram_id, file_id, unique_id: (
                    _ensure_tdlib_download_monitor(
                        worker_app,
                        session_id=session_id,
                        telegram_id=telegram_id,
                        file_id=file_id,
                        unique_id=unique_id,
                    )
                ),
                avg_speed_interval=_avg_speed_interval,
                persist_speed_statistics=_persist_speed_statistics,
            ),
        )
    )
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


_load_dotenv_if_present()

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


register_routers(app)
