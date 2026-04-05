from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import secrets
import sqlite3
import os
import time
from copy import deepcopy
from collections import deque
from datetime import datetime
from pathlib import Path
from contextlib import asynccontextmanager
from dataclasses import dataclass
from threading import Lock
from typing import Any
from urllib.parse import unquote
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

from .config import AppConfig
from .db import (
    count_files_by_type,
    cancel_file_download,
    create_telegram_account,
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
    update_auto_settings,
    update_file_tags,
    update_files_tags,
    update_telegram_proxy,
    upsert_settings,
)
from .filter_expr import evaluate_filter_expr as _evaluate_filter_expr
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
    default_chat_auto as _default_chat_auto,
    load_tdlib_chat_files as _load_tdlib_chat_files,
    load_tdlib_chat_files_count as _load_tdlib_chat_files_count,
    load_tdlib_chats as _load_tdlib_chats,
    load_tdlib_network_statistics as _load_tdlib_network_statistics,
    load_tdlib_ping_seconds as _load_tdlib_ping_seconds,
    load_tdlib_session_for_account as _load_tdlib_session_for_account,
    parse_link_files as _parse_link_files,
    tdlib_test_network as _tdlib_test_network,
)
from .tdlib_file_mapper import (
    extract_td_message_file as _extract_td_message_file,
    td_message_to_file as _td_message_to_file,
)
from .transfer_ops import execute_transfer as _execute_transfer


SESSION_COOKIE_NAME = "tf"

EVENT_TYPE_ERROR = -1
EVENT_TYPE_AUTHORIZATION = 1
EVENT_TYPE_METHOD_RESULT = 2
EVENT_TYPE_FILE_UPDATE = 3
EVENT_TYPE_FILE_DOWNLOAD = 4
EVENT_TYPE_FILE_STATUS = 5

AUTO_DOWNLOAD_DEFAULT_LIMIT = 5
HISTORY_PRELOAD_STATE = 1
HISTORY_DOWNLOAD_STATE = 2
HISTORY_DOWNLOAD_SCAN_STATE = 3
HISTORY_TRANSFER_STATE = 4

PRELOAD_SCAN_INTERVAL_SECONDS = 30
AUTO_DOWNLOAD_SCAN_INTERVAL_SECONDS = 120
AUTO_DOWNLOAD_TICK_INTERVAL_SECONDS = 10
TRANSFER_SCAN_INTERVAL_SECONDS = 120
TRANSFER_TICK_INTERVAL_SECONDS = 3
AUTO_DOWNLOAD_MAX_WAITING_LENGTH = 30
SPEED_INTERVAL_CACHE_TTL_SECONDS = 5.0

TELEGRAM_CONSTRUCTOR_STATE_READY = -1834871737
TELEGRAM_CONSTRUCTOR_WAIT_PHONE_NUMBER = 306402531
TELEGRAM_CONSTRUCTOR_WAIT_CODE = 52643073
TELEGRAM_CONSTRUCTOR_WAIT_PASSWORD = 112238030
TELEGRAM_CONSTRUCTOR_WAIT_OTHER_DEVICE_CONFIRMATION = 860166378

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
TDLIB_DOWNLOAD_TASKS: dict[tuple[str, int, int], asyncio.Task[Any]] = {}
TDLIB_DOWNLOAD_PROGRESS: dict[tuple[str, int, int], dict[str, Any]] = {}
TDLIB_FILE_PREVIEW_CACHE: dict[tuple[int, str], dict[str, Any]] = {}
AUTO_DOWNLOAD_WAITING: dict[int, deque[dict[str, int]]] = {}
AUTO_DOWNLOAD_WAITING_KEYS: set[tuple[int, int, int]] = set()
AUTO_DOWNLOAD_COMMENT_THREADS: dict[tuple[int, int, int], dict[str, Any]] = {}
TRANSFER_WAITING: deque[dict[str, Any]] = deque()
TRANSFER_WAITING_KEYS: set[tuple[int, str]] = set()
SPEED_TRACKERS: dict[int, "AvgSpeedTracker"] = {}
SPEED_TOTAL_DOWNLOADED: dict[int, int] = {}
SPEED_LAST_FILE_DOWNLOADED: dict[tuple[int, int], int] = {}
SPEED_INTERVAL_CACHE_VALUE = 5 * 60
SPEED_INTERVAL_CACHE_AT = 0.0


class AvgSpeedTracker:
    def __init__(self, interval_seconds: int, smoothing_window_size: int = 6) -> None:
        self.interval_seconds = max(1, int(interval_seconds))
        self.smoothing_window_size = max(2, int(smoothing_window_size))
        self._speed_points: deque[tuple[int, int, int]] = deque()

    def set_interval(self, interval_seconds: int) -> None:
        self.interval_seconds = max(1, int(interval_seconds))

    def update(self, downloaded_size: int, timestamp_ms: int) -> None:
        if downloaded_size <= 0:
            self._remove_old_points(timestamp_ms)
            return

        speed = self._calculate_instant_speed(downloaded_size, timestamp_ms)
        if len(self._speed_points) >= self.smoothing_window_size:
            speed = self._smooth_speed(speed)

        self._speed_points.append((downloaded_size, speed, timestamp_ms))
        self._remove_old_points(timestamp_ms)

    def speed_stats(self) -> dict[str, int]:
        return {
            "interval": self.interval_seconds,
            "avgSpeed": self._avg_speed(),
            "medianSpeed": self._median_speed(),
            "maxSpeed": self._max_speed(),
            "minSpeed": self._min_speed(),
        }

    def _remove_old_points(self, timestamp_ms: int) -> None:
        cutoff = timestamp_ms - (self.interval_seconds * 1000)
        while self._speed_points and self._speed_points[0][2] < cutoff:
            self._speed_points.popleft()

    def _recent_points(self, size: int) -> list[tuple[int, int, int]]:
        if size <= 0:
            return []
        return list(self._speed_points)[-size:]

    def _calculate_instant_speed(self, current_size: int, current_time_ms: int) -> int:
        if not self._speed_points:
            return 0

        points_to_consider = min(self.smoothing_window_size, len(self._speed_points))
        recent_points = self._recent_points(points_to_consider)
        if not recent_points:
            return 0

        earliest = recent_points[0]
        time_diff = current_time_ms - earliest[2]
        if time_diff <= 0:
            return 0

        bytes_diff = current_size - earliest[0]
        if bytes_diff < 0:
            bytes_diff = current_size

        return int((bytes_diff * 1000) / time_diff)

    def _smooth_speed(self, current_speed: int) -> int:
        if not self._speed_points:
            return current_speed

        recent = self._recent_points(self.smoothing_window_size)
        recent_speeds = [point[1] for point in recent]
        recent_speeds.append(current_speed)
        if len(recent_speeds) < 2:
            return current_speed

        mean = sum(recent_speeds) / float(len(recent_speeds))
        variance = sum((speed - mean) ** 2 for speed in recent_speeds) / float(
            len(recent_speeds)
        )
        standard_deviation = variance**0.5

        lower = mean - (3 * standard_deviation)
        upper = mean + (3 * standard_deviation)
        filtered = [speed for speed in recent_speeds if lower <= speed <= upper]
        if not filtered:
            return current_speed

        weighted_sum = 0.0
        total_weight = 0.0
        size = len(filtered)
        for idx, speed in enumerate(filtered):
            weight = (idx + 1.0) / size
            weighted_sum += speed * weight
            total_weight += weight

        if total_weight == 0:
            return current_speed
        return int(weighted_sum / total_weight)

    def _avg_speed(self) -> int:
        if len(self._speed_points) < 2:
            return 0

        first = self._speed_points[0]
        last = self._speed_points[-1]
        time_diff = last[2] - first[2]
        if time_diff <= 0:
            return 0

        bytes_downloaded = last[0] - first[0]
        if bytes_downloaded < 0:
            bytes_downloaded = last[0]

        return int((bytes_downloaded * 1000) / time_diff)

    def _median_speed(self) -> int:
        if len(self._speed_points) < 2:
            return 0

        speeds = sorted(point[1] for point in self._speed_points if point[1] > 0)
        if not speeds:
            return 0
        return int(speeds[len(speeds) // 2])

    def _max_speed(self) -> int:
        if not self._speed_points:
            return 0
        return int(max(point[1] for point in self._speed_points))

    def _min_speed(self) -> int:
        positive = [point[1] for point in self._speed_points if point[1] > 0]
        if not positive:
            return 0
        return int(min(positive))


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
    AUTO_DOWNLOAD_WAITING.clear()
    AUTO_DOWNLOAD_WAITING_KEYS.clear()
    AUTO_DOWNLOAD_COMMENT_THREADS.clear()
    TRANSFER_WAITING.clear()
    TRANSFER_WAITING_KEYS.clear()
    SPEED_TRACKERS.clear()
    SPEED_TOTAL_DOWNLOADED.clear()
    SPEED_LAST_FILE_DOWNLOADED.clear()
    global SPEED_INTERVAL_CACHE_VALUE, SPEED_INTERVAL_CACHE_AT
    SPEED_INTERVAL_CACHE_VALUE = 5 * 60
    SPEED_INTERVAL_CACHE_AT = 0.0
    worker_task = asyncio.create_task(_background_workers_loop(app))
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


def _decode_link_value(value: str) -> str:
    current = value.strip()
    if not current:
        return ""

    for _ in range(3):
        decoded = unquote(current)
        if decoded == current:
            break
        current = decoded
    return current


def _state_is_enabled(state: int, bit: int) -> bool:
    return (state & (1 << bit)) != 0


def _state_enable(state: int, bit: int) -> int:
    return state | (1 << bit)


def _auto_download_limit(db: sqlite3.Connection) -> int:
    raw = get_settings_by_keys(db, ["autoDownloadLimit"]).get("autoDownloadLimit")
    parsed = _int_or_default(raw, AUTO_DOWNLOAD_DEFAULT_LIMIT)
    if parsed <= 0:
        return AUTO_DOWNLOAD_DEFAULT_LIMIT
    return parsed


def _avg_speed_interval(db: sqlite3.Connection) -> int:
    global SPEED_INTERVAL_CACHE_VALUE, SPEED_INTERVAL_CACHE_AT

    now = time.monotonic()
    with STATE_LOCK:
        cached_value = SPEED_INTERVAL_CACHE_VALUE
        cached_at = SPEED_INTERVAL_CACHE_AT

    if now - cached_at <= SPEED_INTERVAL_CACHE_TTL_SECONDS:
        return cached_value

    raw = get_settings_by_keys(db, ["avgSpeedInterval"]).get("avgSpeedInterval")
    parsed = _int_or_default(raw, 5 * 60)
    if parsed <= 0:
        parsed = 5 * 60

    with STATE_LOCK:
        SPEED_INTERVAL_CACHE_VALUE = parsed
        SPEED_INTERVAL_CACHE_AT = now

    return parsed


def _live_speed_stats(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
) -> dict[str, int]:
    interval = _avg_speed_interval(db)
    now_ms = int(time.time() * 1000)

    with STATE_LOCK:
        tracker = SPEED_TRACKERS.get(telegram_id)
        if tracker is None:
            return {
                "interval": interval,
                "avgSpeed": 0,
                "medianSpeed": 0,
                "maxSpeed": 0,
                "minSpeed": 0,
            }
        tracker.set_interval(interval)
        tracker.update(0, now_ms)
        return tracker.speed_stats()


def _update_speed_tracker(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int,
    downloaded_size: int,
    timestamp_ms: int,
) -> None:
    if telegram_id <= 0 or file_id <= 0:
        return

    interval = _avg_speed_interval(db)
    key = (telegram_id, file_id)
    normalized_downloaded = max(0, downloaded_size)

    with STATE_LOCK:
        tracker = SPEED_TRACKERS.get(telegram_id)
        if tracker is None:
            tracker = AvgSpeedTracker(interval)
            SPEED_TRACKERS[telegram_id] = tracker
        else:
            tracker.set_interval(interval)

        previous = SPEED_LAST_FILE_DOWNLOADED.get(key)
        if previous is None:
            SPEED_LAST_FILE_DOWNLOADED[key] = normalized_downloaded
            SPEED_TOTAL_DOWNLOADED[telegram_id] = (
                SPEED_TOTAL_DOWNLOADED.get(telegram_id, 0) + normalized_downloaded
            )
        else:
            delta = normalized_downloaded - previous
            if delta < 0:
                delta = normalized_downloaded
            if delta > 0:
                SPEED_TOTAL_DOWNLOADED[telegram_id] = (
                    SPEED_TOTAL_DOWNLOADED.get(telegram_id, 0) + delta
                )
            SPEED_LAST_FILE_DOWNLOADED[key] = normalized_downloaded

        tracker.update(SPEED_TOTAL_DOWNLOADED.get(telegram_id, 0), timestamp_ms)


def _clear_speed_tracker_file(
    *,
    telegram_id: int,
    file_id: int,
) -> None:
    if telegram_id <= 0 or file_id <= 0:
        return
    with STATE_LOCK:
        SPEED_LAST_FILE_DOWNLOADED.pop((telegram_id, file_id), None)


def _persist_speed_statistics(db: sqlite3.Connection) -> None:
    interval = _avg_speed_interval(db)
    now_ms = int(time.time() * 1000)

    with STATE_LOCK:
        items = list(SPEED_TRACKERS.items())

    rows_to_insert: list[tuple[str, str, int, str]] = []
    for telegram_id, tracker in items:
        with STATE_LOCK:
            tracker.set_interval(interval)
            tracker.update(0, now_ms)
            stats = tracker.speed_stats()

        if (
            stats["avgSpeed"] == 0
            and stats["medianSpeed"] == 0
            and stats["maxSpeed"] == 0
            and stats["minSpeed"] == 0
        ):
            continue

        payload = json.dumps(
            {
                "avgSpeed": stats["avgSpeed"],
                "medianSpeed": stats["medianSpeed"],
                "maxSpeed": stats["maxSpeed"],
                "minSpeed": stats["minSpeed"],
            },
            separators=(",", ":"),
            ensure_ascii=False,
        )
        rows_to_insert.append((str(telegram_id), "speed", now_ms, payload))

    if not rows_to_insert:
        return

    db.executemany(
        """
        INSERT INTO statistic_record(related_id, type, timestamp, data)
        VALUES(?, ?, ?, ?)
        """,
        rows_to_insert,
    )
    db.commit()


def _is_download_time(db: sqlite3.Connection) -> bool:
    raw = get_settings_by_keys(db, ["autoDownloadTimeLimited"]).get(
        "autoDownloadTimeLimited"
    )
    text = str(raw or "").strip()
    if not text:
        return True

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return True

    if not isinstance(parsed, dict):
        return True

    start_text = str(parsed.get("startTime") or "").strip()
    end_text = str(parsed.get("endTime") or "").strip()
    if not start_text or not end_text:
        return True

    try:
        start_time = datetime.strptime(start_text, "%H:%M").time()
        end_time = datetime.strptime(end_text, "%H:%M").time()
    except ValueError:
        return True

    if start_time.hour == 0 and start_time.minute == 0:
        if end_time.hour == 0 and end_time.minute == 0:
            return True

    now = datetime.now().time()
    if start_time > end_time:
        return now > start_time or now < end_time
    return now > start_time and now < end_time


def _persist_automation(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    chat_id: int,
    automation: dict[str, Any],
) -> None:
    update_auto_settings(
        db,
        telegram_id=telegram_id,
        chat_id=chat_id,
        auto_payload=deepcopy(automation),
    )


def _db_find_file_by_unique(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    unique_id: str,
) -> sqlite3.Row | None:
    return db.execute(
        """
        SELECT *
        FROM file_record
        WHERE telegram_id = ? AND unique_id = ?
        ORDER BY message_id DESC
        LIMIT 1
        """,
        (telegram_id, unique_id),
    ).fetchone()


def _db_upsert_tdlib_file_record(
    db: sqlite3.Connection,
    *,
    file_payload: dict[str, Any],
) -> None:
    telegram_id = _int_or_default(file_payload.get("telegramId"), 0)
    unique_id = str(file_payload.get("uniqueId") or "").strip()
    if telegram_id <= 0 or not unique_id:
        return

    existing = _db_find_file_by_unique(
        db,
        telegram_id=telegram_id,
        unique_id=unique_id,
    )

    extra_payload = file_payload.get("extra")
    extra_json = (
        json.dumps(extra_payload, separators=(",", ":"), ensure_ascii=False)
        if extra_payload is not None
        else None
    )

    completion_date = _int_or_default(file_payload.get("completionDate"), 0)
    completion_value: int | None = completion_date if completion_date > 0 else None
    download_status = str(file_payload.get("downloadStatus") or "idle")
    transfer_status = str(file_payload.get("transferStatus") or "idle")

    payload_values = {
        "id": _int_or_default(file_payload.get("id"), 0),
        "chat_id": _int_or_default(file_payload.get("chatId"), 0),
        "message_id": _int_or_default(file_payload.get("messageId"), 0),
        "media_album_id": _int_or_default(file_payload.get("mediaAlbumId"), 0),
        "date": _int_or_default(file_payload.get("date"), 0),
        "has_sensitive_content": 1
        if bool(file_payload.get("hasSensitiveContent"))
        else 0,
        "size": _int_or_default(file_payload.get("size"), 0),
        "downloaded_size": _int_or_default(file_payload.get("downloadedSize"), 0),
        "type": str(file_payload.get("type") or "file"),
        "mime_type": str(file_payload.get("mimeType") or "application/octet-stream"),
        "file_name": str(file_payload.get("fileName") or unique_id),
        "thumbnail": str(file_payload.get("thumbnail") or ""),
        "thumbnail_unique_id": None,
        "caption": str(file_payload.get("caption") or ""),
        "extra": extra_json,
        "local_path": str(file_payload.get("localPath") or ""),
        "download_status": download_status,
        "transfer_status": transfer_status,
        "start_date": _int_or_default(file_payload.get("startDate"), 0),
        "completion_date": completion_value,
        "thread_chat_id": _int_or_default(file_payload.get("threadChatId"), 0),
        "message_thread_id": _int_or_default(file_payload.get("messageThreadId"), 0),
        "reaction_count": _int_or_default(file_payload.get("reactionCount"), 0),
    }

    if existing is not None:
        existing_download_status = str(existing["download_status"] or "idle")
        existing_transfer_status = str(existing["transfer_status"] or "idle")

        incoming_download_status = payload_values["download_status"]
        if (
            existing_download_status == "completed"
            and incoming_download_status != "completed"
        ):
            payload_values["download_status"] = existing_download_status
        elif (
            existing_download_status in {"downloading", "paused", "error"}
            and incoming_download_status == "idle"
        ):
            payload_values["download_status"] = existing_download_status

        incoming_transfer_status = payload_values["transfer_status"]
        if (
            existing_transfer_status in {"transferring", "completed", "error"}
            and incoming_transfer_status == "idle"
        ):
            payload_values["transfer_status"] = existing_transfer_status

        if not payload_values["local_path"] and str(existing["local_path"] or ""):
            payload_values["local_path"] = str(existing["local_path"] or "")
        if (
            payload_values["completion_date"] is None
            and existing["completion_date"] is not None
        ):
            payload_values["completion_date"] = _int_or_default(
                existing["completion_date"], 0
            )
        if payload_values["thread_chat_id"] == 0:
            payload_values["thread_chat_id"] = _int_or_default(
                existing["thread_chat_id"],
                0,
            )
        if payload_values["message_thread_id"] == 0:
            payload_values["message_thread_id"] = _int_or_default(
                existing["message_thread_id"],
                0,
            )

    if existing is None:
        db.execute(
            """
            INSERT INTO file_record(
                id, unique_id, telegram_id, chat_id, message_id, media_album_id,
                date, has_sensitive_content, size, downloaded_size, type, mime_type,
                file_name, thumbnail, thumbnail_unique_id, caption, extra, local_path,
                download_status, transfer_status, start_date, completion_date, tags,
                thread_chat_id, message_thread_id, reaction_count
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload_values["id"],
                unique_id,
                telegram_id,
                payload_values["chat_id"],
                payload_values["message_id"],
                payload_values["media_album_id"],
                payload_values["date"],
                payload_values["has_sensitive_content"],
                payload_values["size"],
                payload_values["downloaded_size"],
                payload_values["type"],
                payload_values["mime_type"],
                payload_values["file_name"],
                payload_values["thumbnail"],
                payload_values["thumbnail_unique_id"],
                payload_values["caption"],
                payload_values["extra"],
                payload_values["local_path"],
                payload_values["download_status"],
                payload_values["transfer_status"],
                payload_values["start_date"],
                payload_values["completion_date"],
                None,
                payload_values["thread_chat_id"],
                payload_values["message_thread_id"],
                payload_values["reaction_count"],
            ),
        )
    else:
        db.execute(
            """
            UPDATE file_record
            SET id = ?,
                chat_id = ?,
                message_id = ?,
                media_album_id = ?,
                date = ?,
                has_sensitive_content = ?,
                size = ?,
                downloaded_size = ?,
                type = ?,
                mime_type = ?,
                file_name = ?,
                thumbnail = ?,
                caption = ?,
                extra = ?,
                local_path = ?,
                download_status = ?,
                transfer_status = ?,
                start_date = ?,
                completion_date = ?,
                thread_chat_id = ?,
                message_thread_id = ?,
                reaction_count = ?
            WHERE telegram_id = ? AND unique_id = ?
            """,
            (
                payload_values["id"],
                payload_values["chat_id"],
                payload_values["message_id"],
                payload_values["media_album_id"],
                payload_values["date"],
                payload_values["has_sensitive_content"],
                payload_values["size"],
                payload_values["downloaded_size"],
                payload_values["type"],
                payload_values["mime_type"],
                payload_values["file_name"],
                payload_values["thumbnail"],
                payload_values["caption"],
                payload_values["extra"],
                payload_values["local_path"],
                payload_values["download_status"],
                payload_values["transfer_status"],
                payload_values["start_date"],
                payload_values["completion_date"],
                payload_values["thread_chat_id"],
                payload_values["message_thread_id"],
                payload_values["reaction_count"],
                telegram_id,
                unique_id,
            ),
        )

    db.commit()


def _db_update_tdlib_file_status(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int,
    unique_id: str,
    status_payload: dict[str, Any],
) -> None:
    target = None
    normalized_unique = unique_id.strip()
    if normalized_unique:
        target = _db_find_file_by_unique(
            db,
            telegram_id=telegram_id,
            unique_id=normalized_unique,
        )

    if target is None and file_id > 0:
        target = db.execute(
            """
            SELECT *
            FROM file_record
            WHERE telegram_id = ? AND id = ?
            ORDER BY message_id DESC
            LIMIT 1
            """,
            (telegram_id, file_id),
        ).fetchone()

    if target is None:
        return

    resolved_unique = str(target["unique_id"] or normalized_unique)
    download_status = str(
        status_payload.get("downloadStatus") or target["download_status"]
    )
    local_path = str(status_payload.get("localPath") or "")
    completion_date = _int_or_default(status_payload.get("completionDate"), 0)
    completion_value: int | None = completion_date if completion_date > 0 else None

    db.execute(
        """
        UPDATE file_record
        SET downloaded_size = ?,
            download_status = ?,
            local_path = ?,
            completion_date = ?
        WHERE telegram_id = ? AND unique_id = ?
        """,
        (
            _int_or_default(status_payload.get("downloadedSize"), 0),
            download_status,
            local_path,
            completion_value,
            telegram_id,
            resolved_unique,
        ),
    )
    db.commit()


def _db_count_downloading_files(db: sqlite3.Connection, telegram_id: int) -> int:
    row = db.execute(
        """
        SELECT COUNT(*) AS count
        FROM file_record
        WHERE telegram_id = ?
          AND type != 'thumbnail'
          AND download_status = 'downloading'
        """,
        (telegram_id,),
    ).fetchone()
    return _int_or_default(row["count"] if row else 0, 0)


def _db_transfer_candidates(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    chat_id: int,
    limit: int = 200,
) -> list[dict[str, Any]]:
    rows = db.execute(
        """
        SELECT id, unique_id, telegram_id, chat_id
        FROM file_record
        WHERE telegram_id = ?
          AND chat_id = ?
          AND type != 'thumbnail'
          AND download_status = 'completed'
          AND transfer_status = 'idle'
          AND local_path IS NOT NULL
          AND TRIM(local_path) != ''
        ORDER BY completion_date DESC, message_id DESC
        LIMIT ?
        """,
        (telegram_id, chat_id, limit),
    ).fetchall()
    return [
        {
            "id": _int_or_default(row["id"], 0),
            "uniqueId": str(row["unique_id"] or ""),
            "telegramId": _int_or_default(row["telegram_id"], 0),
            "chatId": _int_or_default(row["chat_id"], 0),
        }
        for row in rows
    ]


def _db_file_for_transfer(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    unique_id: str,
) -> sqlite3.Row | None:
    return db.execute(
        """
        SELECT *
        FROM file_record
        WHERE telegram_id = ? AND unique_id = ?
        ORDER BY message_id DESC
        LIMIT 1
        """,
        (telegram_id, unique_id),
    ).fetchone()


def _db_update_transfer_status(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    unique_id: str,
    transfer_status: str,
    local_path: str | None = None,
) -> dict[str, Any] | None:
    row = _db_file_for_transfer(
        db,
        telegram_id=telegram_id,
        unique_id=unique_id,
    )
    if row is None:
        return None

    next_local_path = str(row["local_path"] or "") if local_path is None else local_path
    db.execute(
        """
        UPDATE file_record
        SET transfer_status = ?,
            local_path = ?
        WHERE telegram_id = ? AND unique_id = ?
        """,
        (transfer_status, next_local_path, telegram_id, unique_id),
    )
    db.commit()

    return {
        "fileId": _int_or_default(row["id"], 0),
        "uniqueId": unique_id,
        "transferStatus": transfer_status,
        "localPath": next_local_path,
    }


def _normalized_download_file_types(rule: dict[str, Any]) -> list[str]:
    file_types_raw = rule.get("fileTypes") if isinstance(rule, dict) else None
    default_order = ["photo", "video", "audio", "file"]
    if not isinstance(file_types_raw, list) or not file_types_raw:
        return default_order

    normalized: list[str] = []
    for item in file_types_raw:
        current = str(item or "").strip().lower()
        if current in {"photo", "video", "audio", "file", "media"}:
            if current not in normalized:
                normalized.append(current)
    return normalized or default_order


def _search_filter_type(file_type: str) -> str | None:
    mapping = {
        "media": "searchMessagesFilterPhotoAndVideo",
        "photo": "searchMessagesFilterPhoto",
        "video": "searchMessagesFilterVideo",
        "audio": "searchMessagesFilterAudio",
        "file": "searchMessagesFilterDocument",
    }
    return mapping.get(str(file_type or "").strip().lower())


def _match_download_rule(
    file_payload: dict[str, Any],
    *,
    automation: dict[str, Any],
    message: dict[str, Any] | None = None,
    skip_query_and_types: bool = False,
) -> bool:
    rule = automation.get("download", {}).get("rule", {})
    if not isinstance(rule, dict):
        rule = {}

    status = str(file_payload.get("downloadStatus") or "").strip().lower()
    if status in {"downloading", "completed"}:
        return False

    if not skip_query_and_types:
        query = str(rule.get("query") or "").strip().lower()
        if query:
            file_name = str(file_payload.get("fileName") or "").lower()
            caption = str(file_payload.get("caption") or "").lower()
            if query not in file_name and query not in caption:
                return False

        file_types = _normalized_download_file_types(rule)
        payload_type = str(file_payload.get("type") or "").strip().lower()
        if file_types:
            if "media" in file_types:
                if payload_type not in {"photo", "video"}:
                    return False
            elif payload_type not in file_types:
                return False

    filter_expr = str(rule.get("filterExpr") or "").strip()
    if filter_expr:
        if message is None:
            return False
        if not _evaluate_filter_expr(
            filter_expr,
            file_payload=file_payload,
            message=message,
        ):
            return False

    return True


def _queue_auto_download_candidate(candidate: dict[str, int]) -> None:
    telegram_id = _int_or_default(candidate.get("telegramId"), 0)
    chat_id = _int_or_default(candidate.get("chatId"), 0)
    message_id = _int_or_default(candidate.get("messageId"), 0)
    if telegram_id <= 0 or chat_id == 0 or message_id == 0:
        return

    key = (telegram_id, chat_id, message_id)
    if key in AUTO_DOWNLOAD_WAITING_KEYS:
        return

    if _auto_waiting_size(telegram_id) > AUTO_DOWNLOAD_MAX_WAITING_LENGTH:
        return

    AUTO_DOWNLOAD_WAITING_KEYS.add(key)
    AUTO_DOWNLOAD_WAITING.setdefault(telegram_id, deque()).append(
        {
            "telegramId": telegram_id,
            "chatId": chat_id,
            "messageId": message_id,
            "fileId": _int_or_default(candidate.get("fileId"), 0),
        }
    )


def _pop_auto_download_candidate(telegram_id: int) -> dict[str, int] | None:
    queue = AUTO_DOWNLOAD_WAITING.get(telegram_id)
    if not queue:
        return None

    candidate = queue.popleft()
    key = (
        telegram_id,
        _int_or_default(candidate.get("chatId"), 0),
        _int_or_default(candidate.get("messageId"), 0),
    )
    AUTO_DOWNLOAD_WAITING_KEYS.discard(key)
    if not queue:
        AUTO_DOWNLOAD_WAITING.pop(telegram_id, None)
    return candidate


def _auto_waiting_size(telegram_id: int) -> int:
    queue = AUTO_DOWNLOAD_WAITING.get(telegram_id)
    if not queue:
        return 0
    return len(queue)


def _queue_comment_thread_scan(
    *,
    telegram_id: int,
    source_chat_id: int,
    thread_chat_id: int,
    message_thread_id: int,
) -> None:
    if (
        telegram_id <= 0
        or source_chat_id == 0
        or thread_chat_id == 0
        or message_thread_id == 0
    ):
        return

    key = (telegram_id, thread_chat_id, message_thread_id)
    current = AUTO_DOWNLOAD_COMMENT_THREADS.get(key)
    if current is not None:
        return

    AUTO_DOWNLOAD_COMMENT_THREADS[key] = {
        "telegramId": telegram_id,
        "sourceChatId": source_chat_id,
        "threadChatId": thread_chat_id,
        "messageThreadId": message_thread_id,
        "nextFileType": "",
        "nextFromMessageId": 0,
        "isComplete": False,
    }


def _queue_transfer_candidate(candidate: dict[str, Any]) -> None:
    telegram_id = _int_or_default(candidate.get("telegramId"), 0)
    unique_id = str(candidate.get("uniqueId") or "").strip()
    if telegram_id <= 0 or not unique_id:
        return

    key = (telegram_id, unique_id)
    if key in TRANSFER_WAITING_KEYS:
        return

    TRANSFER_WAITING_KEYS.add(key)
    TRANSFER_WAITING.append(
        {
            "telegramId": telegram_id,
            "chatId": _int_or_default(candidate.get("chatId"), 0),
            "uniqueId": unique_id,
            "fileId": _int_or_default(candidate.get("id"), 0),
        }
    )


def _pop_transfer_candidate() -> dict[str, Any] | None:
    if not TRANSFER_WAITING:
        return None

    candidate = TRANSFER_WAITING.popleft()
    TRANSFER_WAITING_KEYS.discard(
        (
            _int_or_default(candidate.get("telegramId"), 0),
            str(candidate.get("uniqueId") or "").strip(),
        )
    )
    return candidate


def _tdlib_chat_history_batch(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    chat_id: int,
    from_message_id: int,
    limit: int,
) -> list[dict[str, Any]]:
    if not _load_tdlib_session_for_account(td_manager, telegram_id, root_path):
        raise RuntimeError("TDLib is not ready yet. Please try again.")

    result = td_manager.request(
        str(telegram_id),
        {
            "@type": "getChatHistory",
            "chat_id": chat_id,
            "from_message_id": from_message_id,
            "offset": -1 if from_message_id > 0 else 0,
            "limit": limit,
            "only_local": False,
        },
        timeout_seconds=25.0,
    )
    if str(result.get("@type") or "") == "error":
        raise RuntimeError(str(result.get("message") or "Failed to fetch chat history"))

    messages = result.get("messages")
    if not isinstance(messages, list):
        return []
    return [item for item in messages if isinstance(item, dict)]


def _tdlib_search_chat_messages_batch(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    chat_id: int,
    from_message_id: int,
    query: str,
    file_type: str,
    message_thread_id: int = 0,
    limit: int = AUTO_DOWNLOAD_MAX_WAITING_LENGTH,
) -> tuple[list[dict[str, Any]], int]:
    if not _load_tdlib_session_for_account(td_manager, telegram_id, root_path):
        raise RuntimeError("TDLib is not ready yet. Please try again.")

    filter_type = _search_filter_type(file_type)
    payload: dict[str, Any] = {
        "@type": "searchChatMessages",
        "chat_id": chat_id,
        "query": query,
        "from_message_id": from_message_id,
        "offset": 0,
        "limit": max(1, min(limit, 100)),
        "filter": {"@type": filter_type} if filter_type else None,
    }
    if message_thread_id > 0:
        payload["topic_id"] = {
            "@type": "messageTopicThread",
            "message_thread_id": message_thread_id,
        }

    result = td_manager.request(
        str(telegram_id),
        payload,
        timeout_seconds=25.0,
    )
    if str(result.get("@type") or "") == "error":
        raise RuntimeError(str(result.get("message") or "Failed to search messages"))

    raw_messages = result.get("messages")
    messages = (
        [item for item in raw_messages if isinstance(item, dict)]
        if isinstance(raw_messages, list)
        else []
    )
    next_from_message_id = _int_or_default(result.get("next_from_message_id"), 0)
    return messages, next_from_message_id


def _auto_scan_is_blocked(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
) -> bool:
    limit = _auto_download_limit(db)
    downloading = _db_count_downloading_files(db, telegram_id)
    if downloading >= limit:
        return True
    return _auto_waiting_size(telegram_id) > AUTO_DOWNLOAD_MAX_WAITING_LENGTH


def _automation_supports_comment_download(automation: dict[str, Any]) -> bool:
    download_cfg = automation.get("download") if isinstance(automation, dict) else None
    if not isinstance(download_cfg, dict) or not bool(download_cfg.get("enabled")):
        return False

    rule = download_cfg.get("rule") if isinstance(download_cfg, dict) else None
    if not isinstance(rule, dict):
        return False
    return bool(rule.get("downloadCommentFiles"))


async def _scan_auto_download_scope(
    *,
    db: sqlite3.Connection,
    td_manager: TdlibAuthManager,
    telegram_id: int,
    root_path: str,
    chat_id: int,
    automation: dict[str, Any],
    next_file_type: str,
    next_from_message_id: int,
    message_thread_id: int = 0,
) -> tuple[str, int, bool, int]:
    if _auto_scan_is_blocked(db, telegram_id=telegram_id):
        return next_file_type, next_from_message_id, False, 0

    download_cfg = automation.get("download", {})
    rule = download_cfg.get("rule", {}) if isinstance(download_cfg, dict) else {}
    if not isinstance(rule, dict):
        rule = {}

    file_types = _normalized_download_file_types(rule)
    current_type = str(next_file_type or file_types[0]).strip().lower()
    if current_type not in file_types:
        current_type = file_types[0]
    current_cursor = max(0, next_from_message_id)
    query = str(rule.get("query") or "").strip()
    added = 0

    for _ in range(8):
        if _auto_scan_is_blocked(db, telegram_id=telegram_id):
            break

        try:
            messages, next_cursor = await asyncio.to_thread(
                _tdlib_search_chat_messages_batch,
                td_manager,
                telegram_id=telegram_id,
                root_path=root_path,
                chat_id=chat_id,
                from_message_id=current_cursor,
                query=query,
                file_type=current_type,
                message_thread_id=message_thread_id,
                limit=AUTO_DOWNLOAD_MAX_WAITING_LENGTH,
            )
        except Exception as exc:
            logger.warning(
                "Auto-download scope scan failed for telegram=%s chat=%s type=%s thread=%s: %s",
                telegram_id,
                chat_id,
                current_type,
                message_thread_id,
                exc,
            )
            return current_type, current_cursor, False, added

        if not messages:
            next_type_index = file_types.index(current_type) + 1
            if next_type_index >= len(file_types):
                return current_type, current_cursor, True, added

            current_type = file_types[next_type_index]
            current_cursor = 0
            continue

        for message in messages:
            file_payload = _td_message_to_file(telegram_id, message)
            if file_payload is None:
                continue

            if not _match_download_rule(
                file_payload,
                automation=automation,
                message=message,
                skip_query_and_types=True,
            ):
                continue

            unique_id = str(file_payload.get("uniqueId") or "").strip()
            if unique_id:
                existing = _db_find_file_by_unique(
                    db,
                    telegram_id=telegram_id,
                    unique_id=unique_id,
                )
                if existing is not None:
                    existing_status = str(existing["download_status"] or "idle")
                    if existing_status != "idle":
                        continue

            _db_upsert_tdlib_file_record(db, file_payload=file_payload)
            _queue_auto_download_candidate(
                {
                    "telegramId": telegram_id,
                    "chatId": _int_or_default(file_payload.get("chatId"), chat_id),
                    "messageId": _int_or_default(file_payload.get("messageId"), 0),
                    "fileId": _int_or_default(file_payload.get("id"), 0),
                }
            )
            added += 1

        if next_cursor <= 0:
            next_type_index = file_types.index(current_type) + 1
            if next_type_index >= len(file_types):
                return current_type, 0, True, added

            current_type = file_types[next_type_index]
            current_cursor = 0
            continue

        current_cursor = next_cursor
        break

    return current_type, current_cursor, False, added


async def _run_preload_scan_cycle(app: FastAPI) -> None:
    td_manager = _tdlib_manager_from_app(app)
    if td_manager is None:
        return

    db: sqlite3.Connection = app.state.db
    automations = get_automation_map(db)
    if not automations:
        return

    root_path_cache: dict[int, str | None] = {}
    for (telegram_id, chat_id), automation in automations.items():
        preload_cfg = (
            automation.get("preload") if isinstance(automation, dict) else None
        )
        if not isinstance(preload_cfg, dict) or not bool(preload_cfg.get("enabled")):
            continue

        state = _int_or_default(automation.get("state"), 0)
        if _state_is_enabled(state, HISTORY_PRELOAD_STATE):
            continue

        root_path = _tdlib_account_root_path(
            app,
            db,
            telegram_id,
            root_path_cache,
        )
        if root_path is None:
            continue

        cursor = _int_or_default(preload_cfg.get("nextFromMessageId"), 0)
        try:
            messages = await asyncio.to_thread(
                _tdlib_chat_history_batch,
                td_manager,
                telegram_id=telegram_id,
                root_path=root_path,
                chat_id=chat_id,
                from_message_id=cursor,
                limit=100,
            )
        except Exception as exc:
            logger.warning(
                "Preload scan failed for telegram=%s chat=%s: %s",
                telegram_id,
                chat_id,
                exc,
            )
            continue

        if not messages:
            automation["state"] = _state_enable(state, HISTORY_PRELOAD_STATE)
            _persist_automation(
                db,
                telegram_id=telegram_id,
                chat_id=chat_id,
                automation=automation,
            )
            continue

        for message in messages:
            file_payload = _td_message_to_file(telegram_id, message)
            if file_payload is None:
                continue
            _db_upsert_tdlib_file_record(db, file_payload=file_payload)

        automation.setdefault("preload", {})["nextFromMessageId"] = _int_or_default(
            messages[-1].get("id"),
            cursor,
        )
        _persist_automation(
            db,
            telegram_id=telegram_id,
            chat_id=chat_id,
            automation=automation,
        )


async def _run_auto_download_scan_cycle(app: FastAPI) -> None:
    db: sqlite3.Connection = app.state.db
    if not _is_download_time(db):
        return

    td_manager = _tdlib_manager_from_app(app)
    if td_manager is None:
        return

    automations = get_automation_map(db)
    if not automations:
        return

    root_path_cache: dict[int, str | None] = {}
    for (telegram_id, chat_id), automation in automations.items():
        download_cfg = (
            automation.get("download") if isinstance(automation, dict) else None
        )
        if not isinstance(download_cfg, dict) or not bool(download_cfg.get("enabled")):
            continue

        rule = download_cfg.get("rule")
        if not isinstance(rule, dict) or not bool(rule.get("downloadHistory", True)):
            continue

        state = _int_or_default(automation.get("state"), 0)
        scan_complete = _state_is_enabled(state, HISTORY_DOWNLOAD_SCAN_STATE)
        comment_enabled = _automation_supports_comment_download(automation)
        comment_keys = [
            key
            for key, item in AUTO_DOWNLOAD_COMMENT_THREADS.items()
            if _int_or_default(item.get("telegramId"), 0) == telegram_id
            and _int_or_default(item.get("sourceChatId"), 0) == chat_id
            and not bool(item.get("isComplete"))
        ]
        has_pending_comment_scan = bool(comment_keys)
        if scan_complete and not (comment_enabled and has_pending_comment_scan):
            if _auto_waiting_size(telegram_id) == 0 and not _state_is_enabled(
                state,
                HISTORY_DOWNLOAD_STATE,
            ):
                automation["state"] = _state_enable(state, HISTORY_DOWNLOAD_STATE)
                _persist_automation(
                    db,
                    telegram_id=telegram_id,
                    chat_id=chat_id,
                    automation=automation,
                )
            continue

        root_path = _tdlib_account_root_path(
            app,
            db,
            telegram_id,
            root_path_cache,
        )
        if root_path is None:
            continue

        if not scan_complete:
            (
                next_file_type,
                next_cursor,
                is_complete,
                _,
            ) = await _scan_auto_download_scope(
                db=db,
                td_manager=td_manager,
                telegram_id=telegram_id,
                root_path=root_path,
                chat_id=chat_id,
                automation=automation,
                next_file_type=str(download_cfg.get("nextFileType") or ""),
                next_from_message_id=_int_or_default(
                    download_cfg.get("nextFromMessageId"), 0
                ),
                message_thread_id=0,
            )

            automation.setdefault("download", {})["nextFileType"] = next_file_type
            automation.setdefault("download", {})["nextFromMessageId"] = next_cursor

            next_state = state
            if is_complete:
                next_state = _state_enable(next_state, HISTORY_DOWNLOAD_SCAN_STATE)
            if is_complete and _auto_waiting_size(telegram_id) == 0:
                next_state = _state_enable(next_state, HISTORY_DOWNLOAD_STATE)
            automation["state"] = next_state
            _persist_automation(
                db,
                telegram_id=telegram_id,
                chat_id=chat_id,
                automation=automation,
            )

        if not comment_enabled:
            for key in comment_keys:
                AUTO_DOWNLOAD_COMMENT_THREADS.pop(key, None)
            continue

        for key in comment_keys:
            comment_scan = AUTO_DOWNLOAD_COMMENT_THREADS.get(key)
            if not isinstance(comment_scan, dict):
                continue

            thread_chat_id = _int_or_default(comment_scan.get("threadChatId"), 0)
            message_thread_id = _int_or_default(comment_scan.get("messageThreadId"), 0)
            if thread_chat_id == 0 or message_thread_id == 0:
                AUTO_DOWNLOAD_COMMENT_THREADS.pop(key, None)
                continue

            (
                next_file_type,
                next_cursor,
                comment_complete,
                _,
            ) = await _scan_auto_download_scope(
                db=db,
                td_manager=td_manager,
                telegram_id=telegram_id,
                root_path=root_path,
                chat_id=thread_chat_id,
                automation=automation,
                next_file_type=str(comment_scan.get("nextFileType") or ""),
                next_from_message_id=_int_or_default(
                    comment_scan.get("nextFromMessageId"), 0
                ),
                message_thread_id=message_thread_id,
            )

            if comment_complete:
                AUTO_DOWNLOAD_COMMENT_THREADS.pop(key, None)
            else:
                comment_scan["nextFileType"] = next_file_type
                comment_scan["nextFromMessageId"] = next_cursor


async def _run_auto_download_tick(app: FastAPI) -> None:
    db: sqlite3.Connection = app.state.db
    if not _is_download_time(db):
        return

    td_manager = _tdlib_manager_from_app(app)
    if td_manager is None or not AUTO_DOWNLOAD_WAITING:
        return

    limit = _auto_download_limit(db)
    root_path_cache: dict[int, str | None] = {}

    for telegram_id in list(AUTO_DOWNLOAD_WAITING.keys()):
        surplus = max(0, limit - _db_count_downloading_files(db, telegram_id))
        if surplus <= 0:
            continue

        automation_map = get_automation_map(db, telegram_id=telegram_id)
        root_path = _tdlib_account_root_path(app, db, telegram_id, root_path_cache)
        if root_path is None:
            continue

        while surplus > 0:
            candidate = _pop_auto_download_candidate(telegram_id)
            if candidate is None:
                break

            try:
                file_record = await asyncio.to_thread(
                    _start_tdlib_download_for_message,
                    td_manager,
                    telegram_id=telegram_id,
                    root_path=root_path,
                    chat_id=_int_or_default(candidate.get("chatId"), 0),
                    message_id=_int_or_default(candidate.get("messageId"), 0),
                    file_id=_int_or_default(candidate.get("fileId"), 0),
                )
            except Exception as exc:
                logger.warning(
                    "Auto-download start failed for telegram=%s candidate=%s: %s",
                    telegram_id,
                    candidate,
                    exc,
                )
                continue

            _db_upsert_tdlib_file_record(db, file_payload=file_record)
            status_payload = _td_file_status_payload(file_record)
            await _emit_ws_payload(
                _build_ws_payload(EVENT_TYPE_FILE_STATUS, status_payload),
            )

            candidate_chat_id = _int_or_default(candidate.get("chatId"), 0)
            automation = automation_map.get((telegram_id, candidate_chat_id))
            if isinstance(automation, dict) and _automation_supports_comment_download(
                automation
            ):
                thread_chat_id = _int_or_default(file_record.get("threadChatId"), 0)
                message_thread_id = _int_or_default(
                    file_record.get("messageThreadId"), 0
                )
                file_chat_id = _int_or_default(
                    file_record.get("chatId"), candidate_chat_id
                )
                if (
                    thread_chat_id > 0
                    and message_thread_id > 0
                    and thread_chat_id != file_chat_id
                ):
                    _queue_comment_thread_scan(
                        telegram_id=telegram_id,
                        source_chat_id=candidate_chat_id,
                        thread_chat_id=thread_chat_id,
                        message_thread_id=message_thread_id,
                    )

            monitor_file_id = _int_or_default(file_record.get("id"), 0)
            if monitor_file_id > 0:
                _ensure_tdlib_download_monitor(
                    app,
                    session_id=f"worker:{telegram_id}",
                    telegram_id=telegram_id,
                    file_id=monitor_file_id,
                    unique_id=str(file_record.get("uniqueId") or ""),
                )

            surplus -= 1


async def _emit_transfer_status(payload: dict[str, Any]) -> None:
    await _emit_ws_payload(
        _build_ws_payload(EVENT_TYPE_FILE_STATUS, payload),
    )


async def _run_transfer_scan_cycle(app: FastAPI) -> None:
    db: sqlite3.Connection = app.state.db
    automations = get_automation_map(db)
    if not automations:
        return

    for (telegram_id, chat_id), automation in automations.items():
        transfer_cfg = (
            automation.get("transfer") if isinstance(automation, dict) else None
        )
        if not isinstance(transfer_cfg, dict) or not bool(transfer_cfg.get("enabled")):
            continue

        rule = transfer_cfg.get("rule")
        if not isinstance(rule, dict):
            continue

        candidates = _db_transfer_candidates(
            db,
            telegram_id=telegram_id,
            chat_id=chat_id,
            limit=200,
        )
        for item in candidates:
            _queue_transfer_candidate(item)

        transfer_history = bool(rule.get("transferHistory", True))
        state = _int_or_default(automation.get("state"), 0)
        if (
            transfer_history
            and not candidates
            and not _state_is_enabled(state, HISTORY_TRANSFER_STATE)
        ):
            automation["state"] = _state_enable(state, HISTORY_TRANSFER_STATE)
            _persist_automation(
                db,
                telegram_id=telegram_id,
                chat_id=chat_id,
                automation=automation,
            )


async def _run_transfer_tick(app: FastAPI) -> None:
    candidate = _pop_transfer_candidate()
    if candidate is None:
        return

    db: sqlite3.Connection = app.state.db
    telegram_id = _int_or_default(candidate.get("telegramId"), 0)
    chat_id = _int_or_default(candidate.get("chatId"), 0)
    unique_id = str(candidate.get("uniqueId") or "")
    if telegram_id <= 0 or chat_id == 0 or not unique_id:
        return

    automations = get_automation_map(db, telegram_id=telegram_id)
    automation = automations.get((telegram_id, chat_id))
    if not isinstance(automation, dict):
        return

    transfer_cfg = automation.get("transfer")
    if not isinstance(transfer_cfg, dict) or not bool(transfer_cfg.get("enabled")):
        return

    rule = transfer_cfg.get("rule")
    if not isinstance(rule, dict):
        return

    row = _db_file_for_transfer(
        db,
        telegram_id=telegram_id,
        unique_id=unique_id,
    )
    if row is None:
        return

    in_progress_payload = _db_update_transfer_status(
        db,
        telegram_id=telegram_id,
        unique_id=unique_id,
        transfer_status="transferring",
    )
    if in_progress_payload is not None:
        await _emit_transfer_status(in_progress_payload)

    try:
        transfer_status, resolved_path = await asyncio.to_thread(
            _execute_transfer,
            row,
            rule,
        )
    except Exception as exc:
        logger.warning(
            "Transfer failed for telegram=%s chat=%s unique=%s: %s",
            telegram_id,
            chat_id,
            unique_id,
            exc,
        )
        transfer_status = "error"
        resolved_path = None

    final_payload = _db_update_transfer_status(
        db,
        telegram_id=telegram_id,
        unique_id=unique_id,
        transfer_status=transfer_status,
        local_path=resolved_path,
    )
    if final_payload is not None:
        await _emit_transfer_status(final_payload)


async def _background_workers_loop(app: FastAPI) -> None:
    last_preload = 0.0
    last_auto_scan = 0.0
    last_auto_tick = 0.0
    last_transfer_scan = 0.0
    last_transfer_tick = 0.0
    last_speed_persist = 0.0

    while True:
        now = time.monotonic()

        try:
            if now - last_preload >= PRELOAD_SCAN_INTERVAL_SECONDS:
                last_preload = now
                await _run_preload_scan_cycle(app)

            if now - last_auto_scan >= AUTO_DOWNLOAD_SCAN_INTERVAL_SECONDS:
                last_auto_scan = now
                await _run_auto_download_scan_cycle(app)

            if now - last_auto_tick >= AUTO_DOWNLOAD_TICK_INTERVAL_SECONDS:
                last_auto_tick = now
                await _run_auto_download_tick(app)

            if now - last_transfer_scan >= TRANSFER_SCAN_INTERVAL_SECONDS:
                last_transfer_scan = now
                await _run_transfer_scan_cycle(app)

            if now - last_transfer_tick >= TRANSFER_TICK_INTERVAL_SECONDS:
                last_transfer_tick = now
                await _run_transfer_tick(app)

            db: sqlite3.Connection = app.state.db
            speed_interval_seconds = float(_avg_speed_interval(db))
            if now - last_speed_persist >= speed_interval_seconds:
                last_speed_persist = now
                _persist_speed_statistics(db)
        except Exception as exc:
            logger.exception("Background worker loop error: %s", exc)

        await asyncio.sleep(1.0)


def _apply_chat_auto_settings(
    chats: list[dict[str, Any]],
    *,
    telegram_id: int,
    automation_map: dict[tuple[int, int], dict[str, Any]],
) -> list[dict[str, Any]]:
    for chat in chats:
        chat_id = _int_or_default(chat.get("id"), 0)
        auto = automation_map.get((telegram_id, chat_id))
        chat["auto"] = (
            deepcopy(auto) if isinstance(auto, dict) else _default_chat_auto()
        )
    return chats


def _tdlib_account_root_path(
    app: FastAPI,
    db: sqlite3.Connection,
    telegram_id: int,
    cache: dict[int, str | None] | None = None,
) -> str | None:
    if cache is not None and telegram_id in cache:
        return cache[telegram_id]

    config: AppConfig = app.state.config
    account = get_telegram_account(
        db,
        telegram_id=telegram_id,
        app_root=str(config.app_root),
    )
    root_path = (
        str(account.get("rootPath") or "").strip() if isinstance(account, dict) else ""
    )
    result = root_path or None
    if cache is not None:
        cache[telegram_id] = result
    return result


def _td_file_status_payload(file_record: dict[str, Any]) -> dict[str, Any]:
    return {
        "fileId": _int_or_default(file_record.get("id"), 0),
        "uniqueId": str(file_record.get("uniqueId") or ""),
        "downloadStatus": str(file_record.get("downloadStatus") or "idle"),
        "localPath": str(file_record.get("localPath") or ""),
        "completionDate": _int_or_default(file_record.get("completionDate"), 0),
        "downloadedSize": _int_or_default(file_record.get("downloadedSize"), 0),
        "transferStatus": str(file_record.get("transferStatus") or "idle"),
    }


def _cache_tdlib_file_preview(
    *,
    telegram_id: int,
    unique_id: str,
    file_id: int | None = None,
    mime_type: str | None = None,
    local_path: str | None = None,
) -> None:
    normalized_unique = unique_id.strip()
    if not normalized_unique:
        return

    key = (telegram_id, normalized_unique)
    with STATE_LOCK:
        current = dict(TDLIB_FILE_PREVIEW_CACHE.get(key) or {})
        if file_id is not None and file_id > 0:
            current["fileId"] = file_id
        if mime_type:
            current["mimeType"] = str(mime_type)
        if local_path:
            current["path"] = str(local_path)
        current["updatedAt"] = int(time.time() * 1000)
        TDLIB_FILE_PREVIEW_CACHE[key] = current


def _evict_tdlib_file_preview(
    *,
    telegram_id: int,
    unique_id: str | None = None,
    file_id: int | None = None,
) -> None:
    normalized_unique = str(unique_id or "").strip()
    normalized_file_id = int(file_id or 0)

    with STATE_LOCK:
        to_delete: list[tuple[int, str]] = []
        for key, value in TDLIB_FILE_PREVIEW_CACHE.items():
            key_telegram_id, key_unique_id = key
            if key_telegram_id != telegram_id:
                continue

            matches_unique = (
                bool(normalized_unique) and key_unique_id == normalized_unique
            )
            matches_file_id = (
                normalized_file_id > 0
                and _int_or_default(value.get("fileId"), 0) == normalized_file_id
            )
            if matches_unique or matches_file_id:
                to_delete.append(key)

        for key in to_delete:
            TDLIB_FILE_PREVIEW_CACHE.pop(key, None)


def _cached_tdlib_file_preview(
    *,
    telegram_id: int,
    unique_id: str,
) -> dict[str, Any] | None:
    key = (telegram_id, unique_id.strip())
    with STATE_LOCK:
        entry = TDLIB_FILE_PREVIEW_CACHE.get(key)
        if entry is None:
            return None
        return dict(entry)


def _unique_id_from_tdlib_file_cache(
    *,
    telegram_id: int,
    file_id: int,
) -> str:
    if file_id <= 0:
        return ""

    with STATE_LOCK:
        for (
            cache_telegram_id,
            cache_unique_id,
        ), payload in TDLIB_FILE_PREVIEW_CACHE.items():
            if cache_telegram_id != telegram_id:
                continue
            if _int_or_default(payload.get("fileId"), 0) == file_id:
                return cache_unique_id
    return ""


def _resolve_tdlib_file_reference(
    *,
    telegram_id: int,
    file_id: int,
    unique_id: str,
) -> tuple[int, str]:
    normalized_unique = unique_id.strip()
    normalized_file_id = file_id

    if normalized_file_id <= 0 and normalized_unique:
        cached = _cached_tdlib_file_preview(
            telegram_id=telegram_id,
            unique_id=normalized_unique,
        )
        normalized_file_id = _int_or_default((cached or {}).get("fileId"), 0)

    if not normalized_unique and normalized_file_id > 0:
        normalized_unique = _unique_id_from_tdlib_file_cache(
            telegram_id=telegram_id,
            file_id=normalized_file_id,
        )

    return normalized_file_id, normalized_unique


def _media_type_for_path(path: str, hint: str | None) -> str:
    normalized_hint = str(hint or "").strip()
    if normalized_hint:
        return normalized_hint
    guessed, _ = mimetypes.guess_type(path)
    return str(guessed or "application/octet-stream")


def _resolve_tdlib_preview_info(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    unique_id: str,
) -> dict[str, Any] | None:
    cached = _cached_tdlib_file_preview(telegram_id=telegram_id, unique_id=unique_id)
    if cached is None:
        return None

    path_value = str(cached.get("path") or "").strip()
    if path_value:
        path_obj = Path(path_value)
        if path_obj.exists() and path_obj.is_file():
            return {
                "path": str(path_obj),
                "mimeType": _media_type_for_path(
                    str(path_obj),
                    str(cached.get("mimeType") or ""),
                ),
            }

    file_id = _int_or_default(cached.get("fileId"), 0)
    if file_id == 0:
        return None

    if not _load_tdlib_session_for_account(td_manager, telegram_id, root_path):
        return None

    td_file = td_manager.request(
        str(telegram_id),
        {
            "@type": "getFile",
            "file_id": file_id,
        },
        timeout_seconds=15.0,
    )
    if str(td_file.get("@type") or "") == "error":
        return None

    local = td_file.get("local") if isinstance(td_file.get("local"), dict) else {}
    if not bool(local.get("is_downloading_completed")):
        return None

    resolved_path = str(local.get("path") or "").strip()
    if not resolved_path:
        return None

    resolved_obj = Path(resolved_path)
    if not resolved_obj.exists() or not resolved_obj.is_file():
        return None

    _cache_tdlib_file_preview(
        telegram_id=telegram_id,
        unique_id=unique_id,
        file_id=file_id,
        mime_type=str(cached.get("mimeType") or ""),
        local_path=str(resolved_obj),
    )

    return {
        "path": str(resolved_obj),
        "mimeType": _media_type_for_path(
            str(resolved_obj),
            str(cached.get("mimeType") or ""),
        ),
    }


def _stop_tdlib_download_monitor(
    *,
    session_id: str,
    telegram_id: int,
    file_id: int,
) -> None:
    if file_id <= 0:
        return

    key = (session_id, telegram_id, file_id)
    with STATE_LOCK:
        task = TDLIB_DOWNLOAD_TASKS.pop(key, None)
        TDLIB_DOWNLOAD_PROGRESS.pop(key, None)

    if task is not None and not task.done():
        task.cancel()


def _tdlib_get_file_payload(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    file_id: int,
) -> dict[str, Any]:
    if file_id <= 0:
        raise RuntimeError("File id is required")
    if not _load_tdlib_session_for_account(td_manager, telegram_id, root_path):
        raise RuntimeError("TDLib is not ready yet. Please try again.")

    result = td_manager.request(
        str(telegram_id),
        {
            "@type": "getFile",
            "file_id": file_id,
        },
        timeout_seconds=15.0,
    )
    if str(result.get("@type") or "") == "error":
        raise RuntimeError(str(result.get("message") or "File not found"))
    return result


def _tdlib_cancel_download_fallback(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    file_id: int,
    unique_id: str,
) -> dict[str, Any]:
    resolved_file_id, resolved_unique_id = _resolve_tdlib_file_reference(
        telegram_id=telegram_id,
        file_id=file_id,
        unique_id=unique_id,
    )
    if resolved_file_id <= 0:
        raise RuntimeError("File not found")

    if not _load_tdlib_session_for_account(td_manager, telegram_id, root_path):
        raise RuntimeError("TDLib is not ready yet. Please try again.")

    cancel_result = td_manager.request(
        str(telegram_id),
        {
            "@type": "cancelDownloadFile",
            "file_id": resolved_file_id,
            "only_if_pending": False,
        },
        timeout_seconds=20.0,
    )
    if str(cancel_result.get("@type") or "") == "error":
        raise RuntimeError(
            str(cancel_result.get("message") or "Failed to cancel download")
        )

    current = _tdlib_get_file_payload(
        td_manager,
        telegram_id=telegram_id,
        root_path=root_path,
        file_id=resolved_file_id,
    )
    remote = current.get("remote") if isinstance(current.get("remote"), dict) else {}
    resolved_unique_id = (
        str(remote.get("unique_id") or "").strip() or resolved_unique_id
    )
    _cache_tdlib_file_preview(
        telegram_id=telegram_id,
        unique_id=resolved_unique_id,
        file_id=resolved_file_id,
    )
    if unique_id.strip() and unique_id.strip() != resolved_unique_id:
        _cache_tdlib_file_preview(
            telegram_id=telegram_id,
            unique_id=unique_id.strip(),
            file_id=resolved_file_id,
        )

    return {
        "fileId": resolved_file_id,
        "uniqueId": resolved_unique_id,
        "downloadStatus": "idle",
        "localPath": "",
        "completionDate": 0,
        "downloadedSize": 0,
        "transferStatus": "idle",
    }


def _tdlib_toggle_pause_download_fallback(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    file_id: int,
    unique_id: str,
    is_paused: bool | None,
) -> tuple[dict[str, Any], bool]:
    resolved_file_id, resolved_unique_id = _resolve_tdlib_file_reference(
        telegram_id=telegram_id,
        file_id=file_id,
        unique_id=unique_id,
    )
    if resolved_file_id <= 0:
        raise RuntimeError("File not found")

    current = _tdlib_get_file_payload(
        td_manager,
        telegram_id=telegram_id,
        root_path=root_path,
        file_id=resolved_file_id,
    )
    local = current.get("local") if isinstance(current.get("local"), dict) else {}
    currently_downloading = bool(local.get("is_downloading_active"))
    target_pause = is_paused if is_paused is not None else currently_downloading

    if target_pause:
        action_result = td_manager.request(
            str(telegram_id),
            {
                "@type": "cancelDownloadFile",
                "file_id": resolved_file_id,
                "only_if_pending": False,
            },
            timeout_seconds=20.0,
        )
        if str(action_result.get("@type") or "") == "error":
            raise RuntimeError(
                str(action_result.get("message") or "Failed to pause download")
            )

        refreshed = _tdlib_get_file_payload(
            td_manager,
            telegram_id=telegram_id,
            root_path=root_path,
            file_id=resolved_file_id,
        )
        payload = _td_status_payload_from_td_file(
            refreshed,
            telegram_id=telegram_id,
            fallback_unique_id=resolved_unique_id,
        )
        if payload["downloadStatus"] not in {"completed", "downloading"}:
            payload["downloadStatus"] = (
                "paused"
                if _int_or_default(payload.get("downloadedSize"), 0) > 0
                else "idle"
            )
        return payload, False

    action_result = td_manager.request(
        str(telegram_id),
        {
            "@type": "downloadFile",
            "file_id": resolved_file_id,
            "priority": 16,
            "offset": 0,
            "limit": 0,
            "synchronous": False,
        },
        timeout_seconds=20.0,
    )
    if str(action_result.get("@type") or "") == "error":
        raise RuntimeError(
            str(action_result.get("message") or "Failed to resume download")
        )

    refreshed = _tdlib_get_file_payload(
        td_manager,
        telegram_id=telegram_id,
        root_path=root_path,
        file_id=resolved_file_id,
    )
    payload = _td_status_payload_from_td_file(
        refreshed,
        telegram_id=telegram_id,
        fallback_unique_id=resolved_unique_id,
    )
    if payload["downloadStatus"] != "completed":
        payload["downloadStatus"] = "downloading"
    return payload, True


def _tdlib_remove_file_fallback(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    file_id: int,
    unique_id: str,
) -> dict[str, Any]:
    resolved_file_id, resolved_unique_id = _resolve_tdlib_file_reference(
        telegram_id=telegram_id,
        file_id=file_id,
        unique_id=unique_id,
    )
    if resolved_file_id <= 0 and not resolved_unique_id:
        raise RuntimeError("File not found")

    if not _load_tdlib_session_for_account(td_manager, telegram_id, root_path):
        raise RuntimeError("TDLib is not ready yet. Please try again.")

    if resolved_file_id > 0:
        td_manager.request(
            str(telegram_id),
            {
                "@type": "cancelDownloadFile",
                "file_id": resolved_file_id,
                "only_if_pending": False,
            },
            timeout_seconds=20.0,
        )
        td_manager.request(
            str(telegram_id),
            {
                "@type": "deleteFile",
                "file_id": resolved_file_id,
            },
            timeout_seconds=20.0,
        )

    _evict_tdlib_file_preview(
        telegram_id=telegram_id,
        unique_id=resolved_unique_id or unique_id,
        file_id=resolved_file_id,
    )

    return {
        "fileId": resolved_file_id,
        "uniqueId": resolved_unique_id or unique_id,
        "downloadStatus": "idle",
        "localPath": "",
        "completionDate": 0,
        "downloadedSize": 0,
        "transferStatus": "idle",
        "removed": True,
    }


def _td_file_to_ws(file_payload: dict[str, Any]) -> dict[str, Any]:
    local_raw = (
        file_payload.get("local") if isinstance(file_payload.get("local"), dict) else {}
    )
    remote_raw = (
        file_payload.get("remote")
        if isinstance(file_payload.get("remote"), dict)
        else {}
    )
    return {
        "id": _int_or_default(file_payload.get("id"), 0),
        "size": _int_or_default(file_payload.get("size"), 0),
        "expectedSize": _int_or_default(file_payload.get("expected_size"), 0),
        "local": {
            "path": str(local_raw.get("path") or ""),
            "canBeDownloaded": bool(local_raw.get("can_be_downloaded")),
            "canBeDeleted": bool(local_raw.get("can_be_deleted")),
            "isDownloadingActive": bool(local_raw.get("is_downloading_active")),
            "isDownloadingCompleted": bool(local_raw.get("is_downloading_completed")),
            "downloadOffset": _int_or_default(local_raw.get("download_offset"), 0),
            "downloadedPrefixSize": _int_or_default(
                local_raw.get("downloaded_prefix_size"), 0
            ),
            "downloadedSize": _int_or_default(local_raw.get("downloaded_size"), 0),
        },
        "remote": {
            "id": _int_or_default(remote_raw.get("id"), 0),
            "uniqueId": str(remote_raw.get("unique_id") or ""),
            "isUploadingActive": bool(remote_raw.get("is_uploading_active")),
            "isUploadingCompleted": bool(remote_raw.get("is_uploading_completed")),
            "uploadedSize": _int_or_default(remote_raw.get("uploaded_size"), 0),
        },
    }


def _td_status_payload_from_td_file(
    file_payload: dict[str, Any],
    *,
    telegram_id: int,
    fallback_unique_id: str,
) -> dict[str, Any]:
    local_raw = (
        file_payload.get("local") if isinstance(file_payload.get("local"), dict) else {}
    )
    remote_raw = (
        file_payload.get("remote")
        if isinstance(file_payload.get("remote"), dict)
        else {}
    )
    downloaded_size = _int_or_default(local_raw.get("downloaded_size"), 0)
    is_completed = bool(local_raw.get("is_downloading_completed"))
    is_downloading = bool(local_raw.get("is_downloading_active"))
    status = (
        "completed"
        if is_completed
        else (
            "downloading"
            if is_downloading
            else ("paused" if downloaded_size > 0 else "idle")
        )
    )
    unique_id = str(remote_raw.get("unique_id") or "").strip() or fallback_unique_id
    completion_date = int(time.time() * 1000) if is_completed else 0
    local_path = str(local_raw.get("path") or "") if is_completed else ""

    return {
        "fileId": _int_or_default(file_payload.get("id"), 0),
        "uniqueId": unique_id,
        "downloadStatus": status,
        "localPath": local_path,
        "completionDate": completion_date,
        "downloadedSize": downloaded_size,
        "transferStatus": "idle",
        "telegramId": telegram_id,
    }


async def _emit_tdlib_download_aggregate(
    *,
    session_id: str,
    telegram_id: int,
) -> None:
    with STATE_LOCK:
        account_items = [
            value
            for (sid, tid, _), value in TDLIB_DOWNLOAD_PROGRESS.items()
            if sid == session_id and tid == telegram_id
        ]

    total_size = sum(
        _int_or_default(item.get("totalSize"), 0) for item in account_items
    )
    total_downloaded = sum(
        _int_or_default(item.get("downloadedSize"), 0) for item in account_items
    )
    total_count = sum(1 for item in account_items if bool(item.get("active")))

    await _emit_ws_payload(
        _build_ws_payload(
            EVENT_TYPE_FILE_DOWNLOAD,
            {
                "totalSize": total_size,
                "totalCount": total_count,
                "downloadedSize": total_downloaded,
            },
        ),
        session_id=session_id,
    )


async def _monitor_tdlib_download(
    app: FastAPI,
    *,
    session_id: str,
    telegram_id: int,
    file_id: int,
    unique_id: str,
) -> None:
    monitor_key = (session_id, telegram_id, file_id)
    account_key = str(telegram_id)
    seen_progress = False
    idle_rounds = 0
    last_status_signature: tuple[str, int, str] | None = None

    try:
        while True:
            td_manager = _tdlib_manager_from_app(app)
            if td_manager is None:
                break

            try:
                td_file = await asyncio.to_thread(
                    td_manager.request,
                    account_key,
                    {
                        "@type": "getFile",
                        "file_id": file_id,
                    },
                    15.0,
                )
            except Exception:
                await asyncio.sleep(0.8)
                continue

            if str(td_file.get("@type") or "") == "error":
                break

            ws_file = _td_file_to_ws(td_file)
            status_payload = _td_status_payload_from_td_file(
                td_file,
                telegram_id=telegram_id,
                fallback_unique_id=unique_id,
            )
            status = str(status_payload.get("downloadStatus") or "idle")
            downloaded_size = _int_or_default(status_payload.get("downloadedSize"), 0)
            now_ms = int(time.time() * 1000)
            total_size = max(
                _int_or_default(td_file.get("size"), 0),
                _int_or_default(td_file.get("expected_size"), 0),
            )
            remote_raw = (
                td_file.get("remote") if isinstance(td_file.get("remote"), dict) else {}
            )
            resolved_unique_id = (
                str(remote_raw.get("unique_id") or "").strip() or unique_id
            )
            _cache_tdlib_file_preview(
                telegram_id=telegram_id,
                unique_id=resolved_unique_id,
                file_id=file_id,
                local_path=str(status_payload.get("localPath") or "").strip() or None,
            )
            if unique_id.strip() and unique_id.strip() != resolved_unique_id:
                _cache_tdlib_file_preview(
                    telegram_id=telegram_id,
                    unique_id=unique_id,
                    file_id=file_id,
                    local_path=str(status_payload.get("localPath") or "").strip()
                    or None,
                )

            db: sqlite3.Connection = app.state.db
            _db_update_tdlib_file_status(
                db,
                telegram_id=telegram_id,
                file_id=file_id,
                unique_id=resolved_unique_id,
                status_payload=status_payload,
            )
            _update_speed_tracker(
                db,
                telegram_id=telegram_id,
                file_id=file_id,
                downloaded_size=downloaded_size,
                timestamp_ms=now_ms,
            )

            await _emit_ws_payload(
                _build_ws_payload(EVENT_TYPE_FILE_UPDATE, {"file": ws_file}),
                session_id=session_id,
            )

            status_signature = (
                status,
                downloaded_size,
                str(status_payload.get("localPath") or ""),
            )
            if status_signature != last_status_signature:
                await _emit_ws_payload(
                    _build_ws_payload(EVENT_TYPE_FILE_STATUS, status_payload),
                    session_id=session_id,
                )
                last_status_signature = status_signature

            with STATE_LOCK:
                TDLIB_DOWNLOAD_PROGRESS[monitor_key] = {
                    "totalSize": total_size,
                    "downloadedSize": downloaded_size,
                    "active": status == "downloading",
                }
            await _emit_tdlib_download_aggregate(
                session_id=session_id,
                telegram_id=telegram_id,
            )

            if status == "completed":
                break

            if status == "downloading" or downloaded_size > 0:
                seen_progress = True
                idle_rounds = 0
            else:
                idle_rounds += 1
                if seen_progress and idle_rounds >= 3:
                    break
                if not seen_progress and idle_rounds >= 8:
                    break

            await asyncio.sleep(0.8)
    finally:
        with STATE_LOCK:
            TDLIB_DOWNLOAD_TASKS.pop(monitor_key, None)
            TDLIB_DOWNLOAD_PROGRESS.pop(monitor_key, None)
        _clear_speed_tracker_file(telegram_id=telegram_id, file_id=file_id)

        await _emit_tdlib_download_aggregate(
            session_id=session_id,
            telegram_id=telegram_id,
        )


def _ensure_tdlib_download_monitor(
    app: FastAPI,
    *,
    session_id: str,
    telegram_id: int,
    file_id: int,
    unique_id: str,
) -> None:
    key = (session_id, telegram_id, file_id)
    with STATE_LOCK:
        existing = TDLIB_DOWNLOAD_TASKS.get(key)
        if existing is not None and not existing.done():
            return

        task = asyncio.create_task(
            _monitor_tdlib_download(
                app,
                session_id=session_id,
                telegram_id=telegram_id,
                file_id=file_id,
                unique_id=unique_id,
            )
        )
        TDLIB_DOWNLOAD_TASKS[key] = task


def _start_tdlib_download_for_message(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    chat_id: int,
    message_id: int,
    file_id: int,
) -> dict[str, Any]:
    if not _load_tdlib_session_for_account(td_manager, telegram_id, root_path):
        raise RuntimeError("TDLib is not ready yet. Please try again.")

    account_key = str(telegram_id)
    message_result = td_manager.request(
        account_key,
        {
            "@type": "getMessage",
            "chat_id": chat_id,
            "message_id": message_id,
        },
        timeout_seconds=25.0,
    )
    if str(message_result.get("@type") or "") == "error":
        raise RuntimeError(str(message_result.get("message") or "Message not found"))

    message_thread = td_manager.request(
        account_key,
        {
            "@type": "getMessageThread",
            "chat_id": chat_id,
            "message_id": message_id,
        },
        timeout_seconds=15.0,
    )
    if str(message_thread.get("@type") or "") == "error":
        message_thread = {}

    extracted = _extract_td_message_file(message_result)
    if extracted is None:
        raise RuntimeError("This message doesn't contain a downloadable file.")

    td_file = extracted["file"]
    target_file_id = _int_or_default(td_file.get("id"), 0)
    if target_file_id == 0:
        raise RuntimeError("Invalid TDLib file id")

    if file_id != 0 and target_file_id != file_id:
        target_file_id = file_id

    start_result = td_manager.request(
        account_key,
        {
            "@type": "addFileToDownloads",
            "file_id": target_file_id,
            "chat_id": chat_id,
            "message_id": message_id,
            "priority": 32,
        },
        timeout_seconds=25.0,
    )
    if str(start_result.get("@type") or "") == "error":
        fallback = td_manager.request(
            account_key,
            {
                "@type": "downloadFile",
                "file_id": target_file_id,
                "priority": 16,
                "offset": 0,
                "limit": 0,
                "synchronous": False,
            },
            timeout_seconds=25.0,
        )
        if str(fallback.get("@type") or "") == "error":
            raise RuntimeError(
                str(
                    fallback.get("message")
                    or start_result.get("message")
                    or "Failed to start download"
                )
            )

    download_result = td_manager.request(
        account_key,
        {
            "@type": "getFile",
            "file_id": target_file_id,
        },
        timeout_seconds=15.0,
    )
    if str(download_result.get("@type") or "") == "error":
        download_result = {"@type": "ok"}

    file_payload = _td_message_to_file(telegram_id, message_result)
    if file_payload is None:
        raise RuntimeError("Failed to map TDLib file payload")

    file_payload["threadChatId"] = _int_or_default(message_thread.get("chat_id"), 0)
    file_payload["messageThreadId"] = _int_or_default(
        message_thread.get("message_thread_id"),
        _int_or_default(file_payload.get("messageThreadId"), 0),
    )

    if str(download_result.get("@type") or "") == "file":
        local = (
            download_result.get("local")
            if isinstance(download_result.get("local"), dict)
            else {}
        )
        is_completed = bool(local.get("is_downloading_completed"))
        is_downloading = bool(local.get("is_downloading_active"))
        file_payload["downloadedSize"] = _int_or_default(
            local.get("downloaded_size"), file_payload.get("downloadedSize")
        )
        file_payload["downloadStatus"] = (
            "completed"
            if is_completed
            else ("downloading" if is_downloading else "downloading")
        )
        if is_completed:
            file_payload["localPath"] = str(local.get("path") or "")
            file_payload["completionDate"] = int(time.time() * 1000)
    else:
        file_payload["downloadStatus"] = "downloading"

    remote_raw = (
        td_file.get("remote") if isinstance(td_file.get("remote"), dict) else {}
    )
    remote_unique_id = str(remote_raw.get("unique_id") or "").strip()
    payload_unique_id = str(file_payload.get("uniqueId") or "").strip()
    mime_type = str(file_payload.get("mimeType") or "").strip()
    local_path = str(file_payload.get("localPath") or "").strip()

    if remote_unique_id:
        _cache_tdlib_file_preview(
            telegram_id=telegram_id,
            unique_id=remote_unique_id,
            file_id=target_file_id,
            mime_type=mime_type,
            local_path=local_path or None,
        )
    if payload_unique_id and payload_unique_id != remote_unique_id:
        _cache_tdlib_file_preview(
            telegram_id=telegram_id,
            unique_id=payload_unique_id,
            file_id=target_file_id,
            mime_type=mime_type,
            local_path=local_path or None,
        )

    return file_payload


def _int_or_default(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


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

    config: AppConfig = app.state.config
    db: sqlite3.Connection = app.state.db
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


def _method_error(message: str) -> JSONResponse:
    return JSONResponse(status_code=400, content={"error": message})


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "on"}:
        return True
    if text in {"false", "0", "no", "off"}:
        return False
    return None


def _file_status_from_file_record(file_record: dict[str, Any]) -> dict[str, Any]:
    return {
        "fileId": _int_or_default(file_record.get("id"), 0),
        "uniqueId": str(file_record.get("uniqueId") or ""),
        "downloadStatus": str(file_record.get("downloadStatus") or "idle"),
        "localPath": str(file_record.get("localPath") or ""),
        "completionDate": _int_or_default(file_record.get("completionDate"), 0),
        "downloadedSize": _int_or_default(file_record.get("downloadedSize"), 0),
        "transferStatus": str(file_record.get("transferStatus") or "idle"),
    }


def _parse_batch_files(payload: dict[str, Any]) -> list[dict[str, Any]]:
    files = payload.get("files")
    if not isinstance(files, list):
        raise HTTPException(status_code=400, detail="'files' must be an array.")

    normalized: list[dict[str, Any]] = []
    for item in files:
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "telegramId": _int_or_default(item.get("telegramId"), 0),
                "chatId": _int_or_default(item.get("chatId"), 0),
                "messageId": _int_or_default(item.get("messageId"), 0),
                "fileId": _int_or_default(item.get("fileId"), 0),
                "uniqueId": str(item.get("uniqueId") or "").strip(),
            }
        )
    return normalized


def get_db(request: Request) -> sqlite3.Connection:
    return request.app.state.db


def not_implemented() -> JSONResponse:
    return JSONResponse(
        status_code=501,
        content={
            "error": "This endpoint is not implemented in the Python backend yet."
        },
    )


def _get_filters(request: Request) -> dict[str, str]:
    filters = dict(request.query_params)
    search = filters.get("search")
    if search:
        filters["search"] = unquote(search)
    link = filters.get("link")
    if link:
        filters["link"] = _decode_link_value(link)
    return filters


def _to_telegram_id(telegram_id: str) -> int:
    try:
        return int(telegram_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=404, detail="Telegram account not found."
        ) from exc


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
    with STATE_LOCK:
        WS_CONNECTIONS.setdefault(session_id, set()).add(websocket)
        if telegram_id:
            SESSION_TELEGRAM_SELECTION[session_id] = telegram_id
        selected_id = SESSION_TELEGRAM_SELECTION.get(session_id)
        pending = PENDING_TELEGRAMS.get(selected_id) if selected_id else None

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
        with STATE_LOCK:
            session_connections = WS_CONNECTIONS.get(session_id)
            if session_connections is not None:
                session_connections.discard(websocket)
                if not session_connections:
                    del WS_CONNECTIONS[session_id]


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
    with STATE_LOCK:
        if not telegram_id:
            SESSION_TELEGRAM_SELECTION.pop(session_id, None)
        else:
            SESSION_TELEGRAM_SELECTION[session_id] = telegram_id
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
    if offline_requested or _int_or_default(db_result.get("size"), 0) > 0:
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
