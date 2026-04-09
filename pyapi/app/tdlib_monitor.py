from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, Awaitable, Callable

from fastapi import FastAPI

from .tdlib import TdlibAuthManager
from .tdlib_downloads import (
    cache_tdlib_file_preview as _cache_tdlib_file_preview,
    td_file_to_ws as _td_file_to_ws,
    td_status_payload_from_td_file as _td_status_payload_from_td_file,
)


logger = logging.getLogger(__name__)

_STATE_LOCK = Lock()
_TDLIB_DOWNLOAD_TASKS: dict[tuple[str, int, int], asyncio.Task[Any]] = {}
_TDLIB_DOWNLOAD_PROGRESS: dict[tuple[str, int, int], dict[str, Any]] = {}
MONITOR_POLL_INTERVAL_SECONDS = 0.8
MONITOR_IDLE_AFTER_PROGRESS_ROUNDS = 3
MONITOR_PENDING_IDLE_TIMEOUT_SECONDS = 15 * 60


@dataclass(frozen=True)
class TdlibMonitorDeps:
    emit_file_update: Callable[[str, dict[str, Any]], Awaitable[None]]
    emit_file_status: Callable[[str, dict[str, Any]], Awaitable[None]]
    emit_download_aggregate: Callable[[str, dict[str, Any]], Awaitable[None]]
    update_tdlib_file_status: Callable[
        [sqlite3.Connection, int, int, str, dict[str, Any]],
        None,
    ]
    update_speed_tracker: Callable[[sqlite3.Connection, int, int, int, int], None]
    clear_speed_tracker_file: Callable[[int, int], None]


def reset_tdlib_monitor_state() -> None:
    with _STATE_LOCK:
        tasks = list(_TDLIB_DOWNLOAD_TASKS.values())
        _TDLIB_DOWNLOAD_TASKS.clear()
        _TDLIB_DOWNLOAD_PROGRESS.clear()

    for task in tasks:
        if not task.done():
            task.cancel()


def stop_tdlib_download_monitor(
    *,
    session_id: str,
    telegram_id: int,
    file_id: int,
) -> None:
    if file_id <= 0:
        return

    key = (session_id, telegram_id, file_id)
    with _STATE_LOCK:
        task = _TDLIB_DOWNLOAD_TASKS.pop(key, None)
        _TDLIB_DOWNLOAD_PROGRESS.pop(key, None)

    if task is not None and not task.done():
        task.cancel()


async def emit_tdlib_download_aggregate(
    *,
    session_id: str,
    telegram_id: int,
    deps: TdlibMonitorDeps,
) -> None:
    with _STATE_LOCK:
        account_items = [
            value
            for (sid, tid, _), value in _TDLIB_DOWNLOAD_PROGRESS.items()
            if sid == session_id and tid == telegram_id
        ]

    total_size = sum(
        _int_or_default(item.get("totalSize"), 0) for item in account_items
    )
    total_downloaded = sum(
        _int_or_default(item.get("downloadedSize"), 0) for item in account_items
    )
    total_count = sum(1 for item in account_items if bool(item.get("active")))

    await deps.emit_download_aggregate(
        session_id,
        {
            "totalSize": total_size,
            "totalCount": total_count,
            "downloadedSize": total_downloaded,
        },
    )


def ensure_tdlib_download_monitor(
    app: FastAPI,
    *,
    session_id: str,
    telegram_id: int,
    file_id: int,
    unique_id: str,
    deps: TdlibMonitorDeps,
) -> None:
    key = (session_id, telegram_id, file_id)
    with _STATE_LOCK:
        existing = _TDLIB_DOWNLOAD_TASKS.get(key)
        if existing is not None and not existing.done():
            return

        task = asyncio.create_task(
            _monitor_tdlib_download(
                app,
                session_id=session_id,
                telegram_id=telegram_id,
                file_id=file_id,
                unique_id=unique_id,
                deps=deps,
            )
        )
        _TDLIB_DOWNLOAD_TASKS[key] = task


def _int_or_default(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _tdlib_manager_from_app(app: FastAPI) -> TdlibAuthManager | None:
    manager = getattr(app.state, "tdlib_manager", None)
    if isinstance(manager, TdlibAuthManager):
        return manager
    return None


async def _monitor_tdlib_download(
    app: FastAPI,
    *,
    session_id: str,
    telegram_id: int,
    file_id: int,
    unique_id: str,
    deps: TdlibMonitorDeps,
) -> None:
    monitor_key = (session_id, telegram_id, file_id)
    account_key = str(telegram_id)
    seen_progress = False
    idle_rounds = 0
    last_status_signature: tuple[str, int, str] | None = None
    pending_started_at = time.monotonic()

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
                await asyncio.sleep(MONITOR_POLL_INTERVAL_SECONDS)
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
            deps.update_tdlib_file_status(
                db,
                telegram_id,
                file_id,
                resolved_unique_id,
                status_payload,
            )
            deps.update_speed_tracker(
                db,
                telegram_id,
                file_id,
                downloaded_size,
                now_ms,
            )

            await deps.emit_file_update(session_id, {"file": ws_file})

            status_signature = (
                status,
                downloaded_size,
                str(status_payload.get("localPath") or ""),
            )
            if status_signature != last_status_signature:
                await deps.emit_file_status(session_id, status_payload)
                last_status_signature = status_signature

            with _STATE_LOCK:
                _TDLIB_DOWNLOAD_PROGRESS[monitor_key] = {
                    "totalSize": total_size,
                    "downloadedSize": downloaded_size,
                    "active": status == "downloading",
                }
            await emit_tdlib_download_aggregate(
                session_id=session_id,
                telegram_id=telegram_id,
                deps=deps,
            )

            if status == "completed":
                break

            if status == "downloading" or downloaded_size > 0:
                seen_progress = True
                idle_rounds = 0
            else:
                idle_rounds += 1
                if seen_progress and idle_rounds >= MONITOR_IDLE_AFTER_PROGRESS_ROUNDS:
                    break
                if (
                    not seen_progress
                    and time.monotonic() - pending_started_at
                    >= MONITOR_PENDING_IDLE_TIMEOUT_SECONDS
                ):
                    break

            await asyncio.sleep(MONITOR_POLL_INTERVAL_SECONDS)
    finally:
        with _STATE_LOCK:
            _TDLIB_DOWNLOAD_TASKS.pop(monitor_key, None)
            _TDLIB_DOWNLOAD_PROGRESS.pop(monitor_key, None)
        deps.clear_speed_tracker_file(telegram_id, file_id)

        await emit_tdlib_download_aggregate(
            session_id=session_id,
            telegram_id=telegram_id,
            deps=deps,
        )
