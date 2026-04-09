from __future__ import annotations

import json
import logging
import sqlite3
import time
from collections import deque
from copy import deepcopy
from threading import Lock
from typing import Any

from fastapi import FastAPI

from .app_state import (
    EVENT_TYPE_FILE_DOWNLOAD,
    EVENT_TYPE_FILE_STATUS,
    EVENT_TYPE_FILE_UPDATE,
    _build_ws_payload,
    _emit_ws_payload,
)
from .automation_workers import queue_transfer_candidate as _queue_transfer_candidate
from .db import (
    find_chat_group_for_chat,
    get_automation_map,
    get_settings_by_keys,
    get_telegram_account,
)
from .file_record_ops import (
    file_for_transfer as _db_file_for_transfer,
    update_tdlib_file_status as _update_tdlib_file_status,
)
from .route_utils import _int_or_default
from .tdlib_monitor import (
    TdlibMonitorDeps,
    emit_tdlib_download_aggregate as _emit_tdlib_download_aggregate_impl,
    ensure_tdlib_download_monitor as _ensure_tdlib_download_monitor_impl,
    stop_tdlib_download_monitor as _stop_tdlib_download_monitor_impl,
)
from .tdlib_queries import default_chat_auto as _default_chat_auto


SPEED_INTERVAL_CACHE_TTL_SECONDS = 5.0

logger = logging.getLogger(__name__)

SPEED_LOCK = Lock()
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


def reset_speed_state() -> None:
    global SPEED_INTERVAL_CACHE_VALUE, SPEED_INTERVAL_CACHE_AT
    with SPEED_LOCK:
        SPEED_TRACKERS.clear()
        SPEED_TOTAL_DOWNLOADED.clear()
        SPEED_LAST_FILE_DOWNLOADED.clear()
        SPEED_INTERVAL_CACHE_VALUE = 5 * 60
        SPEED_INTERVAL_CACHE_AT = 0.0


def _avg_speed_interval(db: sqlite3.Connection) -> int:
    global SPEED_INTERVAL_CACHE_VALUE, SPEED_INTERVAL_CACHE_AT

    now = time.monotonic()
    with SPEED_LOCK:
        cached_value = SPEED_INTERVAL_CACHE_VALUE
        cached_at = SPEED_INTERVAL_CACHE_AT

    if now - cached_at <= SPEED_INTERVAL_CACHE_TTL_SECONDS:
        return cached_value

    raw = get_settings_by_keys(db, ["avgSpeedInterval"]).get("avgSpeedInterval")
    parsed = _int_or_default(raw, 5 * 60)
    if parsed <= 0:
        parsed = 5 * 60

    with SPEED_LOCK:
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

    with SPEED_LOCK:
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

    with SPEED_LOCK:
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
    with SPEED_LOCK:
        SPEED_LAST_FILE_DOWNLOADED.pop((telegram_id, file_id), None)


def _persist_speed_statistics(db: sqlite3.Connection) -> None:
    interval = _avg_speed_interval(db)
    now_ms = int(time.time() * 1000)

    with SPEED_LOCK:
        items = list(SPEED_TRACKERS.items())

    rows_to_insert: list[tuple[str, str, int, str]] = []
    for telegram_id, tracker in items:
        with SPEED_LOCK:
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


def _db_update_tdlib_file_status(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int,
    unique_id: str,
    status_payload: dict[str, Any],
) -> None:
    _update_tdlib_file_status(
        db,
        telegram_id=telegram_id,
        file_id=file_id,
        unique_id=unique_id,
        status_payload=status_payload,
        on_completed=lambda db_conn, completed_telegram_id, completed_file_id, completed_unique_id: (
            _queue_transfer_for_completed_file(
                db_conn,
                telegram_id=completed_telegram_id,
                file_id=completed_file_id,
                unique_id=completed_unique_id,
            )
        ),
    )


def _queue_transfer_for_completed_file(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int,
    unique_id: str,
) -> None:
    if telegram_id <= 0 or not unique_id:
        return

    row = _db_file_for_transfer(
        db,
        telegram_id=telegram_id,
        file_id=file_id,
        unique_id=unique_id,
    )
    if row is None:
        return

    if str(row["download_status"] or "").strip().lower() != "completed":
        return
    if str(row["transfer_status"] or "idle").strip().lower() != "idle":
        return
    if not str(row["local_path"] or "").strip():
        return

    chat_id = _int_or_default(row["chat_id"], 0)
    if chat_id == 0:
        return

    automations = get_automation_map(db, telegram_id=telegram_id)
    automation = automations.get((telegram_id, chat_id))
    if not isinstance(automation, dict):
        group = find_chat_group_for_chat(
            db,
            telegram_id=telegram_id,
            chat_id=chat_id,
        )
        automation = group.get("auto") if isinstance(group, dict) else None
    if not isinstance(automation, dict):
        return

    transfer_cfg = automation.get("transfer")
    if not isinstance(transfer_cfg, dict) or not bool(transfer_cfg.get("enabled")):
        return

    rule = transfer_cfg.get("rule")
    if not isinstance(rule, dict) or not str(rule.get("destination") or "").strip():
        return

    _queue_transfer_candidate(
        {
            "fileId": _int_or_default(row["id"], 0),
            "uniqueId": unique_id,
            "telegramId": telegram_id,
            "chatId": chat_id,
        }
    )


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

    account = get_telegram_account(
        db,
        telegram_id=telegram_id,
        app_root=str(app.state.config.app_root),
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


def _tdlib_monitor_deps() -> TdlibMonitorDeps:
    async def _emit_file_update(
        session_id: str,
        payload: dict[str, Any],
    ) -> None:
        await _emit_ws_payload(
            _build_ws_payload(EVENT_TYPE_FILE_UPDATE, payload),
            session_id=session_id,
        )

    async def _emit_file_status(
        session_id: str,
        payload: dict[str, Any],
    ) -> None:
        await _emit_ws_payload(
            _build_ws_payload(EVENT_TYPE_FILE_STATUS, payload),
            session_id=session_id,
        )

    async def _emit_download_aggregate(
        session_id: str,
        payload: dict[str, Any],
    ) -> None:
        await _emit_ws_payload(
            _build_ws_payload(EVENT_TYPE_FILE_DOWNLOAD, payload),
            session_id=session_id,
        )

    return TdlibMonitorDeps(
        emit_file_update=_emit_file_update,
        emit_file_status=_emit_file_status,
        emit_download_aggregate=_emit_download_aggregate,
        update_tdlib_file_status=lambda db, telegram_id, file_id, unique_id, status_payload: (
            _db_update_tdlib_file_status(
                db,
                telegram_id=telegram_id,
                file_id=file_id,
                unique_id=unique_id,
                status_payload=status_payload,
            )
        ),
        update_speed_tracker=lambda db, telegram_id, file_id, downloaded_size, timestamp_ms: (
            _update_speed_tracker(
                db,
                telegram_id=telegram_id,
                file_id=file_id,
                downloaded_size=downloaded_size,
                timestamp_ms=timestamp_ms,
            )
        ),
        clear_speed_tracker_file=lambda telegram_id, file_id: _clear_speed_tracker_file(
            telegram_id=telegram_id,
            file_id=file_id,
        ),
    )


def _stop_tdlib_download_monitor(
    *,
    session_id: str,
    telegram_id: int,
    file_id: int,
) -> None:
    _stop_tdlib_download_monitor_impl(
        session_id=session_id,
        telegram_id=telegram_id,
        file_id=file_id,
    )


async def _emit_tdlib_download_aggregate(
    *,
    session_id: str,
    telegram_id: int,
) -> None:
    await _emit_tdlib_download_aggregate_impl(
        session_id=session_id,
        telegram_id=telegram_id,
        deps=_tdlib_monitor_deps(),
    )


def _ensure_tdlib_download_monitor(
    app: FastAPI,
    *,
    session_id: str,
    telegram_id: int,
    file_id: int,
    unique_id: str,
) -> None:
    _ensure_tdlib_download_monitor_impl(
        app,
        session_id=session_id,
        telegram_id=telegram_id,
        file_id=file_id,
        unique_id=unique_id,
        deps=_tdlib_monitor_deps(),
    )
