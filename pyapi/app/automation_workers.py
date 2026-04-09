from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable

from fastapi import FastAPI

from .db import (
    get_automation_map,
    get_settings_by_keys,
    list_chat_group_automations,
    update_auto_settings,
    update_chat_group_auto_settings,
)
from .file_record_ops import (
    count_downloading_files as _db_count_downloading_files,
    file_for_transfer as _db_file_for_transfer,
    find_file_by_unique as _db_find_file_by_unique,
    transfer_candidates as _db_transfer_candidates,
    update_transfer_status as _db_update_transfer_status,
    upsert_tdlib_file_record as _db_upsert_tdlib_file_record,
)
from .filter_expr import evaluate_filter_expr as _evaluate_filter_expr
from .tdlib import TdlibAuthManager
from .tdlib_downloads import (
    start_tdlib_download_for_message as _start_tdlib_download_for_message,
)
from .tdlib_file_mapper import td_message_to_file as _td_message_to_file
from .tdlib_queries import (
    load_tdlib_session_for_account as _load_tdlib_session_for_account,
)
from .transfer_ops import execute_transfer as _execute_transfer


logger = logging.getLogger(__name__)

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

AUTO_DOWNLOAD_WAITING: dict[int, deque[dict[str, int]]] = {}
AUTO_DOWNLOAD_WAITING_KEYS: set[tuple[int, int, int]] = set()
AUTO_DOWNLOAD_COMMENT_THREADS: dict[tuple[int, int, int], dict[str, Any]] = {}
TRANSFER_WAITING: deque[dict[str, Any]] = deque()
TRANSFER_WAITING_KEYS: set[tuple[int, int, str]] = set()


@dataclass(frozen=True)
class WorkerDeps:
    tdlib_account_root_path: Callable[
        [FastAPI, sqlite3.Connection, int, dict[int, str | None] | None],
        str | None,
    ]
    emit_file_status: Callable[[dict[str, Any]], Awaitable[None]]
    td_file_status_payload: Callable[[dict[str, Any]], dict[str, Any]]
    ensure_tdlib_download_monitor: Callable[[FastAPI, str, int, int, str], None]
    avg_speed_interval: Callable[[sqlite3.Connection], int]
    persist_speed_statistics: Callable[[sqlite3.Connection], None]


def reset_worker_state() -> None:
    AUTO_DOWNLOAD_WAITING.clear()
    AUTO_DOWNLOAD_WAITING_KEYS.clear()
    AUTO_DOWNLOAD_COMMENT_THREADS.clear()
    TRANSFER_WAITING.clear()
    TRANSFER_WAITING_KEYS.clear()


def queue_transfer_candidate(candidate: dict[str, Any]) -> None:
    telegram_id = _int_or_default(candidate.get("telegramId"), 0)
    file_id = _int_or_default(candidate.get("fileId") or candidate.get("id"), 0)
    unique_id = str(candidate.get("uniqueId") or "").strip()
    if telegram_id <= 0 or not unique_id:
        return

    key = (telegram_id, file_id, unique_id)
    if key in TRANSFER_WAITING_KEYS:
        return

    TRANSFER_WAITING_KEYS.add(key)
    TRANSFER_WAITING.append(
        {
            "telegramId": telegram_id,
            "chatId": _int_or_default(candidate.get("chatId"), 0),
            "uniqueId": unique_id,
            "fileId": file_id,
        }
    )


def _int_or_default(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _state_is_enabled(state: int, bit: int) -> bool:
    return (state & (1 << bit)) != 0


def _state_enable(state: int, bit: int) -> int:
    return state | (1 << bit)


def _tdlib_manager_from_app(app: FastAPI) -> TdlibAuthManager | None:
    manager = getattr(app.state, "tdlib_manager", None)
    if isinstance(manager, TdlibAuthManager):
        return manager
    return None


def _auto_download_limit(db: sqlite3.Connection) -> int:
    raw = get_settings_by_keys(db, ["autoDownloadLimit"]).get("autoDownloadLimit")
    parsed = _int_or_default(raw, AUTO_DOWNLOAD_DEFAULT_LIMIT)
    if parsed <= 0:
        return AUTO_DOWNLOAD_DEFAULT_LIMIT
    return parsed


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


def _persist_group_automation(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    group_id: str,
    automation: dict[str, Any],
) -> None:
    update_chat_group_auto_settings(
        db,
        telegram_id=telegram_id,
        group_id=group_id,
        auto_payload=deepcopy(automation),
    )


def _group_member_progress(
    automation: dict[str, Any],
    *,
    chat_id: int,
) -> dict[str, Any]:
    progress_map = automation.setdefault("progressByChat", {})
    if not isinstance(progress_map, dict):
        progress_map = {}
        automation["progressByChat"] = progress_map

    progress = progress_map.get(str(chat_id))
    if not isinstance(progress, dict):
        progress = {
            "state": 0,
            "preload": {"nextFromMessageId": 0},
            "download": {"nextFileType": "", "nextFromMessageId": 0},
        }
        progress_map[str(chat_id)] = progress

    progress.setdefault("preload", {})
    progress.setdefault("download", {})
    progress["state"] = _int_or_default(progress.get("state"), 0)
    return progress


def _group_chat_ids(group: dict[str, Any]) -> list[int]:
    raw_chat_ids = group.get("chatIds") if isinstance(group, dict) else None
    if not isinstance(raw_chat_ids, list):
        return []

    result: list[int] = []
    seen: set[int] = set()
    for item in raw_chat_ids:
        chat_id = _int_or_default(item, 0)
        if chat_id == 0 or chat_id in seen:
            continue
        seen.add(chat_id)
        result.append(chat_id)
    return result


def _resolve_effective_automation_for_chat(
    *,
    telegram_id: int,
    chat_id: int,
    direct_automations: dict[tuple[int, int], dict[str, Any]],
    group_automations: list[dict[str, Any]],
) -> dict[str, Any] | None:
    direct = direct_automations.get((telegram_id, chat_id))
    if isinstance(direct, dict):
        return direct

    for group in group_automations:
        if _int_or_default(group.get("telegramId"), telegram_id) != telegram_id:
            continue
        if chat_id not in _group_chat_ids(group):
            continue
        auto = group.get("auto")
        if isinstance(auto, dict):
            return auto
    return None


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


def _pop_transfer_candidate() -> dict[str, Any] | None:
    if not TRANSFER_WAITING:
        return None

    candidate = TRANSFER_WAITING.popleft()
    TRANSFER_WAITING_KEYS.discard(
        (
            _int_or_default(candidate.get("telegramId"), 0),
            _int_or_default(candidate.get("fileId"), 0),
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


async def _run_preload_scan_cycle(app: FastAPI, deps: WorkerDeps) -> None:
    td_manager = _tdlib_manager_from_app(app)
    if td_manager is None:
        return

    db: sqlite3.Connection = app.state.db
    automations = get_automation_map(db)
    group_automations = list_chat_group_automations(db)
    if not automations and not group_automations:
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

        root_path = deps.tdlib_account_root_path(app, db, telegram_id, root_path_cache)
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

    for group in group_automations:
        telegram_id = _int_or_default(group.get("telegramId"), 0)
        group_id = str(group.get("groupId") or "").strip()
        automation = group.get("auto") if isinstance(group.get("auto"), dict) else None
        if telegram_id <= 0 or not group_id or not isinstance(automation, dict):
            continue

        preload_cfg = (
            automation.get("preload") if isinstance(automation, dict) else None
        )
        if not isinstance(preload_cfg, dict) or not bool(preload_cfg.get("enabled")):
            continue

        root_path = deps.tdlib_account_root_path(app, db, telegram_id, root_path_cache)
        if root_path is None:
            continue

        for chat_id in _group_chat_ids(group):
            if (telegram_id, chat_id) in automations:
                continue

            progress = _group_member_progress(automation, chat_id=chat_id)
            state = _int_or_default(progress.get("state"), 0)
            if _state_is_enabled(state, HISTORY_PRELOAD_STATE):
                continue

            cursor = _int_or_default(
                progress.get("preload", {}).get("nextFromMessageId"), 0
            )
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
                    "Preload scan failed for telegram=%s group=%s chat=%s: %s",
                    telegram_id,
                    group_id,
                    chat_id,
                    exc,
                )
                continue

            if not messages:
                progress["state"] = _state_enable(state, HISTORY_PRELOAD_STATE)
                _persist_group_automation(
                    db,
                    telegram_id=telegram_id,
                    group_id=group_id,
                    automation=automation,
                )
                continue

            for message in messages:
                file_payload = _td_message_to_file(telegram_id, message)
                if file_payload is None:
                    continue
                _db_upsert_tdlib_file_record(db, file_payload=file_payload)

            progress.setdefault("preload", {})["nextFromMessageId"] = _int_or_default(
                messages[-1].get("id"),
                cursor,
            )
            _persist_group_automation(
                db,
                telegram_id=telegram_id,
                group_id=group_id,
                automation=automation,
            )


async def _run_auto_download_scan_cycle(app: FastAPI, deps: WorkerDeps) -> None:
    db: sqlite3.Connection = app.state.db
    if not _is_download_time(db):
        return

    td_manager = _tdlib_manager_from_app(app)
    if td_manager is None:
        return

    automations = get_automation_map(db)
    group_automations = list_chat_group_automations(db)
    if not automations and not group_automations:
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

        root_path = deps.tdlib_account_root_path(app, db, telegram_id, root_path_cache)
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

    for group in group_automations:
        telegram_id = _int_or_default(group.get("telegramId"), 0)
        group_id = str(group.get("groupId") or "").strip()
        automation = group.get("auto") if isinstance(group.get("auto"), dict) else None
        if telegram_id <= 0 or not group_id or not isinstance(automation, dict):
            continue

        download_cfg = (
            automation.get("download") if isinstance(automation, dict) else None
        )
        if not isinstance(download_cfg, dict) or not bool(download_cfg.get("enabled")):
            continue

        rule = download_cfg.get("rule")
        if not isinstance(rule, dict) or not bool(rule.get("downloadHistory", True)):
            continue

        root_path = deps.tdlib_account_root_path(app, db, telegram_id, root_path_cache)
        if root_path is None:
            continue

        for chat_id in _group_chat_ids(group):
            if (telegram_id, chat_id) in automations:
                continue

            progress = _group_member_progress(automation, chat_id=chat_id)
            state = _int_or_default(progress.get("state"), 0)
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
                    progress["state"] = _state_enable(state, HISTORY_DOWNLOAD_STATE)
                    _persist_group_automation(
                        db,
                        telegram_id=telegram_id,
                        group_id=group_id,
                        automation=automation,
                    )
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
                    next_file_type=str(
                        progress.get("download", {}).get("nextFileType") or ""
                    ),
                    next_from_message_id=_int_or_default(
                        progress.get("download", {}).get("nextFromMessageId"),
                        0,
                    ),
                    message_thread_id=0,
                )

                progress.setdefault("download", {})["nextFileType"] = next_file_type
                progress.setdefault("download", {})["nextFromMessageId"] = next_cursor

                next_state = state
                if is_complete:
                    next_state = _state_enable(next_state, HISTORY_DOWNLOAD_SCAN_STATE)
                if is_complete and _auto_waiting_size(telegram_id) == 0:
                    next_state = _state_enable(next_state, HISTORY_DOWNLOAD_STATE)
                progress["state"] = next_state
                _persist_group_automation(
                    db,
                    telegram_id=telegram_id,
                    group_id=group_id,
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
                message_thread_id = _int_or_default(
                    comment_scan.get("messageThreadId"), 0
                )
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
                        comment_scan.get("nextFromMessageId"),
                        0,
                    ),
                    message_thread_id=message_thread_id,
                )

                if comment_complete:
                    AUTO_DOWNLOAD_COMMENT_THREADS.pop(key, None)
                else:
                    comment_scan["nextFileType"] = next_file_type
                    comment_scan["nextFromMessageId"] = next_cursor


async def _run_auto_download_tick(app: FastAPI, deps: WorkerDeps) -> None:
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
        group_automations = list_chat_group_automations(db, telegram_id=telegram_id)
        root_path = deps.tdlib_account_root_path(app, db, telegram_id, root_path_cache)
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
            await deps.emit_file_status(deps.td_file_status_payload(file_record))

            candidate_chat_id = _int_or_default(candidate.get("chatId"), 0)
            automation = _resolve_effective_automation_for_chat(
                telegram_id=telegram_id,
                chat_id=candidate_chat_id,
                direct_automations=automation_map,
                group_automations=group_automations,
            )
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
                deps.ensure_tdlib_download_monitor(
                    app,
                    f"worker:{telegram_id}",
                    telegram_id,
                    monitor_file_id,
                    str(file_record.get("uniqueId") or ""),
                )

            surplus -= 1


async def _run_transfer_scan_cycle(app: FastAPI) -> None:
    db: sqlite3.Connection = app.state.db
    automations = get_automation_map(db)
    group_automations = list_chat_group_automations(db)
    if not automations and not group_automations:
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
            queue_transfer_candidate(item)

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

    for group in group_automations:
        telegram_id = _int_or_default(group.get("telegramId"), 0)
        group_id = str(group.get("groupId") or "").strip()
        automation = group.get("auto") if isinstance(group.get("auto"), dict) else None
        if telegram_id <= 0 or not group_id or not isinstance(automation, dict):
            continue

        transfer_cfg = (
            automation.get("transfer") if isinstance(automation, dict) else None
        )
        if not isinstance(transfer_cfg, dict) or not bool(transfer_cfg.get("enabled")):
            continue

        rule = transfer_cfg.get("rule")
        if not isinstance(rule, dict):
            continue

        for chat_id in _group_chat_ids(group):
            if (telegram_id, chat_id) in automations:
                continue

            candidates = _db_transfer_candidates(
                db,
                telegram_id=telegram_id,
                chat_id=chat_id,
                limit=200,
            )
            for item in candidates:
                queue_transfer_candidate(item)

            transfer_history = bool(rule.get("transferHistory", True))
            progress = _group_member_progress(automation, chat_id=chat_id)
            state = _int_or_default(progress.get("state"), 0)
            if (
                transfer_history
                and not candidates
                and not _state_is_enabled(state, HISTORY_TRANSFER_STATE)
            ):
                progress["state"] = _state_enable(state, HISTORY_TRANSFER_STATE)
                _persist_group_automation(
                    db,
                    telegram_id=telegram_id,
                    group_id=group_id,
                    automation=automation,
                )


async def _run_transfer_tick(deps: WorkerDeps, app: FastAPI) -> None:
    candidate = _pop_transfer_candidate()
    if candidate is None:
        return

    db: sqlite3.Connection = app.state.db
    telegram_id = _int_or_default(candidate.get("telegramId"), 0)
    chat_id = _int_or_default(candidate.get("chatId"), 0)
    file_id = _int_or_default(candidate.get("fileId"), 0)
    unique_id = str(candidate.get("uniqueId") or "")
    if telegram_id <= 0 or chat_id == 0 or not unique_id:
        return

    automations = get_automation_map(db, telegram_id=telegram_id)
    group_automations = list_chat_group_automations(db, telegram_id=telegram_id)
    automation = _resolve_effective_automation_for_chat(
        telegram_id=telegram_id,
        chat_id=chat_id,
        direct_automations=automations,
        group_automations=group_automations,
    )
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
        file_id=file_id,
        unique_id=unique_id,
    )
    if row is None:
        return

    in_progress_payload = _db_update_transfer_status(
        db,
        telegram_id=telegram_id,
        file_id=file_id,
        unique_id=unique_id,
        transfer_status="transferring",
    )
    if in_progress_payload is not None:
        await deps.emit_file_status(in_progress_payload)

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
        file_id=file_id,
        unique_id=unique_id,
        transfer_status=transfer_status,
        local_path=resolved_path,
    )
    if final_payload is not None:
        await deps.emit_file_status(final_payload)


async def background_workers_loop(app: FastAPI, deps: WorkerDeps) -> None:
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
                await _run_preload_scan_cycle(app, deps)

            if now - last_auto_scan >= AUTO_DOWNLOAD_SCAN_INTERVAL_SECONDS:
                last_auto_scan = now
                await _run_auto_download_scan_cycle(app, deps)

            if now - last_auto_tick >= AUTO_DOWNLOAD_TICK_INTERVAL_SECONDS:
                last_auto_tick = now
                await _run_auto_download_tick(app, deps)

            if now - last_transfer_scan >= TRANSFER_SCAN_INTERVAL_SECONDS:
                last_transfer_scan = now
                await _run_transfer_scan_cycle(app)

            if now - last_transfer_tick >= TRANSFER_TICK_INTERVAL_SECONDS:
                last_transfer_tick = now
                await _run_transfer_tick(deps, app)

            db: sqlite3.Connection = app.state.db
            speed_interval_seconds = float(deps.avg_speed_interval(db))
            if now - last_speed_persist >= speed_interval_seconds:
                last_speed_persist = now
                deps.persist_speed_statistics(db)
        except Exception as exc:
            logger.exception("Background worker loop error: %s", exc)

        await asyncio.sleep(1.0)
