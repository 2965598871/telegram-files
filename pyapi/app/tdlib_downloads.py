from __future__ import annotations

import logging
import mimetypes
import sqlite3
import time
from pathlib import Path
from threading import Lock
from typing import Any, Callable

from .file_record_ops import find_file_by_unique as _find_file_by_unique
from .tdlib import TdlibAuthManager
from .tdlib_file_mapper import (
    extract_td_message_file as _extract_td_message_file,
    td_message_to_file as _td_message_to_file,
)
from .tdlib_queries import (
    load_tdlib_session_for_account as _load_tdlib_session_for_account,
)

logger = logging.getLogger(__name__)

_PREVIEW_CACHE_LOCK = Lock()
_TDLIB_FILE_PREVIEW_CACHE: dict[tuple[int, str], dict[str, Any]] = {}


def _int_or_default(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def cache_tdlib_file_preview(
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
    with _PREVIEW_CACHE_LOCK:
        current = dict(_TDLIB_FILE_PREVIEW_CACHE.get(key) or {})
        if file_id is not None and file_id > 0:
            current["fileId"] = file_id
        if mime_type:
            current["mimeType"] = str(mime_type)
        if local_path:
            current["path"] = str(local_path)
        current["updatedAt"] = int(time.time() * 1000)
        _TDLIB_FILE_PREVIEW_CACHE[key] = current


def _evict_tdlib_file_preview(
    *,
    telegram_id: int,
    unique_id: str | None = None,
    file_id: int | None = None,
) -> None:
    normalized_unique = str(unique_id or "").strip()
    normalized_file_id = int(file_id or 0)

    with _PREVIEW_CACHE_LOCK:
        to_delete: list[tuple[int, str]] = []
        for key, value in _TDLIB_FILE_PREVIEW_CACHE.items():
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
            _TDLIB_FILE_PREVIEW_CACHE.pop(key, None)


def cached_tdlib_file_preview(
    *,
    telegram_id: int,
    unique_id: str,
) -> dict[str, Any] | None:
    key = (telegram_id, unique_id.strip())
    with _PREVIEW_CACHE_LOCK:
        entry = _TDLIB_FILE_PREVIEW_CACHE.get(key)
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

    with _PREVIEW_CACHE_LOCK:
        for (
            cache_telegram_id,
            cache_unique_id,
        ), payload in _TDLIB_FILE_PREVIEW_CACHE.items():
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
        cached = cached_tdlib_file_preview(
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


def media_type_for_path(path: str, hint: str | None) -> str:
    normalized_hint = str(hint or "").strip()
    if normalized_hint:
        return normalized_hint
    guessed, _ = mimetypes.guess_type(path)
    return str(guessed or "application/octet-stream")


def resolve_tdlib_preview_info(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    unique_id: str,
) -> dict[str, Any] | None:
    cached = cached_tdlib_file_preview(telegram_id=telegram_id, unique_id=unique_id)
    if cached is None:
        return None

    path_value = str(cached.get("path") or "").strip()
    if path_value:
        path_obj = Path(path_value)
        if path_obj.exists() and path_obj.is_file():
            return {
                "path": str(path_obj),
                "mimeType": media_type_for_path(
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

    cache_tdlib_file_preview(
        telegram_id=telegram_id,
        unique_id=unique_id,
        file_id=file_id,
        mime_type=str(cached.get("mimeType") or ""),
        local_path=str(resolved_obj),
    )

    return {
        "path": str(resolved_obj),
        "mimeType": media_type_for_path(
            str(resolved_obj),
            str(cached.get("mimeType") or ""),
        ),
    }


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


def tdlib_cancel_download_fallback(
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
    cache_tdlib_file_preview(
        telegram_id=telegram_id,
        unique_id=resolved_unique_id,
        file_id=resolved_file_id,
    )
    if unique_id.strip() and unique_id.strip() != resolved_unique_id:
        cache_tdlib_file_preview(
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


def tdlib_toggle_pause_download_fallback(
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
        payload = td_status_payload_from_td_file(
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
    payload = td_status_payload_from_td_file(
        refreshed,
        telegram_id=telegram_id,
        fallback_unique_id=resolved_unique_id,
    )
    if payload["downloadStatus"] != "completed":
        payload["downloadStatus"] = "downloading"
    return payload, True


def tdlib_remove_file_fallback(
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


def td_file_to_ws(file_payload: dict[str, Any]) -> dict[str, Any]:
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


def td_status_payload_from_td_file(
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


def _apply_completed_duplicate(
    db: sqlite3.Connection | None,
    *,
    telegram_id: int,
    target_file_id: int,
    file_payload: dict[str, Any],
) -> dict[str, Any] | None:
    if db is None:
        return None

    unique_id = str(file_payload.get("uniqueId") or "").strip()
    if not unique_id:
        return None

    existing = _find_file_by_unique(
        db,
        telegram_id=telegram_id,
        unique_id=unique_id,
    )
    if existing is None:
        return None

    existing_status = str(existing["download_status"] or "idle").strip().lower()
    existing_path = str(existing["local_path"] or "").strip()
    if existing_status != "completed" or not existing_path:
        return None

    file_payload["alreadyDownloaded"] = True
    file_payload["downloadStatus"] = "completed"
    file_payload["localPath"] = existing_path
    file_payload["downloadedSize"] = max(
        _int_or_default(existing["downloaded_size"], 0),
        _int_or_default(existing["size"], 0),
        _int_or_default(file_payload.get("downloadedSize"), 0),
    )
    existing_completion = _int_or_default(existing["completion_date"], 0)
    if existing_completion > 0:
        file_payload["completionDate"] = existing_completion

    cache_tdlib_file_preview(
        telegram_id=telegram_id,
        unique_id=unique_id,
        file_id=target_file_id,
        mime_type=str(file_payload.get("mimeType") or "").strip() or None,
        local_path=existing_path,
    )
    return file_payload


def start_tdlib_download_for_message(
    td_manager: TdlibAuthManager,
    *,
    db: sqlite3.Connection | None = None,
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

    file_payload = _td_message_to_file(telegram_id, message_result)
    if file_payload is None:
        raise RuntimeError("Failed to map TDLib file payload")

    file_payload["threadChatId"] = _int_or_default(message_thread.get("chat_id"), 0)
    file_payload["messageThreadId"] = _int_or_default(
        message_thread.get("message_thread_id"),
        _int_or_default(file_payload.get("messageThreadId"), 0),
    )

    duplicate_payload = _apply_completed_duplicate(
        db,
        telegram_id=telegram_id,
        target_file_id=target_file_id,
        file_payload=file_payload,
    )
    if duplicate_payload is not None:
        return duplicate_payload

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
        cache_tdlib_file_preview(
            telegram_id=telegram_id,
            unique_id=remote_unique_id,
            file_id=target_file_id,
            mime_type=mime_type,
            local_path=local_path or None,
        )
    if payload_unique_id and payload_unique_id != remote_unique_id:
        cache_tdlib_file_preview(
            telegram_id=telegram_id,
            unique_id=payload_unique_id,
            file_id=target_file_id,
            mime_type=mime_type,
            local_path=local_path or None,
        )

    return file_payload


def enrich_tdlib_thumbnails_for_files(
    db: sqlite3.Connection,
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    files: list[dict[str, Any]],
    upsert_file_record: Callable[..., None],
) -> bool:
    if not files:
        return False
    if not _load_tdlib_session_for_account(td_manager, telegram_id, root_path):
        return False

    account_key = str(telegram_id)
    changed = False

    for file_item in files:
        if not isinstance(file_item, dict):
            continue
        existing_thumb = (
            file_item.get("thumbnailFile")
            if isinstance(file_item.get("thumbnailFile"), dict)
            else None
        )
        if existing_thumb is not None:
            extra = (
                existing_thumb.get("extra")
                if isinstance(existing_thumb.get("extra"), dict)
                else {}
            )
            tw = _int_or_default(extra.get("width"), 0)
            th = _int_or_default(extra.get("height"), 0)
            if max(tw, th) >= 320:
                continue  # thumbnail is adequate quality

        chat_id = _int_or_default(file_item.get("chatId"), 0)
        message_id = _int_or_default(file_item.get("messageId"), 0)
        if chat_id == 0 or message_id == 0:
            continue

        message_result = td_manager.request(
            account_key,
            {
                "@type": "getMessage",
                "chat_id": chat_id,
                "message_id": message_id,
            },
            timeout_seconds=12.0,
        )
        if str(message_result.get("@type") or "") == "error":
            continue

        mapped = _td_message_to_file(telegram_id, message_result)
        if mapped is None:
            continue

        thumbnail_payload = (
            mapped.get("thumbnailFile")
            if isinstance(mapped.get("thumbnailFile"), dict)
            else None
        )
        if thumbnail_payload is None:
            continue

        thumbnail_file_id = _int_or_default(thumbnail_payload.get("id"), 0)
        thumbnail_path = str(thumbnail_payload.get("localPath") or "").strip()
        if thumbnail_file_id > 0 and not thumbnail_path:
            download_result = td_manager.request(
                account_key,
                {
                    "@type": "downloadFile",
                    "file_id": thumbnail_file_id,
                    "priority": 1,
                    "offset": 0,
                    "limit": 0,
                    "synchronous": True,
                },
                timeout_seconds=15.0,
            )
            if str(download_result.get("@type") or "") == "file":
                local = (
                    download_result.get("local")
                    if isinstance(download_result.get("local"), dict)
                    else {}
                )
                if bool(local.get("is_downloading_completed")):
                    resolved_path = str(local.get("path") or "").strip()
                    if resolved_path:
                        thumbnail_payload["localPath"] = resolved_path
                        thumbnail_payload["downloadStatus"] = "completed"
                        thumbnail_payload["downloadedSize"] = _int_or_default(
                            local.get("downloaded_size"),
                            _int_or_default(thumbnail_payload.get("downloadedSize"), 0),
                        )
                        thumbnail_payload["size"] = max(
                            _int_or_default(download_result.get("size"), 0),
                            _int_or_default(download_result.get("expected_size"), 0),
                            _int_or_default(thumbnail_payload.get("size"), 0),
                        )

        current_unique_id = str(file_item.get("uniqueId") or "").strip()
        mapped_unique_id = str(mapped.get("uniqueId") or "").strip()
        if current_unique_id and current_unique_id != mapped_unique_id:
            mapped["uniqueId"] = current_unique_id

        upsert_file_record(db, file_payload=mapped)
        changed = True

    return changed
