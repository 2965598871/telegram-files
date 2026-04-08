from __future__ import annotations

import sqlite3
import time
from typing import Any

from .file_record_ops import find_file_by_unique as _find_file_by_unique
from .tdlib import TdlibAuthManager
from .tdlib_file_mapper import td_message_to_file


def _int_or_default(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def default_chat_auto() -> dict[str, Any]:
    return {
        "preload": {"enabled": False},
        "download": {
            "enabled": False,
            "rule": {
                "query": "",
                "fileTypes": [],
                "downloadHistory": True,
                "downloadCommentFiles": False,
                "filterExpr": "",
            },
        },
        "transfer": {
            "enabled": False,
            "rule": {
                "transferHistory": True,
                "destination": "",
                "transferPolicy": "GROUP_BY_CHAT",
                "duplicationPolicy": "OVERWRITE",
                "extra": {},
            },
        },
        "state": 0,
    }


def _td_chat_type(chat_type: dict[str, Any]) -> str:
    type_name = str(chat_type.get("@type") or "")
    if type_name == "chatTypePrivate":
        return "private"
    if type_name == "chatTypeBasicGroup":
        return "group"
    if type_name == "chatTypeSupergroup":
        return "channel" if bool(chat_type.get("is_channel")) else "group"
    return "private"


def _td_chat_to_response(
    telegram_id: int,
    chat_payload: dict[str, Any],
) -> dict[str, Any]:
    chat_id = _int_or_default(chat_payload.get("id"), 0)
    title = str(chat_payload.get("title") or "").strip()
    photo = chat_payload.get("photo")
    minithumbnail = photo.get("minithumbnail") if isinstance(photo, dict) else None
    avatar = (
        str(minithumbnail.get("data") or "") if isinstance(minithumbnail, dict) else ""
    )
    return {
        "id": str(chat_id),
        "kind": "chat",
        "name": "Saved Messages" if chat_id == telegram_id else (title or str(chat_id)),
        "type": _td_chat_type(chat_payload.get("type") or {}),
        "avatar": avatar,
        "unreadCount": _int_or_default(chat_payload.get("unread_count"), 0),
        "lastMessage": "",
        "lastMessageTime": "",
        "auto": default_chat_auto(),
    }


def load_tdlib_session_for_account(
    td_manager: TdlibAuthManager,
    telegram_id: int,
    root_path: str,
) -> bool:
    account_key = str(telegram_id)
    td_manager.ensure_session(account_key, root_path)
    return td_manager.prepare_authorization(account_key, timeout_seconds=15.0)


def load_tdlib_chats(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    query: str,
    archived: bool,
    activated_chat_id: int | None,
) -> list[dict[str, Any]]:
    if not load_tdlib_session_for_account(td_manager, telegram_id, root_path):
        return []

    account_key = str(telegram_id)
    chat_list_type = "chatListArchive" if archived else "chatListMain"

    load_result = td_manager.request(
        account_key,
        {
            "@type": "loadChats",
            "chat_list": {"@type": chat_list_type},
            "limit": 100,
        },
        timeout_seconds=20.0,
    )
    if (
        str(load_result.get("@type") or "") == "error"
        and _int_or_default(load_result.get("code"), 0) != 404
    ):
        return []

    chats_result = td_manager.request(
        account_key,
        {
            "@type": "getChats",
            "chat_list": {"@type": chat_list_type},
            "limit": 100,
        },
        timeout_seconds=20.0,
    )
    if str(chats_result.get("@type") or "") == "error":
        return []

    chat_ids_raw = chats_result.get("chat_ids")
    chat_ids = [
        _int_or_default(item, 0)
        for item in (chat_ids_raw if isinstance(chat_ids_raw, list) else [])
        if _int_or_default(item, 0) != 0
    ]

    if (
        activated_chat_id is not None
        and activated_chat_id != 0
        and activated_chat_id not in chat_ids
    ):
        chat_ids.insert(0, activated_chat_id)

    normalized_query = query.strip().lower()
    chats: list[dict[str, Any]] = []
    seen: set[int] = set()
    for chat_id in chat_ids:
        if chat_id in seen:
            continue
        chat_result = td_manager.request(
            account_key,
            {"@type": "getChat", "chat_id": chat_id},
            timeout_seconds=20.0,
        )
        if str(chat_result.get("@type") or "") == "error":
            continue
        chat_name = str(chat_result.get("title") or "")
        if normalized_query and normalized_query not in chat_name.lower():
            continue
        chats.append(_td_chat_to_response(telegram_id, chat_result))
        seen.add(chat_id)

    return chats


def load_tdlib_chat_files_count(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    chat_id: int,
) -> dict[str, int]:
    if not load_tdlib_session_for_account(td_manager, telegram_id, root_path):
        raise RuntimeError("TDLib is not ready yet. Please try again.")

    account_key = str(telegram_id)
    filters = {
        "media": "searchMessagesFilterPhotoAndVideo",
        "photo": "searchMessagesFilterPhoto",
        "video": "searchMessagesFilterVideo",
        "audio": "searchMessagesFilterAudio",
        "file": "searchMessagesFilterDocument",
    }

    result: dict[str, int] = {
        "media": 0,
        "photo": 0,
        "video": 0,
        "audio": 0,
        "file": 0,
    }
    for file_type, filter_type in filters.items():
        response = td_manager.request(
            account_key,
            {
                "@type": "getChatMessageCount",
                "chat_id": chat_id,
                "filter": {
                    "@type": filter_type,
                },
                "return_local": False,
            },
            timeout_seconds=20.0,
        )
        if str(response.get("@type") or "") == "error":
            raise RuntimeError(
                str(response.get("message") or "Failed to get chat message count")
            )
        result[file_type] = _int_or_default(response.get("count"), 0)

    return result


def load_tdlib_network_statistics(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
) -> dict[str, int]:
    if not load_tdlib_session_for_account(td_manager, telegram_id, root_path):
        raise RuntimeError("TDLib is not ready yet. Please try again.")

    response = td_manager.request(
        str(telegram_id),
        {
            "@type": "getNetworkStatistics",
        },
        timeout_seconds=20.0,
    )
    if str(response.get("@type") or "") == "error":
        raise RuntimeError(
            str(response.get("message") or "Failed to load network statistics")
        )

    sent_bytes = 0
    received_bytes = 0
    entries = response.get("entries")
    if isinstance(entries, list):
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("@type") or "") != "networkStatisticsEntryFile":
                continue
            sent_bytes += _int_or_default(entry.get("sent_bytes"), 0)
            received_bytes += _int_or_default(entry.get("received_bytes"), 0)

    return {
        "sinceDate": _int_or_default(response.get("since_date"), int(time.time())),
        "sentBytes": sent_bytes,
        "receivedBytes": received_bytes,
    }


def load_tdlib_ping_seconds(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
) -> float:
    if not load_tdlib_session_for_account(td_manager, telegram_id, root_path):
        raise RuntimeError("TDLib is not ready yet. Please try again.")

    account_key = str(telegram_id)
    proxies = td_manager.request(
        account_key,
        {
            "@type": "getProxies",
        },
        timeout_seconds=15.0,
    )
    if str(proxies.get("@type") or "") == "error":
        raise RuntimeError(str(proxies.get("message") or "Failed to load proxies"))

    proxy_id = 0
    raw_proxies = proxies.get("proxies")
    if isinstance(raw_proxies, list):
        for item in raw_proxies:
            if not isinstance(item, dict):
                continue
            if bool(item.get("is_enabled")):
                proxy_id = _int_or_default(item.get("id"), 0)
                break

    ping_payload = td_manager.request(
        account_key,
        {
            "@type": "pingProxy",
            "proxy_id": proxy_id,
        },
        timeout_seconds=20.0,
    )
    if str(ping_payload.get("@type") or "") == "error":
        if proxy_id == 0:
            return 0.0
        raise RuntimeError(str(ping_payload.get("message") or "Failed to ping proxy"))

    try:
        return float(ping_payload.get("seconds") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def tdlib_test_network(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
) -> bool:
    if not load_tdlib_session_for_account(td_manager, telegram_id, root_path):
        raise RuntimeError("TDLib is not ready yet. Please try again.")

    result = td_manager.request(
        str(telegram_id),
        {
            "@type": "testNetwork",
        },
        timeout_seconds=20.0,
    )
    return str(result.get("@type") or "") != "error"


def parse_link_files(
    td_manager: TdlibAuthManager,
    *,
    db: sqlite3.Connection | None = None,
    telegram_id: int,
    root_path: str,
    link: str,
) -> dict[str, Any]:
    if not load_tdlib_session_for_account(td_manager, telegram_id, root_path):
        raise RuntimeError("TDLib is not ready yet. Please try again.")

    account_key = str(telegram_id)
    link_info = td_manager.request(
        account_key,
        {
            "@type": "getMessageLinkInfo",
            "url": link,
        },
        timeout_seconds=25.0,
    )
    if str(link_info.get("@type") or "") == "error":
        raise RuntimeError(str(link_info.get("message") or "Failed to parse link"))

    messages: list[dict[str, Any]] = []
    message = link_info.get("message")
    if isinstance(message, dict):
        messages.append(message)

    for_album = bool(link_info.get("for_album"))
    if for_album and isinstance(message, dict):
        media_album_id = _int_or_default(message.get("media_album_id"), 0)
        chat_id = _int_or_default(message.get("chat_id"), 0)
        from_message_id = _int_or_default(message.get("id"), 0)
        if media_album_id != 0 and chat_id != 0 and from_message_id != 0:
            history = td_manager.request(
                account_key,
                {
                    "@type": "getChatHistory",
                    "chat_id": chat_id,
                    "from_message_id": from_message_id,
                    "offset": 0,
                    "limit": 60,
                    "only_local": False,
                },
                timeout_seconds=25.0,
            )
            history_messages = (
                history.get("messages") if isinstance(history, dict) else None
            )
            if isinstance(history_messages, list):
                album_messages = [
                    item
                    for item in history_messages
                    if isinstance(item, dict)
                    and _int_or_default(item.get("media_album_id"), 0) == media_album_id
                ]
                if album_messages:
                    messages = album_messages

    converted_files = [
        file_payload
        for file_payload in (
            td_message_to_file(telegram_id, item)
            for item in messages
            if isinstance(item, dict)
        )
        if file_payload is not None
    ]
    converted_files = _apply_media_album_captions(converted_files)
    converted_files = [
        _apply_archive_download_state(
            db,
            telegram_id=telegram_id,
            file_payload=file_payload,
        )
        for file_payload in converted_files
    ]

    return {
        "files": converted_files,
        "count": len(converted_files),
        "size": len(converted_files),
        "nextFromMessageId": 0,
    }


def _matches_td_file_filters(
    file_payload: dict[str, Any],
    filters: dict[str, str],
) -> bool:
    normalized_search = str(filters.get("search") or "").strip().lower()
    if normalized_search:
        file_name = str(file_payload.get("fileName") or "").lower()
        caption = str(file_payload.get("caption") or "").lower()
        if normalized_search not in file_name and normalized_search not in caption:
            return False

    normalized_type = str(filters.get("type") or "").strip().lower()
    if normalized_type and normalized_type != "all":
        payload_type = str(file_payload.get("type") or "").strip().lower()
        if normalized_type == "media":
            if payload_type not in {"photo", "video"}:
                return False
        elif payload_type != normalized_type:
            return False

    normalized_download_status = (
        str(filters.get("downloadStatus") or "").strip().lower()
    )
    if (
        normalized_download_status
        and str(file_payload.get("downloadStatus") or "").strip().lower()
        != normalized_download_status
    ):
        return False

    normalized_transfer_status = (
        str(filters.get("transferStatus") or "").strip().lower()
    )
    if (
        normalized_transfer_status
        and str(file_payload.get("transferStatus") or "").strip().lower()
        != normalized_transfer_status
    ):
        return False

    normalized_already_downloaded = (
        str(filters.get("alreadyDownloaded") or "").strip().lower()
    )
    if normalized_already_downloaded:
        expected = normalized_already_downloaded in {"true", "1", "yes", "on"}
        if bool(file_payload.get("alreadyDownloaded")) != expected:
            return False

    message_thread_id_filter = _int_or_default(filters.get("messageThreadId"), 0)
    if (
        message_thread_id_filter != 0
        and _int_or_default(file_payload.get("messageThreadId"), 0)
        != message_thread_id_filter
    ):
        return False

    if str(filters.get("tags") or "").strip():
        return False

    return True


def _apply_archive_download_state(
    db: sqlite3.Connection | None,
    *,
    telegram_id: int,
    file_payload: dict[str, Any],
) -> dict[str, Any]:
    normalized = dict(file_payload)
    normalized["alreadyDownloaded"] = False
    if db is None:
        return normalized

    unique_id = str(normalized.get("uniqueId") or "").strip()
    if not unique_id:
        return normalized

    existing = _find_file_by_unique(
        db,
        telegram_id=telegram_id,
        unique_id=unique_id,
    )
    if existing is None:
        return normalized

    existing_status = str(existing["download_status"] or "idle").strip().lower()
    existing_path = str(existing["local_path"] or "").strip()
    if existing_status != "completed" or not existing_path:
        return normalized

    normalized["alreadyDownloaded"] = True
    normalized["downloadStatus"] = "completed"
    normalized["localPath"] = existing_path
    normalized["downloadedSize"] = max(
        _int_or_default(existing["downloaded_size"], 0),
        _int_or_default(existing["size"], 0),
        _int_or_default(normalized.get("downloadedSize"), 0),
    )

    completion_date = _int_or_default(existing["completion_date"], 0)
    if completion_date > 0:
        normalized["completionDate"] = completion_date

    transfer_status = str(existing["transfer_status"] or "").strip()
    if transfer_status:
        normalized["transferStatus"] = transfer_status

    return normalized


def _apply_media_album_captions(
    files: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    album_captions: dict[tuple[int, int], str] = {}
    for file_payload in files:
        media_album_id = _int_or_default(file_payload.get("mediaAlbumId"), 0)
        chat_id = _int_or_default(file_payload.get("chatId"), 0)
        caption = str(file_payload.get("caption") or "")
        if media_album_id == 0 or chat_id == 0 or not caption.strip():
            continue
        album_captions[(chat_id, media_album_id)] = caption

    if not album_captions:
        return files

    normalized_files: list[dict[str, Any]] = []
    for file_payload in files:
        media_album_id = _int_or_default(file_payload.get("mediaAlbumId"), 0)
        chat_id = _int_or_default(file_payload.get("chatId"), 0)
        if (
            media_album_id != 0
            and chat_id != 0
            and not str(file_payload.get("caption") or "").strip()
        ):
            album_caption = album_captions.get((chat_id, media_album_id), "")
            if album_caption:
                normalized_files.append({**file_payload, "caption": album_caption})
                continue
        normalized_files.append(file_payload)
    return normalized_files


def load_tdlib_chat_files(
    td_manager: TdlibAuthManager,
    *,
    db: sqlite3.Connection | None = None,
    telegram_id: int,
    root_path: str,
    chat_id: int,
    filters: dict[str, str],
) -> dict[str, Any]:
    if not load_tdlib_session_for_account(td_manager, telegram_id, root_path):
        raise RuntimeError("TDLib is not ready yet. Please try again.")

    account_key = str(telegram_id)
    from_message_id = _int_or_default(filters.get("fromMessageId"), 0)
    limit = _int_or_default(filters.get("limit"), 20)
    if limit <= 0:
        limit = 20
    if limit > 200:
        limit = 200

    request_limit = min(max(limit * 3, 50), 100)
    max_batches = 12
    files: list[dict[str, Any]] = []
    seen_message_ids: set[int] = set()
    next_cursor = from_message_id
    has_more = False

    for _ in range(max_batches):
        history = td_manager.request(
            account_key,
            {
                "@type": "getChatHistory",
                "chat_id": chat_id,
                "from_message_id": next_cursor,
                "offset": -1 if next_cursor > 0 else 0,
                "limit": request_limit,
                "only_local": False,
            },
            timeout_seconds=25.0,
        )
        if str(history.get("@type") or "") == "error":
            raise RuntimeError(
                str(history.get("message") or "Failed to load chat history")
            )

        history_messages = (
            history.get("messages") if isinstance(history, dict) else None
        )
        if not isinstance(history_messages, list) or not history_messages:
            has_more = False
            next_cursor = 0
            break

        batch_last_message_id = 0
        batch_files: list[dict[str, Any]] = []
        for message in history_messages:
            if not isinstance(message, dict):
                continue

            message_id = _int_or_default(message.get("id"), 0)
            if message_id <= 0 or message_id in seen_message_ids:
                continue
            seen_message_ids.add(message_id)
            batch_last_message_id = message_id

            file_payload = td_message_to_file(telegram_id, message)
            if file_payload is None:
                continue

            batch_files.append(file_payload)

        batch_files = _apply_media_album_captions(batch_files)
        batch_files = [
            _apply_archive_download_state(
                db,
                telegram_id=telegram_id,
                file_payload=file_payload,
            )
            for file_payload in batch_files
        ]
        for file_payload in batch_files:
            if not _matches_td_file_filters(file_payload, filters):
                continue

            files.append(file_payload)
            if len(files) >= limit:
                break

        if len(files) >= limit:
            has_more = True
            next_cursor = batch_last_message_id
            break

        if len(history_messages) < request_limit:
            has_more = False
            next_cursor = 0
            break

        if batch_last_message_id <= 0:
            has_more = False
            next_cursor = 0
            break

        has_more = True
        next_cursor = batch_last_message_id

    returned_files = files[:limit]
    return {
        "files": returned_files,
        "count": 1_000_000_000 if has_more and next_cursor > 0 else len(returned_files),
        "size": len(returned_files),
        "nextFromMessageId": next_cursor if has_more and next_cursor > 0 else 0,
    }
