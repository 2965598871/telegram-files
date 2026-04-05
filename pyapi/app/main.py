from __future__ import annotations

import asyncio
import logging
import secrets
import sqlite3
import os
import time
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
from .settings_keys import default_value_for
from .tdlib import (
    TdlibAuthManager,
    TdlibConfigurationError,
    TdlibRequestTimeout,
)


SESSION_COOKIE_NAME = "tf"

EVENT_TYPE_ERROR = -1
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
    try:
        yield
    finally:
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


def _phone_auth_settings() -> dict[str, Any]:
    return {
        "@type": "phoneNumberAuthenticationSettings",
        "allow_flash_call": False,
        "allow_missed_call": False,
        "is_current_phone_number": False,
        "allow_sms_retriever_api": False,
        "authentication_tokens": [],
    }


def _build_tdlib_method_payload(
    method: str,
    params: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if method == "SetAuthenticationPhoneNumber":
        phone_number = str(params.get("phoneNumber") or "").strip()
        if not phone_number:
            raise ValueError("phoneNumber is required")
        return (
            {
                "@type": "setAuthenticationPhoneNumber",
                "phone_number": phone_number,
                "settings": _phone_auth_settings(),
            },
            {"phoneNumber": phone_number},
        )

    if method == "CheckAuthenticationCode":
        code_value = str(params.get("code") or "").strip()
        if not code_value:
            raise ValueError("code is required")
        return (
            {
                "@type": "checkAuthenticationCode",
                "code": code_value,
            },
            {},
        )

    if method == "CheckAuthenticationPassword":
        password_value = str(params.get("password") or "").strip()
        if not password_value:
            raise ValueError("password is required")
        return (
            {
                "@type": "checkAuthenticationPassword",
                "password": password_value,
            },
            {},
        )

    if method == "RequestQrCodeAuthentication":
        return (
            {
                "@type": "requestQrCodeAuthentication",
                "other_user_ids": [],
            },
            {},
        )

    raise ValueError(f"Unsupported auth method: {method}")


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


def _default_chat_auto() -> dict[str, Any]:
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
        "name": "Saved Messages" if chat_id == telegram_id else (title or str(chat_id)),
        "type": _td_chat_type(chat_payload.get("type") or {}),
        "avatar": avatar,
        "unreadCount": _int_or_default(chat_payload.get("unread_count"), 0),
        "lastMessage": "",
        "lastMessageTime": "",
        "auto": _default_chat_auto(),
    }


def _safe_td_file(td_file: Any) -> dict[str, Any] | None:
    return td_file if isinstance(td_file, dict) else None


def _extract_td_message_file(message: dict[str, Any]) -> dict[str, Any] | None:
    content = message.get("content")
    if not isinstance(content, dict):
        return None

    content_type = str(content.get("@type") or "")
    caption = content.get("caption")
    caption_text = str(caption.get("text") or "") if isinstance(caption, dict) else ""

    if content_type == "messagePhoto":
        photo = content.get("photo")
        if not isinstance(photo, dict):
            return None
        sizes = photo.get("sizes")
        candidates = (
            [item for item in sizes if isinstance(item, dict)]
            if isinstance(sizes, list)
            else []
        )
        best = max(
            candidates,
            key=lambda item: _int_or_default(item.get("width"), 0)
            * _int_or_default(item.get("height"), 0),
            default=None,
        )
        td_file = _safe_td_file(best.get("photo") if isinstance(best, dict) else None)
        if td_file is None:
            return None
        minithumbnail = photo.get("minithumbnail")
        thumbnail = (
            str(minithumbnail.get("data") or "")
            if isinstance(minithumbnail, dict)
            else ""
        )
        return {
            "file": td_file,
            "caption": caption_text,
            "type": "photo",
            "fileName": f"photo_{_int_or_default(message.get('id'), 0)}.jpg",
            "mimeType": "image/jpeg",
            "thumbnail": thumbnail,
            "extra": {
                "width": _int_or_default(
                    best.get("width") if isinstance(best, dict) else 0
                ),
                "height": _int_or_default(
                    best.get("height") if isinstance(best, dict) else 0
                ),
                "type": str(best.get("type") or "") if isinstance(best, dict) else "",
            },
            "hasSensitiveContent": bool(content.get("has_spoiler")),
        }

    if content_type == "messageVideo":
        video = content.get("video")
        if not isinstance(video, dict):
            return None
        td_file = _safe_td_file(video.get("video"))
        if td_file is None:
            return None
        minithumbnail = video.get("minithumbnail")
        thumbnail = (
            str(minithumbnail.get("data") or "")
            if isinstance(minithumbnail, dict)
            else ""
        )
        return {
            "file": td_file,
            "caption": caption_text,
            "type": "video",
            "fileName": str(
                video.get("file_name")
                or f"video_{_int_or_default(message.get('id'), 0)}.mp4"
            ),
            "mimeType": str(video.get("mime_type") or "video/mp4"),
            "thumbnail": thumbnail,
            "extra": {
                "width": _int_or_default(video.get("width"), 0),
                "height": _int_or_default(video.get("height"), 0),
                "duration": _int_or_default(video.get("duration"), 0),
                "mimeType": str(video.get("mime_type") or ""),
            },
            "hasSensitiveContent": bool(content.get("has_spoiler")),
        }

    if content_type == "messageAnimation":
        animation = content.get("animation")
        if not isinstance(animation, dict):
            return None
        td_file = _safe_td_file(animation.get("animation"))
        if td_file is None:
            return None
        minithumbnail = animation.get("minithumbnail")
        thumbnail = (
            str(minithumbnail.get("data") or "")
            if isinstance(minithumbnail, dict)
            else ""
        )
        return {
            "file": td_file,
            "caption": caption_text,
            "type": "video",
            "fileName": str(
                animation.get("file_name")
                or f"animation_{_int_or_default(message.get('id'), 0)}.mp4"
            ),
            "mimeType": str(animation.get("mime_type") or "video/mp4"),
            "thumbnail": thumbnail,
            "extra": {
                "width": _int_or_default(animation.get("width"), 0),
                "height": _int_or_default(animation.get("height"), 0),
                "duration": _int_or_default(animation.get("duration"), 0),
                "mimeType": str(animation.get("mime_type") or ""),
            },
            "hasSensitiveContent": bool(content.get("has_spoiler")),
        }

    if content_type == "messageAudio":
        audio = content.get("audio")
        if not isinstance(audio, dict):
            return None
        td_file = _safe_td_file(audio.get("audio"))
        if td_file is None:
            return None
        return {
            "file": td_file,
            "caption": caption_text,
            "type": "audio",
            "fileName": str(
                audio.get("file_name")
                or f"audio_{_int_or_default(message.get('id'), 0)}.mp3"
            ),
            "mimeType": str(audio.get("mime_type") or "audio/mpeg"),
            "thumbnail": "",
            "extra": None,
            "hasSensitiveContent": False,
        }

    if content_type == "messageDocument":
        document = content.get("document")
        if not isinstance(document, dict):
            return None
        td_file = _safe_td_file(document.get("document"))
        if td_file is None:
            return None
        minithumbnail = document.get("minithumbnail")
        thumbnail = (
            str(minithumbnail.get("data") or "")
            if isinstance(minithumbnail, dict)
            else ""
        )
        return {
            "file": td_file,
            "caption": caption_text,
            "type": "file",
            "fileName": str(
                document.get("file_name")
                or f"file_{_int_or_default(message.get('id'), 0)}"
            ),
            "mimeType": str(document.get("mime_type") or "application/octet-stream"),
            "thumbnail": thumbnail,
            "extra": None,
            "hasSensitiveContent": False,
        }

    return None


def _reaction_count_from_message(message: dict[str, Any]) -> int:
    interaction = message.get("interaction_info")
    if not isinstance(interaction, dict):
        return 0
    reactions = interaction.get("reactions")
    if not isinstance(reactions, dict):
        return 0
    entries = reactions.get("reactions")
    if not isinstance(entries, list):
        return 0
    return sum(
        _int_or_default(item.get("total_count"), 0)
        for item in entries
        if isinstance(item, dict)
    )


def _td_message_to_file(
    telegram_id: int, message: dict[str, Any]
) -> dict[str, Any] | None:
    extracted = _extract_td_message_file(message)
    if extracted is None:
        return None

    td_file = extracted["file"]
    local = td_file.get("local") if isinstance(td_file.get("local"), dict) else {}
    remote = td_file.get("remote") if isinstance(td_file.get("remote"), dict) else {}

    is_completed = bool(local.get("is_downloading_completed"))
    is_downloading = bool(local.get("is_downloading_active"))
    download_status = (
        "completed" if is_completed else ("downloading" if is_downloading else "idle")
    )
    date_seconds = _int_or_default(message.get("date"), 0)

    unique_id = str(remote.get("unique_id") or "").strip()
    if not unique_id:
        unique_id = f"td-{_int_or_default(message.get('chat_id'), 0)}-{_int_or_default(message.get('id'), 0)}-{_int_or_default(td_file.get('id'), 0)}"

    return {
        "id": _int_or_default(td_file.get("id"), 0),
        "telegramId": telegram_id,
        "uniqueId": unique_id,
        "messageId": _int_or_default(message.get("id"), 0),
        "chatId": _int_or_default(message.get("chat_id"), 0),
        "fileName": extracted["fileName"],
        "type": extracted["type"],
        "mimeType": extracted["mimeType"],
        "size": _int_or_default(td_file.get("size"), 0),
        "downloadedSize": _int_or_default(local.get("downloaded_size"), 0),
        "thumbnail": extracted["thumbnail"],
        "thumbnailFile": None,
        "downloadStatus": download_status,
        "date": date_seconds,
        "formatDate": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(date_seconds))
        if date_seconds > 0
        else "",
        "caption": extracted["caption"],
        "localPath": str(local.get("path") or "") if is_completed else "",
        "hasSensitiveContent": extracted["hasSensitiveContent"],
        "startDate": 0,
        "completionDate": int(time.time() * 1000) if is_completed else 0,
        "originalDeleted": False,
        "transferStatus": "idle",
        "extra": extracted["extra"],
        "tags": None,
        "loaded": False,
        "threadChatId": 0,
        "messageThreadId": _int_or_default(message.get("message_thread_id"), 0),
        "hasReply": False,
        "reactionCount": _reaction_count_from_message(message),
    }


def _load_tdlib_session_for_account(
    td_manager: TdlibAuthManager,
    telegram_id: int,
    root_path: str,
) -> bool:
    account_key = str(telegram_id)
    td_manager.ensure_session(account_key, root_path)
    return td_manager.prepare_authorization(account_key, timeout_seconds=15.0)


def _load_tdlib_chats(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    query: str,
    archived: bool,
    activated_chat_id: int | None,
) -> list[dict[str, Any]]:
    if not _load_tdlib_session_for_account(td_manager, telegram_id, root_path):
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


def _parse_link_files(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    link: str,
) -> dict[str, Any]:
    if not _load_tdlib_session_for_account(td_manager, telegram_id, root_path):
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
            _td_message_to_file(telegram_id, item)
            for item in messages
            if isinstance(item, dict)
        )
        if file_payload is not None
    ]

    return {
        "files": converted_files,
        "count": len(converted_files),
        "size": len(converted_files),
        "nextFromMessageId": 0,
    }


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
            total_size = max(
                _int_or_default(td_file.get("size"), 0),
                _int_or_default(td_file.get("expected_size"), 0),
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

    extracted = _extract_td_message_file(message_result)
    if extracted is None:
        raise RuntimeError("This message doesn't contain a downloadable file.")

    td_file = extracted["file"]
    target_file_id = _int_or_default(td_file.get("id"), 0)
    if target_file_id == 0:
        raise RuntimeError("Invalid TDLib file id")

    if file_id != 0 and target_file_id != file_id:
        target_file_id = file_id

    download_result = td_manager.request(
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
    if str(download_result.get("@type") or "") == "error":
        raise RuntimeError(
            str(download_result.get("message") or "Failed to start download")
        )

    file_payload = _td_message_to_file(telegram_id, message_result)
    if file_payload is None:
        raise RuntimeError("Failed to map TDLib file payload")

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
def telegram_api_methods() -> dict[str, list[str]]:
    return {
        "methods": sorted(SUPPORTED_TELEGRAM_METHODS.keys()),
    }


@app.get("/telegram/api/{method}/parameters")
def telegram_api_method_parameters(method: str) -> dict[str, Any]:
    if method not in SUPPORTED_TELEGRAM_METHODS:
        raise HTTPException(status_code=404, detail="Method not found")
    return {
        "parameters": SUPPORTED_TELEGRAM_METHODS[method],
    }


@app.post("/telegram/api/{method}")
async def telegram_api_method(
    method: str,
    payload: dict[str, Any] | None,
    request: Request,
    db: sqlite3.Connection = Depends(get_db),
) -> JSONResponse:
    if method not in SUPPORTED_TELEGRAM_METHODS:
        return _method_error(f"Unsupported method: {method}")

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
            return _method_error(f"Unsupported method: {method}")

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

    return td_chats if td_chats else db_chats


@app.get("/telegram/{telegramId}/download-statistics")
def telegram_download_statistics(
    telegramId: str,
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
                "interval": 300,
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
    return get_telegram_download_statistics(db, normalized_telegram_id)


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
def telegram_ping(
    telegramId: str,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, float]:
    if _is_pending_account(telegramId):
        with STATE_LOCK:
            pending = PENDING_TELEGRAMS.get(telegramId)
            seconds = 0.08 if pending is not None and pending.proxy else 0.0
        return {"ping": seconds}

    return {"ping": get_telegram_ping_seconds(db, _to_telegram_id(telegramId))}


@app.get("/telegram/{telegramId}/test-network")
def telegram_test_network(telegramId: str) -> dict[str, bool]:
    if _is_pending_account(telegramId):
        return {"success": True}
    try:
        int(telegramId)
    except ValueError as exc:
        raise HTTPException(
            status_code=404, detail="Telegram account not found."
        ) from exc
    return {"success": True}


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

    return list_files(db, telegram_id=telegramId, chat_id=chatId, filters=filters)


@app.get("/telegram/{telegramId}/chat/{chatId}/files/count")
def telegram_files_count(
    telegramId: int,
    chatId: int,
    db: sqlite3.Connection = Depends(get_db),
) -> dict[str, int]:
    return count_files_by_type(db, telegram_id=telegramId, chat_id=chatId)


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
def file_preview(
    telegramId: int,
    uniqueId: str,
    db: sqlite3.Connection = Depends(get_db),
) -> FileResponse:
    info = get_file_preview_info(
        db,
        telegram_id=telegramId,
        unique_id=uniqueId,
    )
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
    file_record = start_file_download(
        db,
        telegram_id=telegramId,
        chat_id=chat_id,
        message_id=message_id,
        file_id=file_id,
    )
    if file_record is None:
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
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

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
        raise HTTPException(status_code=404, detail="File not found")

    session_id = _session_id_from_request(request)
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
        raise HTTPException(status_code=404, detail="File not found")

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
        raise HTTPException(status_code=404, detail="File not found")

    session_id = _session_id_from_request(request)
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
        await _emit_ws_payload(
            _build_ws_payload(
                EVENT_TYPE_FILE_STATUS,
                _file_status_from_file_record(file_record),
            ),
            session_id=session_id,
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
        if result is None:
            failed += 1
            continue

        processed += 1
        await _emit_ws_payload(
            _build_ws_payload(EVENT_TYPE_FILE_STATUS, result),
            session_id=session_id,
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
        if result is None:
            failed += 1
            continue

        processed += 1
        await _emit_ws_payload(
            _build_ws_payload(EVENT_TYPE_FILE_STATUS, result),
            session_id=session_id,
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
        if result is None:
            failed += 1
            continue

        processed += 1
        await _emit_ws_payload(
            _build_ws_payload(EVENT_TYPE_FILE_STATUS, result),
            session_id=session_id,
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
