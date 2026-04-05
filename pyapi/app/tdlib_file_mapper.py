from __future__ import annotations

import time
from typing import Any


def _int_or_default(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_td_file(td_file: Any) -> dict[str, Any] | None:
    return td_file if isinstance(td_file, dict) else None


def extract_td_message_file(message: dict[str, Any]) -> dict[str, Any] | None:
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


def td_message_to_file(
    telegram_id: int,
    message: dict[str, Any],
) -> dict[str, Any] | None:
    extracted = extract_td_message_file(message)
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
        unique_id = (
            f"td-{_int_or_default(message.get('chat_id'), 0)}-"
            f"{_int_or_default(message.get('id'), 0)}-"
            f"{_int_or_default(td_file.get('id'), 0)}"
        )

    return {
        "id": _int_or_default(td_file.get("id"), 0),
        "telegramId": telegram_id,
        "uniqueId": unique_id,
        "messageId": _int_or_default(message.get("id"), 0),
        "chatId": _int_or_default(message.get("chat_id"), 0),
        "mediaAlbumId": _int_or_default(message.get("media_album_id"), 0),
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
