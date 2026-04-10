from __future__ import annotations

import logging
import sqlite3
from typing import Any

from .file_record_ops import upsert_tdlib_file_record as _upsert_tdlib_file_record
from .tdlib import TdlibAuthManager
from .tdlib_file_mapper import td_message_to_file as _td_message_to_file
from .tdlib_queries import load_tdlib_session_for_account as _load_tdlib_session

logger = logging.getLogger(__name__)


def _int_or_default(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _text(value: Any) -> str:
    return str(value or "").strip()


def _preserve_existing_identity(
    row: sqlite3.Row,
    mapped: dict[str, Any],
) -> dict[str, Any]:
    normalized = dict(mapped)
    normalized["id"] = _int_or_default(row["id"], _int_or_default(mapped.get("id"), 0))
    normalized["uniqueId"] = _text(row["unique_id"]) or _text(mapped.get("uniqueId"))

    if _text(row["caption"]) and not _text(mapped.get("caption")):
        normalized["caption"] = _text(row["caption"])
    if (
        _int_or_default(row["media_album_id"], 0) > 0
        and _int_or_default(mapped.get("mediaAlbumId"), 0) == 0
    ):
        normalized["mediaAlbumId"] = _int_or_default(row["media_album_id"], 0)
    return normalized


def _propagate_album_captions(db: sqlite3.Connection, *, telegram_id: int) -> int:
    rows = db.execute(
        """
        SELECT id, unique_id, chat_id, media_album_id, caption
        FROM file_record
        WHERE telegram_id = ?
          AND type != 'thumbnail'
        ORDER BY message_id DESC
        """,
        (telegram_id,),
    ).fetchall()
    caption_map: dict[tuple[int, int], str] = {}
    for row in rows:
        chat_id = _int_or_default(row["chat_id"], 0)
        media_album_id = _int_or_default(row["media_album_id"], 0)
        caption = _text(row["caption"])
        if chat_id == 0 or media_album_id == 0 or not caption:
            continue
        caption_map.setdefault((chat_id, media_album_id), caption)

    if not caption_map:
        return 0

    updated = 0
    for row in rows:
        chat_id = _int_or_default(row["chat_id"], 0)
        media_album_id = _int_or_default(row["media_album_id"], 0)
        if chat_id == 0 or media_album_id == 0 or _text(row["caption"]):
            continue
        caption = caption_map.get((chat_id, media_album_id), "")
        if not caption:
            continue
        cursor = db.execute(
            """
            UPDATE file_record
            SET caption = ?
            WHERE id = ? AND unique_id = ?
            """,
            (
                caption,
                _int_or_default(row["id"], 0),
                _text(row["unique_id"]),
            ),
        )
        updated += max(cursor.rowcount, 0)
    db.commit()
    return updated


def _get_message(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    chat_id: int,
    message_id: int,
) -> dict[str, Any]:
    payload = td_manager.request(
        str(telegram_id),
        {
            "@type": "getMessage",
            "chat_id": chat_id,
            "message_id": message_id,
        },
        timeout_seconds=15.0,
    )
    if str(payload.get("@type") or "") == "error":
        raise RuntimeError(str(payload.get("message") or "Message not found"))
    return payload


def _ensure_thumbnail_download(
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    thumbnail_payload: dict[str, Any],
) -> None:
    thumbnail_file_id = _int_or_default(thumbnail_payload.get("id"), 0)
    if thumbnail_file_id <= 0:
        return

    if _text(thumbnail_payload.get("localPath")):
        return

    download_result = td_manager.request(
        str(telegram_id),
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
    if str(download_result.get("@type") or "") != "file":
        return

    local = (
        download_result.get("local")
        if isinstance(download_result.get("local"), dict)
        else {}
    )
    if not bool(local.get("is_downloading_completed")):
        return

    resolved_path = _text(local.get("path"))
    if not resolved_path:
        return

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


def run_album_metadata_backfill(
    db: sqlite3.Connection,
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    limit: int = 100,
) -> dict[str, int]:
    if not _load_tdlib_session(td_manager, telegram_id, root_path):
        raise RuntimeError("TDLib is not ready yet. Please try again.")

    stats = {
        "scanned": 0,
        "updated": 0,
        "skipped": 0,
        "failed": 0,
        "captionsPropagated": _propagate_album_captions(db, telegram_id=telegram_id),
    }
    rows = db.execute(
        """
        SELECT id, unique_id, chat_id, message_id, media_album_id, caption
        FROM file_record
        WHERE telegram_id = ?
          AND type != 'thumbnail'
          AND chat_id != 0
          AND message_id != 0
          AND COALESCE(media_album_id, 0) = 0
        ORDER BY message_id DESC
        LIMIT ?
        """,
        (telegram_id, max(1, limit)),
    ).fetchall()

    for row in rows:
        stats["scanned"] += 1
        chat_id = _int_or_default(row["chat_id"], 0)
        message_id = _int_or_default(row["message_id"], 0)
        if chat_id == 0 or message_id == 0:
            stats["skipped"] += 1
            continue

        try:
            message = _get_message(
                td_manager,
                telegram_id=telegram_id,
                chat_id=chat_id,
                message_id=message_id,
            )
            mapped = _td_message_to_file(telegram_id, message)
            if mapped is None:
                stats["skipped"] += 1
                continue

            before_album_id = _int_or_default(row["media_album_id"], 0)
            before_caption = _text(row["caption"])
            normalized = _preserve_existing_identity(row, mapped)
            _upsert_tdlib_file_record(db, file_payload=normalized)
            if (
                before_album_id == 0
                and _int_or_default(normalized.get("mediaAlbumId"), 0) != 0
            ) or (not before_caption and _text(normalized.get("caption"))):
                stats["updated"] += 1
            else:
                stats["skipped"] += 1
        except Exception as exc:
            logger.warning(
                "Album maintenance failed for telegram=%s chat=%s message=%s: %s",
                telegram_id,
                chat_id,
                message_id,
                exc,
            )
            stats["failed"] += 1

    stats["captionsPropagated"] += _propagate_album_captions(
        db, telegram_id=telegram_id
    )
    return stats


def run_thumbnail_backfill(
    db: sqlite3.Connection,
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    limit: int = 100,
) -> dict[str, int]:
    if not _load_tdlib_session(td_manager, telegram_id, root_path):
        raise RuntimeError("TDLib is not ready yet. Please try again.")

    stats = {
        "scanned": 0,
        "updated": 0,
        "skipped": 0,
        "failed": 0,
    }
    rows = db.execute(
        """
        SELECT id, unique_id, chat_id, message_id, media_album_id, caption,
               thumbnail_unique_id
        FROM file_record
        WHERE telegram_id = ?
          AND type != 'thumbnail'
          AND chat_id != 0
          AND message_id != 0
          AND (thumbnail_unique_id IS NULL OR TRIM(thumbnail_unique_id) = '')
        ORDER BY message_id DESC
        LIMIT ?
        """,
        (telegram_id, max(1, limit)),
    ).fetchall()

    for row in rows:
        stats["scanned"] += 1
        chat_id = _int_or_default(row["chat_id"], 0)
        message_id = _int_or_default(row["message_id"], 0)
        if chat_id == 0 or message_id == 0:
            stats["skipped"] += 1
            continue

        try:
            message = _get_message(
                td_manager,
                telegram_id=telegram_id,
                chat_id=chat_id,
                message_id=message_id,
            )
            mapped = _td_message_to_file(telegram_id, message)
            if mapped is None:
                stats["skipped"] += 1
                continue

            thumbnail_payload = (
                mapped.get("thumbnailFile")
                if isinstance(mapped.get("thumbnailFile"), dict)
                else None
            )
            if thumbnail_payload is None:
                stats["skipped"] += 1
                continue

            before_thumbnail_unique_id = _text(row["thumbnail_unique_id"])
            _ensure_thumbnail_download(
                td_manager,
                telegram_id=telegram_id,
                thumbnail_payload=thumbnail_payload,
            )
            normalized = _preserve_existing_identity(row, mapped)
            _upsert_tdlib_file_record(db, file_payload=normalized)
            current = db.execute(
                """
                SELECT thumbnail_unique_id
                FROM file_record
                WHERE telegram_id = ? AND id = ? AND unique_id = ?
                LIMIT 1
                """,
                (
                    telegram_id,
                    _int_or_default(row["id"], 0),
                    _text(row["unique_id"]),
                ),
            ).fetchone()
            after_thumbnail_unique_id = _text(
                current["thumbnail_unique_id"] if current is not None else ""
            )
            if not before_thumbnail_unique_id and after_thumbnail_unique_id:
                stats["updated"] += 1
            else:
                stats["skipped"] += 1
        except Exception as exc:
            logger.warning(
                "Thumbnail maintenance failed for telegram=%s chat=%s message=%s: %s",
                telegram_id,
                chat_id,
                message_id,
                exc,
            )
            stats["failed"] += 1

    return stats


def run_maintenance_backfills(
    db: sqlite3.Connection,
    td_manager: TdlibAuthManager,
    *,
    telegram_id: int,
    root_path: str,
    limit: int = 100,
    run_album: bool = True,
    run_thumbnail: bool = True,
) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    if run_album:
        result["album"] = run_album_metadata_backfill(
            db,
            td_manager,
            telegram_id=telegram_id,
            root_path=root_path,
            limit=limit,
        )
    if run_thumbnail:
        result["thumbnail"] = run_thumbnail_backfill(
            db,
            td_manager,
            telegram_id=telegram_id,
            root_path=root_path,
            limit=limit,
        )
    return result
