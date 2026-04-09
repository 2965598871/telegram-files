from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Callable


def _int_or_default(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def find_file_by_unique(
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


def find_file_by_identity(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int,
    unique_id: str,
) -> sqlite3.Row | None:
    if file_id <= 0 or not unique_id.strip():
        return None

    return db.execute(
        """
        SELECT *
        FROM file_record
        WHERE telegram_id = ? AND id = ? AND unique_id = ?
        ORDER BY message_id DESC
        LIMIT 1
        """,
        (telegram_id, file_id, unique_id.strip()),
    ).fetchone()


def _upsert_tdlib_thumbnail_record(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    chat_id: int,
    message_id: int,
    date: int,
    thumbnail_payload: dict[str, Any],
) -> str:
    unique_id = str(thumbnail_payload.get("uniqueId") or "").strip()
    if not unique_id:
        return ""

    file_id = _int_or_default(thumbnail_payload.get("id"), 0)
    mime_type = str(thumbnail_payload.get("mimeType") or "image/jpeg")
    size = _int_or_default(thumbnail_payload.get("size"), 0)
    downloaded_size = _int_or_default(thumbnail_payload.get("downloadedSize"), 0)
    local_path = str(thumbnail_payload.get("localPath") or "").strip()
    download_status = str(thumbnail_payload.get("downloadStatus") or "idle")
    if local_path:
        download_status = "completed"

    completion_date = int(time.time() * 1000) if download_status == "completed" else 0
    completion_value: int | None = completion_date if completion_date > 0 else None

    extra_payload = thumbnail_payload.get("extra")
    extra_json = (
        json.dumps(extra_payload, separators=(",", ":"), ensure_ascii=False)
        if extra_payload is not None
        else None
    )

    existing = find_file_by_unique(
        db,
        telegram_id=telegram_id,
        unique_id=unique_id,
    )
    if existing is not None:
        existing_status = str(existing["download_status"] or "idle")
        if existing_status == "completed" and download_status != "completed":
            download_status = existing_status
            local_path = str(existing["local_path"] or local_path)
            completion_value = _int_or_default(existing["completion_date"], 0) or None
        if not local_path and str(existing["local_path"] or ""):
            local_path = str(existing["local_path"] or "")
            download_status = "completed"
            completion_value = _int_or_default(existing["completion_date"], 0) or None

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
                file_id,
                unique_id,
                telegram_id,
                chat_id,
                message_id,
                0,
                date,
                0,
                size,
                downloaded_size,
                "thumbnail",
                mime_type,
                f"thumbnail_{message_id}_{file_id}",
                "",
                None,
                "",
                extra_json,
                local_path,
                download_status,
                "idle",
                0,
                completion_value,
                None,
                0,
                0,
                0,
            ),
        )
    else:
        db.execute(
            """
            UPDATE file_record
            SET id = ?,
                chat_id = ?,
                message_id = ?,
                date = ?,
                size = ?,
                downloaded_size = ?,
                type = 'thumbnail',
                mime_type = ?,
                file_name = ?,
                extra = ?,
                local_path = ?,
                download_status = ?,
                transfer_status = 'idle',
                completion_date = ?
            WHERE telegram_id = ? AND unique_id = ?
            """,
            (
                file_id,
                chat_id,
                message_id,
                date,
                size,
                downloaded_size,
                mime_type,
                f"thumbnail_{message_id}_{file_id}",
                extra_json,
                local_path,
                download_status,
                completion_value,
                telegram_id,
                unique_id,
            ),
        )

    return unique_id


def upsert_tdlib_file_record(
    db: sqlite3.Connection,
    *,
    file_payload: dict[str, Any],
) -> None:
    telegram_id = _int_or_default(file_payload.get("telegramId"), 0)
    file_id = _int_or_default(file_payload.get("id"), 0)
    unique_id = str(file_payload.get("uniqueId") or "").strip()
    if telegram_id <= 0 or not unique_id:
        return

    existing = find_file_by_identity(
        db,
        telegram_id=telegram_id,
        file_id=file_id,
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
    thumbnail_payload = (
        file_payload.get("thumbnailFile")
        if isinstance(file_payload.get("thumbnailFile"), dict)
        else None
    )
    thumbnail_unique_id = (
        str(thumbnail_payload.get("uniqueId") or "").strip()
        if thumbnail_payload is not None
        else ""
    )

    payload_values = {
        "id": file_id,
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
        "thumbnail_unique_id": thumbnail_unique_id or None,
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
        if (
            not payload_values["thumbnail_unique_id"]
            and str(existing["thumbnail_unique_id"] or "").strip()
        ):
            payload_values["thumbnail_unique_id"] = str(existing["thumbnail_unique_id"])
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
                thumbnail_unique_id = ?,
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
            WHERE telegram_id = ? AND id = ? AND unique_id = ?
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
                payload_values["thumbnail_unique_id"],
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
                payload_values["id"],
                unique_id,
            ),
        )

    if thumbnail_payload is not None:
        linked_unique_id = _upsert_tdlib_thumbnail_record(
            db,
            telegram_id=telegram_id,
            chat_id=payload_values["chat_id"],
            message_id=payload_values["message_id"],
            date=payload_values["date"],
            thumbnail_payload=thumbnail_payload,
        )
        if (
            linked_unique_id
            and payload_values["thumbnail_unique_id"] != linked_unique_id
        ):
            db.execute(
                """
                UPDATE file_record
                SET thumbnail_unique_id = ?
                WHERE telegram_id = ? AND id = ? AND unique_id = ?
                """,
                (linked_unique_id, telegram_id, payload_values["id"], unique_id),
            )

    db.commit()


def update_tdlib_file_status(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int,
    unique_id: str,
    status_payload: dict[str, Any],
    on_completed: Callable[[sqlite3.Connection, int, int, str], None] | None = None,
) -> None:
    target = None
    normalized_unique = unique_id.strip()
    if normalized_unique and file_id > 0:
        target = find_file_by_identity(
            db,
            telegram_id=telegram_id,
            file_id=file_id,
            unique_id=normalized_unique,
        )
    if target is None and normalized_unique:
        target = find_file_by_unique(
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
        WHERE telegram_id = ? AND id = ? AND unique_id = ?
        """,
        (
            _int_or_default(status_payload.get("downloadedSize"), 0),
            download_status,
            local_path,
            completion_value,
            telegram_id,
            _int_or_default(target["id"], 0),
            resolved_unique,
        ),
    )
    db.commit()

    if (
        on_completed is not None
        and download_status.strip().lower() == "completed"
        and local_path.strip()
    ):
        on_completed(db, telegram_id, _int_or_default(target["id"], 0), resolved_unique)


def count_downloading_files(db: sqlite3.Connection, telegram_id: int) -> int:
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


def transfer_candidates(
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


def file_for_transfer(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int = 0,
    unique_id: str,
) -> sqlite3.Row | None:
    row = find_file_by_identity(
        db,
        telegram_id=telegram_id,
        file_id=file_id,
        unique_id=unique_id,
    )
    if row is not None:
        return row

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


def update_transfer_status(
    db: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int = 0,
    unique_id: str,
    transfer_status: str,
    local_path: str | None = None,
) -> dict[str, Any] | None:
    row = file_for_transfer(
        db,
        telegram_id=telegram_id,
        file_id=file_id,
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
        WHERE telegram_id = ? AND id = ? AND unique_id = ?
        """,
        (
            transfer_status,
            next_local_path,
            telegram_id,
            _int_or_default(row["id"], 0),
            unique_id,
        ),
    )
    db.commit()

    return {
        "fileId": _int_or_default(row["id"], 0),
        "uniqueId": unique_id,
        "transferStatus": transfer_status,
        "localPath": next_local_path,
    }
