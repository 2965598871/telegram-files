from __future__ import annotations

import json
import sqlite3
import time
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import AppConfig


def create_connection(config: AppConfig) -> sqlite3.Connection:
    if config.db_type != "sqlite":
        raise RuntimeError(
            f"DB_TYPE={config.db_type!r} is not supported by the Python backend yet. "
            "Only sqlite is available in this migration phase."
        )

    conn = sqlite3.connect(config.sqlite_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS setting_record
        (
            key   VARCHAR(255) PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS telegram_record
        (
            id         BIGINT PRIMARY KEY,
            first_name VARCHAR(255),
            root_path  VARCHAR(255),
            proxy      VARCHAR(255)
        );

        CREATE TABLE IF NOT EXISTS chat_group_record
        (
            id            VARCHAR(255) PRIMARY KEY,
            telegram_id   BIGINT      NOT NULL,
            name          VARCHAR(255) NOT NULL,
            chat_ids      TEXT        NOT NULL,
            auto_settings TEXT,
            created_at    BIGINT      NOT NULL
        );

        CREATE TABLE IF NOT EXISTS file_record
        (
            id                    INT,
            unique_id             VARCHAR(255),
            telegram_id           BIGINT,
            chat_id               BIGINT,
            message_id            BIGINT,
            media_album_id        BIGINT,
            date                  INT,
            has_sensitive_content BOOLEAN,
            size                  BIGINT,
            downloaded_size       BIGINT,
            type                  VARCHAR(255),
            mime_type             VARCHAR(255),
            file_name             VARCHAR(255),
            thumbnail             VARCHAR(2056),
            thumbnail_unique_id   VARCHAR(255),
            caption               VARCHAR(4096),
            extra                 VARCHAR(4096),
            local_path            VARCHAR(1024),
            download_status       VARCHAR(255),
            transfer_status       VARCHAR(255),
            start_date            BIGINT,
            completion_date       BIGINT,
            tags                  VARCHAR(2056),
            thread_chat_id        BIGINT,
            message_thread_id     BIGINT,
            reaction_count        BIGINT DEFAULT 0,
            PRIMARY KEY (id, unique_id)
        );

        CREATE TABLE IF NOT EXISTS statistic_record
        (
            related_id VARCHAR(255),
            type       VARCHAR(255),
            timestamp  BIGINT,
            data       TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_file_record_chat
            ON file_record (telegram_id, chat_id, type, message_id DESC);

        CREATE INDEX IF NOT EXISTS idx_file_record_chat_date
            ON file_record (telegram_id, chat_id, type, date DESC, message_id DESC);

        CREATE INDEX IF NOT EXISTS idx_file_record_album
            ON file_record (telegram_id, chat_id, media_album_id, type);

        CREATE INDEX IF NOT EXISTS idx_file_record_unique
            ON file_record (telegram_id, unique_id);

        CREATE INDEX IF NOT EXISTS idx_chat_group_record_telegram
            ON chat_group_record (telegram_id, created_at DESC);
        """
    )
    conn.commit()


def upsert_settings(conn: sqlite3.Connection, values: dict[str, str]) -> None:
    conn.executemany(
        """
        INSERT INTO setting_record(key, value)
        VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        list(values.items()),
    )
    conn.commit()


def get_settings_by_keys(conn: sqlite3.Connection, keys: list[str]) -> dict[str, Any]:
    if not keys:
        return {}

    placeholders = ", ".join(["?"] * len(keys))
    rows = conn.execute(
        f"SELECT key, value FROM setting_record WHERE key IN ({placeholders})",
        keys,
    ).fetchall()
    return {str(row["key"]): row["value"] for row in rows}


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if value is None:
        return False
    return str(value).lower() in {"1", "true", "yes", "on"}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _size_to_bytes(size: int, unit: str) -> int:
    factors = {
        "KB": 1024,
        "MB": 1024 * 1024,
        "GB": 1024 * 1024 * 1024,
    }
    factor = factors.get(unit.upper(), 1)
    return size * factor


def _parse_extra(extra: Any) -> Any:
    if extra is None:
        return None
    text = str(extra).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _serialize_file_row(
    row: sqlite3.Row,
    thumbnail_row: sqlite3.Row | None,
    album_caption: str = "",
) -> dict[str, Any]:
    date_seconds = _safe_int(row["date"], 0)
    completion_date = row["completion_date"]
    thumbnail_file: dict[str, Any] | None = None
    if thumbnail_row is not None and (
        str(thumbnail_row["download_status"] or "") == "completed"
        or str(thumbnail_row["local_path"] or "").strip() != ""
    ):
        thumbnail_file = {
            "uniqueId": str(thumbnail_row["unique_id"] or ""),
            "mimeType": str(thumbnail_row["mime_type"] or ""),
            "extra": _parse_extra(thumbnail_row["extra"]),
        }

    return {
        "id": _safe_int(row["id"]),
        "telegramId": _safe_int(row["telegram_id"]),
        "uniqueId": str(row["unique_id"] or ""),
        "messageId": _safe_int(row["message_id"]),
        "mediaAlbumId": _safe_int(row["media_album_id"]),
        "chatId": _safe_int(row["chat_id"]),
        "fileName": str(row["file_name"] or ""),
        "type": str(row["type"] or "file"),
        "mimeType": str(row["mime_type"] or ""),
        "size": _safe_int(row["size"]),
        "downloadedSize": _safe_int(row["downloaded_size"]),
        "thumbnail": row["thumbnail"],
        "thumbnailFile": thumbnail_file,
        "downloadStatus": str(row["download_status"] or "idle"),
        "date": date_seconds,
        "formatDate": (
            datetime.fromtimestamp(date_seconds).strftime("%Y-%m-%d %H:%M:%S")
            if date_seconds > 0
            else ""
        ),
        "caption": str(row["caption"] or "") or album_caption,
        "localPath": str(row["local_path"] or ""),
        "hasSensitiveContent": _to_bool(row["has_sensitive_content"]),
        "startDate": _safe_int(row["start_date"]),
        "completionDate": _safe_int(completion_date),
        "originalDeleted": False,
        "transferStatus": str(row["transfer_status"] or "idle"),
        "extra": _parse_extra(row["extra"]),
        "tags": row["tags"],
        "alreadyDownloaded": (
            str(row["download_status"] or "").strip().lower() == "completed"
            and str(row["local_path"] or "").strip() != ""
        ),
        "loaded": True,
        "threadChatId": _safe_int(row["thread_chat_id"]),
        "messageThreadId": _safe_int(row["message_thread_id"]),
        "hasReply": False,
        "reactionCount": _safe_int(row["reaction_count"]),
    }


def _load_album_caption_map(
    conn: sqlite3.Connection,
    rows: list[sqlite3.Row],
    *,
    telegram_id: int | None,
) -> dict[tuple[int, int, int], str]:
    album_keys = {
        (
            _safe_int(row["telegram_id"]),
            _safe_int(row["chat_id"]),
            _safe_int(row["media_album_id"]),
        )
        for row in rows
        if _safe_int(row["media_album_id"]) != 0
    }
    if not album_keys:
        return {}

    album_ids = sorted({album_id for _, _, album_id in album_keys})
    placeholders = ", ".join(["?"] * len(album_ids))
    query = (
        "SELECT telegram_id, chat_id, media_album_id, caption "
        "FROM file_record "
        f"WHERE media_album_id IN ({placeholders}) "
        "AND type != 'thumbnail' "
        "AND TRIM(COALESCE(caption, '')) != ''"
    )
    params: list[Any] = list(album_ids)
    if telegram_id is not None and telegram_id != -1:
        query += " AND telegram_id = ?"
        params.append(telegram_id)

    album_caption_map: dict[tuple[int, int, int], str] = {}
    for caption_row in conn.execute(query, params).fetchall():
        key = (
            _safe_int(caption_row["telegram_id"]),
            _safe_int(caption_row["chat_id"]),
            _safe_int(caption_row["media_album_id"]),
        )
        if key not in album_keys or key in album_caption_map:
            continue
        album_caption_map[key] = str(caption_row["caption"] or "")
    return album_caption_map


def list_files(
    conn: sqlite3.Connection,
    *,
    telegram_id: int | None,
    chat_id: int,
    chat_ids: list[int] | None = None,
    filters: dict[str, str],
) -> dict[str, Any]:
    where_clauses = ["type != 'thumbnail'"]
    params: list[Any] = []

    if telegram_id is not None and telegram_id != -1:
        where_clauses.append("telegram_id = ?")
        params.append(telegram_id)

    normalized_chat_ids = sorted({chat for chat in (chat_ids or []) if chat != 0})
    if normalized_chat_ids:
        placeholders = ", ".join(["?"] * len(normalized_chat_ids))
        where_clauses.append(f"chat_id IN ({placeholders})")
        params.extend(normalized_chat_ids)
    elif chat_id not in (0, -1):
        where_clauses.append("chat_id = ?")
        params.append(chat_id)

    search = (filters.get("search") or "").strip()
    if search:
        where_clauses.append(
            "(file_name LIKE ? OR caption LIKE ? OR ("
            "media_album_id != 0 AND EXISTS ("
            "SELECT 1 FROM file_record AS a "
            "WHERE a.telegram_id = file_record.telegram_id "
            "AND a.chat_id = file_record.chat_id "
            "AND a.media_album_id = file_record.media_album_id "
            "AND a.type != 'thumbnail' "
            "AND a.caption LIKE ?)))"
        )
        params.append(f"%{search}%")
        params.append(f"%{search}%")
        params.append(f"%{search}%")

    file_type = (filters.get("type") or "").strip()
    if file_type and file_type != "all":
        if file_type == "media":
            where_clauses.append("type IN ('photo', 'video')")
        else:
            where_clauses.append("type = ?")
            params.append(file_type)

    download_status = (filters.get("downloadStatus") or "").strip()
    if download_status:
        where_clauses.append("download_status = ?")
        params.append(download_status)

    transfer_status = (filters.get("transferStatus") or "").strip()
    if transfer_status:
        where_clauses.append("transfer_status = ?")
        params.append(transfer_status)

    already_downloaded = _to_bool(filters.get("alreadyDownloaded"))
    if already_downloaded is not None:
        condition = (
            "download_status = 'completed' AND TRIM(COALESCE(local_path, '')) != ''"
        )
        where_clauses.append(condition if already_downloaded else f"NOT ({condition})")

    tags = [
        tag.strip() for tag in (filters.get("tags") or "").split(",") if tag.strip()
    ]
    if tags:
        tag_clauses = []
        for tag in tags:
            tag_clauses.append("tags LIKE ?")
            params.append(f"%{tag}%")
        where_clauses.append(f"({' OR '.join(tag_clauses)})")

    message_thread_id = _safe_int(filters.get("messageThreadId"), 0)
    if message_thread_id != 0:
        where_clauses.append("message_thread_id = ?")
        params.append(message_thread_id)

    date_type = (filters.get("dateType") or "").strip()
    date_range = (filters.get("dateRange") or "").strip()
    if date_type and date_range:
        dates = [part.strip() for part in date_range.split(",")]
        if len(dates) == 2 and dates[0] and dates[1]:
            try:
                start = datetime.fromisoformat(dates[0])
                end = datetime.fromisoformat(dates[1])
                start_ms = int(
                    start.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
                    * 1000
                )
                end_ms = int(
                    end.replace(
                        hour=23, minute=59, second=59, microsecond=999000
                    ).timestamp()
                    * 1000
                )
                if date_type == "sent":
                    where_clauses.append("date >= ? AND date <= ?")
                    params.append(start_ms // 1000)
                    params.append(end_ms // 1000)
                else:
                    where_clauses.append(
                        "completion_date >= ? AND completion_date <= ?"
                    )
                    params.append(start_ms)
                    params.append(end_ms)
            except ValueError:
                pass

    size_range = (filters.get("sizeRange") or "").strip()
    size_unit = (filters.get("sizeUnit") or "").strip()
    if size_range and size_unit:
        sizes = [part.strip() for part in size_range.split(",")]
        if len(sizes) == 2:
            min_size = _safe_int(sizes[0], -1)
            max_size = _safe_int(sizes[1], -1)
            if min_size >= 0 and max_size >= 0:
                where_clauses.append("size >= ? AND size <= ?")
                params.append(_size_to_bytes(min_size, size_unit))
                params.append(_size_to_bytes(max_size, size_unit))

    sort = (filters.get("sort") or "").strip()
    order = (filters.get("order") or "desc").strip().lower()
    sort_columns = {
        "date": "date",
        "completion_date": "completion_date",
        "size": "size",
        "reaction_count": "reaction_count",
    }
    custom_sort = sort in sort_columns and order in {"asc", "desc"}
    sort_column = sort_columns.get(sort, "message_id")

    order_by = "message_id DESC"
    if custom_sort:
        order_by = f"{sort_column} {order.upper()}, message_id DESC"
        if sort_column == "completion_date":
            where_clauses.append("completion_date IS NOT NULL")

    from_message_id = _safe_int(filters.get("fromMessageId"), 0)
    if from_message_id > 0:
        if custom_sort:
            from_sort_field = _safe_int(filters.get("fromSortField"), 0)
            comparator = ">" if order == "asc" else "<"
            where_clauses.append(
                f"({sort_column} {comparator} ? OR ({sort_column} = ? AND message_id < ?))"
            )
            params.append(from_sort_field)
            params.append(from_sort_field)
            params.append(from_message_id)
        else:
            where_clauses.append("message_id < ?")
            params.append(from_message_id)

    limit = _safe_int(filters.get("limit"), 20)
    if limit <= 0:
        limit = 20
    if limit > 200:
        limit = 200

    where_sql = " AND ".join(where_clauses)
    rows = conn.execute(
        f"SELECT * FROM file_record WHERE {where_sql} ORDER BY {order_by} LIMIT ?",
        [*params, limit + 1],
    ).fetchall()
    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]

    thumbnail_ids = [
        str(row["thumbnail_unique_id"])
        for row in rows
        if row["thumbnail_unique_id"] is not None
        and str(row["thumbnail_unique_id"]).strip()
    ]
    thumbnail_map: dict[str, sqlite3.Row] = {}
    if thumbnail_ids:
        placeholders = ", ".join(["?"] * len(thumbnail_ids))
        if telegram_id is not None and telegram_id != -1:
            thumbnail_rows = conn.execute(
                f"SELECT * FROM file_record WHERE telegram_id = ? AND unique_id IN ({placeholders})",
                [telegram_id, *thumbnail_ids],
            ).fetchall()
        else:
            thumbnail_rows = conn.execute(
                f"SELECT * FROM file_record WHERE unique_id IN ({placeholders})",
                thumbnail_ids,
            ).fetchall()
        thumbnail_map = {str(t_row["unique_id"]): t_row for t_row in thumbnail_rows}

    album_caption_map = _load_album_caption_map(
        conn,
        rows,
        telegram_id=telegram_id,
    )

    files = [
        _serialize_file_row(
            row,
            thumbnail_map.get(str(row["thumbnail_unique_id"] or "")),
            album_caption_map.get(
                (
                    _safe_int(row["telegram_id"]),
                    _safe_int(row["chat_id"]),
                    _safe_int(row["media_album_id"]),
                ),
                "",
            ),
        )
        for row in rows
    ]
    next_from_message_id = files[-1]["messageId"] if has_more and files else 0
    return {
        "files": files,
        "count": 1_000_000_000 if has_more and next_from_message_id > 0 else len(files),
        "size": len(files),
        "nextFromMessageId": next_from_message_id,
    }


def count_files_by_type(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    chat_id: int,
    chat_ids: list[int] | None = None,
) -> dict[str, int]:
    where_clauses = ["type != 'thumbnail'"]
    params: list[Any] = []

    if telegram_id != -1:
        where_clauses.append("telegram_id = ?")
        params.append(telegram_id)

    normalized_chat_ids = sorted({chat for chat in (chat_ids or []) if chat != 0})
    if normalized_chat_ids:
        placeholders = ", ".join(["?"] * len(normalized_chat_ids))
        where_clauses.append(f"chat_id IN ({placeholders})")
        params.extend(normalized_chat_ids)
    elif chat_id != -1:
        where_clauses.append("chat_id = ?")
        params.append(chat_id)

    where_sql = " AND ".join(where_clauses)
    rows = conn.execute(
        f"SELECT type, COUNT(*) AS count FROM file_record WHERE {where_sql} GROUP BY type",
        params,
    ).fetchall()

    result: dict[str, int] = {
        "media": 0,
        "photo": 0,
        "video": 0,
        "audio": 0,
        "file": 0,
    }
    for row in rows:
        row_type = str(row["type"] or "")
        row_count = _safe_int(row["count"])
        if row_type in result:
            result[row_type] = row_count

    result["media"] = result["photo"] + result["video"]
    return result


def get_files_count(conn: sqlite3.Connection) -> dict[str, int]:
    row = conn.execute(
        """
        SELECT COUNT(CASE WHEN download_status = 'downloading' THEN 1 END) AS downloading,
               COUNT(CASE WHEN download_status = 'completed' THEN 1 END) AS completed,
               SUM(CASE WHEN download_status = 'completed' THEN size ELSE 0 END) AS downloaded_size
        FROM file_record
        WHERE type != 'thumbnail'
        """
    ).fetchone()
    if row is None:
        return {
            "downloading": 0,
            "completed": 0,
            "downloadedSize": 0,
        }

    return {
        "downloading": _safe_int(row["downloading"]),
        "completed": _safe_int(row["completed"]),
        "downloadedSize": _safe_int(row["downloaded_size"]),
    }


def update_file_tags(conn: sqlite3.Connection, unique_id: str, tags: str) -> None:
    conn.execute(
        "UPDATE file_record SET tags = ? WHERE unique_id = ?",
        (tags, unique_id),
    )
    conn.commit()


def update_files_tags(
    conn: sqlite3.Connection, unique_ids: list[str], tags: str
) -> None:
    if not unique_ids:
        return
    conn.executemany(
        "UPDATE file_record SET tags = ? WHERE unique_id = ?",
        [(tags, unique_id) for unique_id in unique_ids],
    )
    conn.commit()


def _row_to_file_status_payload(
    row: sqlite3.Row,
    *,
    removed: bool = False,
) -> dict[str, Any]:
    return {
        "fileId": _safe_int(row["id"]),
        "uniqueId": str(row["unique_id"] or ""),
        "downloadStatus": str(row["download_status"] or "idle"),
        "localPath": str(row["local_path"] or ""),
        "completionDate": _safe_int(row["completion_date"]),
        "downloadedSize": _safe_int(row["downloaded_size"]),
        "transferStatus": str(row["transfer_status"] or "idle"),
        "removed": removed,
    }


def _find_file_by_unique(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    unique_id: str,
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT *
        FROM file_record
        WHERE telegram_id = ? AND unique_id = ?
        LIMIT 1
        """,
        (telegram_id, unique_id),
    ).fetchone()


def _find_file_by_file_id(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int,
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT *
        FROM file_record
        WHERE telegram_id = ? AND id = ? AND type != 'thumbnail'
        ORDER BY message_id DESC
        LIMIT 1
        """,
        (telegram_id, file_id),
    ).fetchone()


def start_file_download(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    chat_id: int,
    message_id: int,
    file_id: int,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT *
        FROM file_record
        WHERE telegram_id = ?
          AND chat_id = ?
          AND message_id = ?
          AND id = ?
          AND type != 'thumbnail'
        ORDER BY message_id DESC
        LIMIT 1
        """,
        (telegram_id, chat_id, message_id, file_id),
    ).fetchone()
    if row is None:
        return None

    now_ms = int(time.time() * 1000)
    conn.execute(
        """
        UPDATE file_record
        SET download_status = 'downloading',
            start_date = ?,
            completion_date = NULL,
            local_path = CASE WHEN local_path IS NULL THEN '' ELSE local_path END
        WHERE telegram_id = ? AND unique_id = ?
        """,
        (now_ms, telegram_id, str(row["unique_id"])),
    )
    conn.commit()

    updated = _find_file_by_unique(
        conn,
        telegram_id=telegram_id,
        unique_id=str(row["unique_id"]),
    )
    if updated is None:
        return None
    return _serialize_file_row(updated, None)


def cancel_file_download(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int,
    unique_id: str | None = None,
) -> dict[str, Any] | None:
    row = None
    if unique_id is not None and unique_id.strip():
        row = _find_file_by_unique(
            conn,
            telegram_id=telegram_id,
            unique_id=unique_id.strip(),
        )
    if row is None:
        row = _find_file_by_file_id(
            conn,
            telegram_id=telegram_id,
            file_id=file_id,
        )
    if row is None:
        return None

    unique = str(row["unique_id"])
    conn.execute(
        """
        UPDATE file_record
        SET download_status = 'idle',
            downloaded_size = 0,
            local_path = '',
            completion_date = NULL,
            transfer_status = 'idle'
        WHERE telegram_id = ? AND unique_id = ?
        """,
        (telegram_id, unique),
    )
    conn.commit()

    updated = _find_file_by_unique(conn, telegram_id=telegram_id, unique_id=unique)
    if updated is None:
        return None
    return _row_to_file_status_payload(updated)


def toggle_pause_file_download(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int,
    is_paused: bool | None,
    unique_id: str | None = None,
) -> dict[str, Any] | None:
    row = None
    if unique_id is not None and unique_id.strip():
        row = _find_file_by_unique(
            conn,
            telegram_id=telegram_id,
            unique_id=unique_id.strip(),
        )
    if row is None:
        row = _find_file_by_file_id(
            conn,
            telegram_id=telegram_id,
            file_id=file_id,
        )
    if row is None:
        return None

    current = str(row["download_status"] or "idle")
    if is_paused is None:
        target = "paused" if current == "downloading" else "downloading"
    else:
        target = "paused" if is_paused else "downloading"

    conn.execute(
        """
        UPDATE file_record
        SET download_status = ?
        WHERE telegram_id = ? AND unique_id = ?
        """,
        (target, telegram_id, str(row["unique_id"])),
    )
    conn.commit()

    updated = _find_file_by_unique(
        conn,
        telegram_id=telegram_id,
        unique_id=str(row["unique_id"]),
    )
    if updated is None:
        return None
    return _row_to_file_status_payload(updated)


def remove_file_download(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    file_id: int,
    unique_id: str | None = None,
) -> dict[str, Any] | None:
    row = None
    if unique_id is not None and unique_id.strip():
        row = _find_file_by_unique(
            conn,
            telegram_id=telegram_id,
            unique_id=unique_id.strip(),
        )
    if row is None:
        row = _find_file_by_file_id(
            conn,
            telegram_id=telegram_id,
            file_id=file_id,
        )
    if row is None:
        return None

    unique = str(row["unique_id"])
    conn.execute(
        """
        UPDATE file_record
        SET download_status = 'idle',
            downloaded_size = 0,
            local_path = '',
            completion_date = NULL,
            transfer_status = 'idle'
        WHERE telegram_id = ? AND unique_id = ?
        """,
        (telegram_id, unique),
    )
    conn.commit()

    updated = _find_file_by_unique(conn, telegram_id=telegram_id, unique_id=unique)
    if updated is None:
        return None
    return _row_to_file_status_payload(updated, removed=True)


def get_file_preview_info(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    unique_id: str,
) -> dict[str, Any] | None:
    row = _find_file_by_unique(conn, telegram_id=telegram_id, unique_id=unique_id)
    if row is None:
        return None
    local_path = str(row["local_path"] or "").strip()
    if not local_path:
        return None
    return {
        "path": local_path,
        "mimeType": str(row["mime_type"] or "application/octet-stream"),
    }


def _default_auto_settings() -> dict[str, Any]:
    return {
        "preload": {
            "enabled": False,
        },
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


def _load_automation_items(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    row = conn.execute(
        "SELECT value FROM setting_record WHERE key = 'automation' LIMIT 1"
    ).fetchone()
    if row is None:
        return []

    raw = row["value"]
    if raw is None or str(raw).strip() == "":
        return []

    try:
        parsed = json.loads(str(raw))
    except json.JSONDecodeError:
        return []

    if isinstance(parsed, dict) and isinstance(parsed.get("automations"), list):
        return [item for item in parsed["automations"] if isinstance(item, dict)]

    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]

    return []


def _save_automation_items(
    conn: sqlite3.Connection,
    items: list[dict[str, Any]],
) -> None:
    payload = json.dumps({"automations": items}, separators=(",", ":"))
    conn.execute(
        """
        INSERT INTO setting_record(key, value)
        VALUES('automation', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (payload,),
    )
    conn.commit()


def update_auto_settings(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    chat_id: int,
    auto_payload: dict[str, Any],
) -> None:
    normalized_payload = auto_payload if isinstance(auto_payload, dict) else {}
    automation = _deep_merge_dict(
        _default_auto_settings(),
        deepcopy(normalized_payload),
    )
    has_enabled = (
        bool(automation.get("preload", {}).get("enabled"))
        or bool(automation.get("download", {}).get("enabled"))
        or bool(automation.get("transfer", {}).get("enabled"))
    )

    items = _load_automation_items(conn)
    exists = any(
        _safe_int(item.get("telegramId"), 0) == telegram_id
        and _safe_int(item.get("chatId"), 0) == chat_id
        for item in items
    )

    if not has_enabled and not exists:
        return

    items = [
        item
        for item in items
        if not (
            _safe_int(item.get("telegramId"), 0) == telegram_id
            and _safe_int(item.get("chatId"), 0) == chat_id
        )
    ]

    if has_enabled:
        automation["telegramId"] = telegram_id
        automation["chatId"] = chat_id
        automation["state"] = _safe_int(automation.get("state"), 0)
        items.append(automation)

    _save_automation_items(conn, items)


def _deep_merge_dict(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge_dict(base[key], value)
        else:
            base[key] = value
    return base


def _load_automation_map(
    conn: sqlite3.Connection,
) -> dict[tuple[int, int], dict[str, Any]]:
    items = _load_automation_items(conn)
    if not items:
        return {}

    result: dict[tuple[int, int], dict[str, Any]] = {}
    for item in items:
        telegram_id = _safe_int(item.get("telegramId"), 0)
        chat_id = _safe_int(item.get("chatId"), 0)
        if telegram_id <= 0 or chat_id == 0:
            continue
        merged = _deep_merge_dict(_default_auto_settings(), deepcopy(item))
        merged["state"] = _safe_int(merged.get("state"), 0)
        result[(telegram_id, chat_id)] = merged
    return result


def get_automation_map(
    conn: sqlite3.Connection,
    *,
    telegram_id: int | None = None,
) -> dict[tuple[int, int], dict[str, Any]]:
    source = _load_automation_map(conn)
    if telegram_id is None:
        return deepcopy(source)
    return {
        key: deepcopy(value) for key, value in source.items() if key[0] == telegram_id
    }


def _normalize_chat_group_name(value: Any) -> str:
    return str(value or "").strip()


def _normalize_chat_group_chat_ids(value: Any) -> list[int]:
    if not isinstance(value, list):
        return []

    result: list[int] = []
    seen: set[int] = set()
    for item in value:
        chat_id = _safe_int(item, 0)
        if chat_id == 0 or chat_id in seen:
            continue
        seen.add(chat_id)
        result.append(chat_id)
    return result


def _default_chat_group_progress() -> dict[str, Any]:
    return {
        "state": 0,
        "preload": {"nextFromMessageId": 0},
        "download": {"nextFileType": "", "nextFromMessageId": 0},
    }


def _coerce_chat_group_auto_settings(
    auto_payload: Any,
    *,
    chat_ids: list[int],
) -> dict[str, Any]:
    merged = _deep_merge_dict(
        _default_auto_settings(),
        deepcopy(auto_payload) if isinstance(auto_payload, dict) else {},
    )

    progress_map_raw = (
        merged.get("progressByChat")
        if isinstance(merged.get("progressByChat"), dict)
        else {}
    )
    progress_map: dict[str, Any] = {}
    aggregate_state = 0

    for bit in (1, 2, 3, 4):
        if chat_ids and all(
            (
                _safe_int(
                    (
                        progress_map_raw.get(str(chat_id), {}).get("state")
                        if isinstance(progress_map_raw.get(str(chat_id)), dict)
                        else 0
                    ),
                    0,
                )
                & (1 << bit)
            )
            != 0
            for chat_id in chat_ids
        ):
            aggregate_state |= 1 << bit

    for chat_id in chat_ids:
        raw_progress = progress_map_raw.get(str(chat_id))
        progress = _deep_merge_dict(
            _default_chat_group_progress(),
            deepcopy(raw_progress) if isinstance(raw_progress, dict) else {},
        )
        progress["state"] = _safe_int(progress.get("state"), 0)
        progress_map[str(chat_id)] = progress

    merged["progressByChat"] = progress_map
    merged["state"] = aggregate_state
    return merged


def _serialize_chat_group_row(row: sqlite3.Row) -> dict[str, Any]:
    group_id = str(row["id"] or "").strip()
    raw_chat_ids = str(row["chat_ids"] or "[]").strip()
    try:
        parsed_chat_ids = json.loads(raw_chat_ids) if raw_chat_ids else []
    except json.JSONDecodeError:
        parsed_chat_ids = []
    chat_ids = _normalize_chat_group_chat_ids(parsed_chat_ids)

    raw_auto = None
    auto_text = str(row["auto_settings"] or "").strip()
    if auto_text:
        try:
            raw_auto = json.loads(auto_text)
        except json.JSONDecodeError:
            raw_auto = None

    auto = _coerce_chat_group_auto_settings(raw_auto, chat_ids=chat_ids)
    return {
        "id": f"group:{group_id}",
        "groupId": group_id,
        "telegramId": str(_safe_int(row["telegram_id"])),
        "kind": "group",
        "name": _normalize_chat_group_name(row["name"]),
        "type": "group",
        "avatar": "",
        "unreadCount": 0,
        "lastMessage": "",
        "lastMessageTime": "",
        "chatIds": [str(chat_id) for chat_id in chat_ids],
        "memberCount": len(chat_ids),
        "auto": auto,
        "createdAt": _safe_int(row["created_at"]),
    }


def _find_chat_group_row(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    group_id: str,
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT *
        FROM chat_group_record
        WHERE telegram_id = ? AND id = ?
        LIMIT 1
        """,
        (telegram_id, group_id),
    ).fetchone()


def get_chat_group(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    group_id: str,
) -> dict[str, Any] | None:
    row = _find_chat_group_row(conn, telegram_id=telegram_id, group_id=group_id)
    if row is None:
        return None
    return _serialize_chat_group_row(row)


def list_chat_groups(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    query: str,
    activated_group_id: str | None,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM chat_group_record
        WHERE telegram_id = ?
        ORDER BY created_at DESC, name COLLATE NOCASE ASC
        """,
        (telegram_id,),
    ).fetchall()

    normalized_query = query.strip().lower()
    groups: list[dict[str, Any]] = []
    activated_group = None
    for row in rows:
        serialized = _serialize_chat_group_row(row)
        if activated_group_id and serialized["groupId"] == activated_group_id:
            activated_group = serialized
        if normalized_query and normalized_query not in serialized["name"].lower():
            continue
        groups.append(serialized)

    if activated_group is not None and not any(
        item["groupId"] == activated_group_id for item in groups
    ):
        groups.insert(0, activated_group)
    return groups


def list_chat_group_automations(
    conn: sqlite3.Connection,
    *,
    telegram_id: int | None = None,
) -> list[dict[str, Any]]:
    query = "SELECT * FROM chat_group_record"
    params: list[Any] = []
    if telegram_id is not None:
        query += " WHERE telegram_id = ?"
        params.append(telegram_id)
    query += " ORDER BY created_at ASC, name COLLATE NOCASE ASC"

    rows = conn.execute(query, params).fetchall()
    return [_serialize_chat_group_row(row) for row in rows]


def _chat_group_name_exists(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    name: str,
    exclude_group_id: str | None = None,
) -> bool:
    rows = conn.execute(
        "SELECT id, name FROM chat_group_record WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchall()
    normalized_name = name.lower()
    for row in rows:
        current_id = str(row["id"] or "").strip()
        if exclude_group_id is not None and current_id == exclude_group_id:
            continue
        if str(row["name"] or "").strip().lower() == normalized_name:
            return True
    return False


def _chat_group_members_overlap(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    chat_ids: list[int],
    exclude_group_id: str | None = None,
) -> str | None:
    if not chat_ids:
        return None

    target = set(chat_ids)
    rows = conn.execute(
        "SELECT id, name, chat_ids FROM chat_group_record WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchall()
    for row in rows:
        current_id = str(row["id"] or "").strip()
        if exclude_group_id is not None and current_id == exclude_group_id:
            continue
        raw_chat_ids = str(row["chat_ids"] or "[]").strip()
        try:
            parsed = json.loads(raw_chat_ids) if raw_chat_ids else []
        except json.JSONDecodeError:
            parsed = []
        existing = set(_normalize_chat_group_chat_ids(parsed))
        if target & existing:
            return _normalize_chat_group_name(row["name"])
    return None


def create_chat_group(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    group_id: str,
    name: str,
    chat_ids: list[int],
) -> dict[str, Any]:
    normalized_name = _normalize_chat_group_name(name)
    normalized_chat_ids = _normalize_chat_group_chat_ids(chat_ids)
    if not normalized_name:
        raise ValueError("Group name is required.")
    if len(normalized_chat_ids) < 2:
        raise ValueError("A group chat must contain at least 2 chats.")
    if _chat_group_name_exists(conn, telegram_id=telegram_id, name=normalized_name):
        raise ValueError("A group chat with this name already exists.")

    overlap_group = _chat_group_members_overlap(
        conn,
        telegram_id=telegram_id,
        chat_ids=normalized_chat_ids,
    )
    if overlap_group is not None:
        raise ValueError(f"One or more chats already belong to '{overlap_group}'.")

    created_at = int(time.time() * 1000)
    auto_payload = json.dumps(_default_auto_settings(), separators=(",", ":"))
    conn.execute(
        """
        INSERT INTO chat_group_record(id, telegram_id, name, chat_ids, auto_settings, created_at)
        VALUES(?, ?, ?, ?, ?, ?)
        """,
        (
            group_id,
            telegram_id,
            normalized_name,
            json.dumps(normalized_chat_ids, separators=(",", ":")),
            auto_payload,
            created_at,
        ),
    )
    conn.commit()

    created = get_chat_group(conn, telegram_id=telegram_id, group_id=group_id)
    if created is None:
        raise ValueError("Failed to create group chat.")
    return created


def update_chat_group(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    group_id: str,
    name: str,
    chat_ids: list[int],
) -> dict[str, Any] | None:
    existing = _find_chat_group_row(conn, telegram_id=telegram_id, group_id=group_id)
    if existing is None:
        return None

    normalized_name = _normalize_chat_group_name(name)
    normalized_chat_ids = _normalize_chat_group_chat_ids(chat_ids)
    if not normalized_name:
        raise ValueError("Group name is required.")
    if len(normalized_chat_ids) < 2:
        raise ValueError("A group chat must contain at least 2 chats.")
    if _chat_group_name_exists(
        conn,
        telegram_id=telegram_id,
        name=normalized_name,
        exclude_group_id=group_id,
    ):
        raise ValueError("A group chat with this name already exists.")

    overlap_group = _chat_group_members_overlap(
        conn,
        telegram_id=telegram_id,
        chat_ids=normalized_chat_ids,
        exclude_group_id=group_id,
    )
    if overlap_group is not None:
        raise ValueError(f"One or more chats already belong to '{overlap_group}'.")

    conn.execute(
        """
        UPDATE chat_group_record
        SET name = ?,
            chat_ids = ?
        WHERE telegram_id = ? AND id = ?
        """,
        (
            normalized_name,
            json.dumps(normalized_chat_ids, separators=(",", ":")),
            telegram_id,
            group_id,
        ),
    )
    conn.commit()
    return get_chat_group(conn, telegram_id=telegram_id, group_id=group_id)


def delete_chat_group(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    group_id: str,
) -> None:
    conn.execute(
        "DELETE FROM chat_group_record WHERE telegram_id = ? AND id = ?",
        (telegram_id, group_id),
    )
    conn.commit()


def update_chat_group_auto_settings(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    group_id: str,
    auto_payload: dict[str, Any],
) -> dict[str, Any] | None:
    existing = _find_chat_group_row(conn, telegram_id=telegram_id, group_id=group_id)
    if existing is None:
        return None

    payload = auto_payload if isinstance(auto_payload, dict) else {}
    conn.execute(
        """
        UPDATE chat_group_record
        SET auto_settings = ?
        WHERE telegram_id = ? AND id = ?
        """,
        (
            json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
            telegram_id,
            group_id,
        ),
    )
    conn.commit()
    return get_chat_group(conn, telegram_id=telegram_id, group_id=group_id)


def find_chat_group_for_chat(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    chat_id: int,
) -> dict[str, Any] | None:
    if telegram_id <= 0 or chat_id == 0:
        return None

    for group in list_chat_group_automations(conn, telegram_id=telegram_id):
        raw_chat_ids = group.get("chatIds")
        chat_ids = (
            {_safe_int(item, 0) for item in raw_chat_ids}
            if isinstance(raw_chat_ids, list)
            else set()
        )
        if chat_id in chat_ids:
            return group
    return None


def list_telegrams(
    conn: sqlite3.Connection, app_root: str, authorized: bool | None
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT id, first_name, root_path, proxy FROM telegram_record ORDER BY id"
    ).fetchall()

    result_map: dict[int, dict[str, Any]] = {}
    for row in rows:
        telegram_id = _safe_int(row["id"])
        if telegram_id <= 0:
            continue
        result_map[telegram_id] = {
            "id": str(telegram_id),
            "name": str(row["first_name"] or telegram_id),
            "phoneNumber": "",
            "avatar": "",
            "status": "active",
            "rootPath": str(
                row["root_path"] or (Path(app_root) / "account" / str(telegram_id))
            ),
            "isPremium": False,
            "proxy": row["proxy"],
        }

    file_rows = conn.execute(
        "SELECT DISTINCT telegram_id FROM file_record WHERE telegram_id IS NOT NULL ORDER BY telegram_id"
    ).fetchall()
    for row in file_rows:
        telegram_id = _safe_int(row["telegram_id"])
        if telegram_id <= 0 or telegram_id in result_map:
            continue
        result_map[telegram_id] = {
            "id": str(telegram_id),
            "name": str(telegram_id),
            "phoneNumber": "",
            "avatar": "",
            "status": "active",
            "rootPath": str(Path(app_root) / "account" / str(telegram_id)),
            "isPremium": False,
            "proxy": None,
        }

    accounts = [result_map[key] for key in sorted(result_map.keys())]
    if authorized is None:
        return accounts

    target_status = "active" if authorized else "inactive"
    return [account for account in accounts if account["status"] == target_status]


def get_telegram_account(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    app_root: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT id, first_name, root_path, proxy FROM telegram_record WHERE id = ? LIMIT 1",
        (telegram_id,),
    ).fetchone()
    if row is None:
        return None

    normalized_id = _safe_int(row["id"])
    if normalized_id <= 0:
        return None

    return {
        "id": str(normalized_id),
        "name": str(row["first_name"] or normalized_id),
        "rootPath": str(
            row["root_path"] or (Path(app_root) / "account" / str(normalized_id))
        ),
        "proxy": row["proxy"],
    }


def create_telegram_account(
    conn: sqlite3.Connection,
    *,
    app_root: str,
    first_name: str,
    proxy_name: str | None,
    phone_number: str,
    root_path: str | None = None,
) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT MAX(max_id) AS max_id
        FROM (
            SELECT COALESCE(MAX(id), 0) AS max_id FROM telegram_record
            UNION ALL
            SELECT COALESCE(MAX(telegram_id), 0) AS max_id FROM file_record
        )
        """
    ).fetchone()
    next_id = _safe_int(row["max_id"] if row else 0) + 1
    normalized_root_path = root_path or str(Path(app_root) / "account" / str(next_id))

    conn.execute(
        """
        INSERT INTO telegram_record(id, first_name, root_path, proxy)
        VALUES(?, ?, ?, ?)
        """,
        (next_id, first_name, normalized_root_path, proxy_name),
    )
    conn.commit()

    return {
        "id": str(next_id),
        "name": first_name,
        "phoneNumber": phone_number,
        "avatar": "",
        "status": "active",
        "rootPath": normalized_root_path,
        "isPremium": False,
        "proxy": proxy_name,
    }


def list_chats(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    query: str,
    activated_chat_id: int | None,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT chat_id, MAX(message_id) AS latest_message_id
        FROM file_record
        WHERE telegram_id = ?
        GROUP BY chat_id
        ORDER BY latest_message_id DESC
        LIMIT 100
        """,
        (telegram_id,),
    ).fetchall()

    auto_map = _load_automation_map(conn)
    normalized_query = query.strip().lower()

    chats: list[dict[str, Any]] = []
    seen: set[int] = set()
    for row in rows:
        chat_id = _safe_int(row["chat_id"])
        if chat_id == 0:
            continue
        chat_name = str(chat_id)
        if normalized_query and normalized_query not in chat_name.lower():
            continue
        auto = auto_map.get((telegram_id, chat_id), _default_auto_settings())
        chats.append(
            {
                "id": str(chat_id),
                "kind": "chat",
                "name": chat_name,
                "type": "channel",
                "avatar": "",
                "unreadCount": 0,
                "lastMessage": "",
                "lastMessageTime": "",
                "auto": auto,
            }
        )
        seen.add(chat_id)

    if (
        activated_chat_id is not None
        and activated_chat_id not in seen
        and activated_chat_id != 0
    ):
        row = conn.execute(
            """
            SELECT chat_id
            FROM file_record
            WHERE telegram_id = ? AND chat_id = ?
            LIMIT 1
            """,
            (telegram_id, activated_chat_id),
        ).fetchone()
        if row is not None:
            auto = auto_map.get(
                (telegram_id, activated_chat_id), _default_auto_settings()
            )
            chats.insert(
                0,
                {
                    "id": str(activated_chat_id),
                    "kind": "chat",
                    "name": str(activated_chat_id),
                    "type": "channel",
                    "avatar": "",
                    "unreadCount": 0,
                    "lastMessage": "",
                    "lastMessageTime": "",
                    "auto": auto,
                },
            )
    return chats


def delete_telegram(conn: sqlite3.Connection, telegram_id: int) -> None:
    conn.execute("DELETE FROM telegram_record WHERE id = ?", (telegram_id,))
    conn.commit()


def update_telegram_proxy(
    conn: sqlite3.Connection,
    *,
    telegram_id: int,
    proxy_name: str | None,
    app_root: str,
) -> str | None:
    current = conn.execute(
        "SELECT id FROM telegram_record WHERE id = ? LIMIT 1",
        (telegram_id,),
    ).fetchone()
    if current is None:
        conn.execute(
            """
            INSERT INTO telegram_record(id, first_name, root_path, proxy)
            VALUES(?, ?, ?, ?)
            """,
            (
                telegram_id,
                str(telegram_id),
                str(Path(app_root) / "account" / str(telegram_id)),
                proxy_name,
            ),
        )
    else:
        conn.execute(
            "UPDATE telegram_record SET proxy = ? WHERE id = ?",
            (proxy_name, telegram_id),
        )
    conn.commit()
    return proxy_name


def get_telegram_ping_seconds(conn: sqlite3.Connection, telegram_id: int) -> float:
    row = conn.execute(
        "SELECT proxy FROM telegram_record WHERE id = ? LIMIT 1",
        (telegram_id,),
    ).fetchone()
    if row is None or row["proxy"] is None or str(row["proxy"]).strip() == "":
        return 0.0
    return 0.08


def _interval_from_settings(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM setting_record WHERE key = 'avgSpeedInterval' LIMIT 1"
    ).fetchone()
    if row is None:
        return 300
    return _safe_int(row["value"], 300)


def get_telegram_download_statistics(
    conn: sqlite3.Connection, telegram_id: int
) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT COUNT(*) AS total,
               COUNT(CASE WHEN download_status = 'downloading' THEN 1 END) AS downloading,
               COUNT(CASE WHEN download_status = 'paused' THEN 1 END) AS paused,
               COUNT(CASE WHEN download_status = 'completed' THEN 1 END) AS completed,
               COUNT(CASE WHEN download_status = 'error' THEN 1 END) AS error,
               COUNT(CASE WHEN download_status = 'completed' AND type = 'photo' THEN 1 END) AS photo,
               COUNT(CASE WHEN download_status = 'completed' AND type = 'video' THEN 1 END) AS video,
               COUNT(CASE WHEN download_status = 'completed' AND type = 'audio' THEN 1 END) AS audio,
               COUNT(CASE WHEN download_status = 'completed' AND type = 'file' THEN 1 END) AS file
        FROM file_record
        WHERE telegram_id = ? AND type != 'thumbnail'
        """,
        (telegram_id,),
    ).fetchone()

    result = {
        "total": _safe_int(row["total"] if row else 0),
        "downloading": _safe_int(row["downloading"] if row else 0),
        "paused": _safe_int(row["paused"] if row else 0),
        "completed": _safe_int(row["completed"] if row else 0),
        "error": _safe_int(row["error"] if row else 0),
        "photo": _safe_int(row["photo"] if row else 0),
        "video": _safe_int(row["video"] if row else 0),
        "audio": _safe_int(row["audio"] if row else 0),
        "file": _safe_int(row["file"] if row else 0),
    }

    interval = _interval_from_settings(conn)
    speed_stats = {
        "interval": interval,
        "avgSpeed": 0,
        "medianSpeed": 0,
        "maxSpeed": 0,
        "minSpeed": 0,
    }
    speed_row = conn.execute(
        """
        SELECT data
        FROM statistic_record
        WHERE type = 'speed' AND related_id = ?
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (str(telegram_id),),
    ).fetchone()
    if speed_row is not None and speed_row["data"] is not None:
        try:
            speed_data = json.loads(str(speed_row["data"]))
            speed_stats.update(
                {
                    "avgSpeed": _safe_int(speed_data.get("avgSpeed"), 0),
                    "medianSpeed": _safe_int(speed_data.get("medianSpeed"), 0),
                    "maxSpeed": _safe_int(speed_data.get("maxSpeed"), 0),
                    "minSpeed": _safe_int(speed_data.get("minSpeed"), 0),
                }
            )
        except json.JSONDecodeError:
            pass

    result["networkStatistics"] = {
        "sinceDate": int(time.time()),
        "sentBytes": 0,
        "receivedBytes": 0,
    }
    result["speedStats"] = speed_stats
    return result


def _phase_start_end_millis(time_range: int) -> tuple[int, int]:
    end_time = int(time.time() * 1000)
    now = datetime.now()
    if time_range == 1:
        start = now.timestamp() - 3600
    elif time_range == 2:
        start = now.timestamp() - 24 * 3600
    elif time_range == 3:
        start = now.timestamp() - 7 * 24 * 3600
    elif time_range == 4:
        start = now.timestamp() - 30 * 24 * 3600
    else:
        start = now.timestamp() - 3600
    return int(start * 1000), end_time


def _speed_bucket_label(ts_millis: int, time_range: int) -> str:
    dt = datetime.fromtimestamp(ts_millis / 1000)
    if time_range == 1:
        minute = (dt.minute // 5) * 5
        bucket = dt.replace(minute=minute, second=0, microsecond=0)
        return bucket.strftime("%Y-%m-%d %H:%M")
    if time_range == 2:
        bucket = dt.replace(minute=0, second=0, microsecond=0)
        return bucket.strftime("%Y-%m-%d %H:%M")
    bucket = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return bucket.strftime("%Y-%m-%d")


def get_telegram_download_statistics_by_phase(
    conn: sqlite3.Connection,
    telegram_id: int,
    time_range: int,
) -> dict[str, Any]:
    start_time, end_time = _phase_start_end_millis(time_range)

    speed_rows = conn.execute(
        """
        SELECT timestamp, data
        FROM statistic_record
        WHERE type = 'speed'
          AND related_id = ?
          AND timestamp >= ?
          AND timestamp <= ?
        ORDER BY timestamp
        """,
        (str(telegram_id), start_time, end_time),
    ).fetchall()

    grouped_speed: dict[str, dict[str, int]] = {}
    for row in speed_rows:
        label = _speed_bucket_label(_safe_int(row["timestamp"]), time_range)
        try:
            payload = json.loads(str(row["data"] or "{}"))
        except json.JSONDecodeError:
            payload = {}
        agg = grouped_speed.setdefault(
            label,
            {
                "count": 0,
                "avgSpeed": 0,
                "medianSpeed": 0,
                "maxSpeed": 0,
                "minSpeed": 0,
            },
        )
        agg["count"] += 1
        agg["avgSpeed"] += _safe_int(payload.get("avgSpeed"), 0)
        agg["medianSpeed"] += _safe_int(payload.get("medianSpeed"), 0)
        agg["maxSpeed"] += _safe_int(payload.get("maxSpeed"), 0)
        agg["minSpeed"] += _safe_int(payload.get("minSpeed"), 0)

    speed_stats: list[dict[str, Any]] = []
    for label in sorted(grouped_speed.keys()):
        agg = grouped_speed[label]
        count = max(agg["count"], 1)
        speed_stats.append(
            {
                "time": label,
                "data": {
                    "avgSpeed": agg["avgSpeed"] // count,
                    "medianSpeed": agg["medianSpeed"] // count,
                    "maxSpeed": agg["maxSpeed"] // count,
                    "minSpeed": agg["minSpeed"] // count,
                },
            }
        )

    if time_range == 1:
        completed_rows = conn.execute(
            """
            SELECT strftime('%Y-%m-%d %H:%M', datetime(completion_date / 1000, 'unixepoch'), 'localtime') AS time,
                   COUNT(*) AS total
            FROM file_record
            WHERE telegram_id = ?
              AND completion_date IS NOT NULL
              AND completion_date >= ?
              AND completion_date <= ?
              AND type != 'thumbnail'
            GROUP BY time
            ORDER BY time
            """,
            (telegram_id, start_time, end_time),
        ).fetchall()
        grouped_completed: dict[str, int] = {}
        for row in completed_rows:
            try:
                minute_time = datetime.strptime(str(row["time"]), "%Y-%m-%d %H:%M")
            except ValueError:
                continue
            minute = (minute_time.minute // 5) * 5
            bucket = minute_time.replace(minute=minute)
            key = bucket.strftime("%Y-%m-%d %H:%M")
            grouped_completed[key] = grouped_completed.get(key, 0) + _safe_int(
                row["total"]
            )
        completed_stats = [
            {"time": key, "total": grouped_completed[key]}
            for key in sorted(grouped_completed.keys())
        ]
    elif time_range == 2:
        completed_rows = conn.execute(
            """
            SELECT strftime('%Y-%m-%d %H:00', datetime(completion_date / 1000, 'unixepoch'), 'localtime') AS time,
                   COUNT(*) AS total
            FROM file_record
            WHERE telegram_id = ?
              AND completion_date IS NOT NULL
              AND completion_date >= ?
              AND completion_date <= ?
              AND type != 'thumbnail'
            GROUP BY time
            ORDER BY time
            """,
            (telegram_id, start_time, end_time),
        ).fetchall()
        completed_stats = [
            {"time": str(row["time"]), "total": _safe_int(row["total"])}
            for row in completed_rows
        ]
    else:
        completed_rows = conn.execute(
            """
            SELECT strftime('%Y-%m-%d', datetime(completion_date / 1000, 'unixepoch'), 'localtime') AS time,
                   COUNT(*) AS total
            FROM file_record
            WHERE telegram_id = ?
              AND completion_date IS NOT NULL
              AND completion_date >= ?
              AND completion_date <= ?
              AND type != 'thumbnail'
            GROUP BY time
            ORDER BY time
            """,
            (telegram_id, start_time, end_time),
        ).fetchall()
        completed_stats = [
            {"time": str(row["time"]), "total": _safe_int(row["total"])}
            for row in completed_rows
        ]

    return {
        "speedStats": speed_stats,
        "completedStats": completed_stats,
    }
