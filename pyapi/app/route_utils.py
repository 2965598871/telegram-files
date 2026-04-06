from __future__ import annotations

from typing import Any
from urllib.parse import unquote

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse


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


def _int_or_default(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


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


def _method_error(message: str) -> JSONResponse:
    return JSONResponse(status_code=400, content={"error": message})


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
