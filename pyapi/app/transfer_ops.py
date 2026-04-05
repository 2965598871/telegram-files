from __future__ import annotations

import hashlib
import shutil
import sqlite3
from pathlib import Path
from typing import Any


def _int_or_default(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _file_md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as source:
        while True:
            block = source.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    index = 1
    while True:
        candidate = parent / f"{stem}-{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def _transfer_target_path(row: sqlite3.Row, rule: dict[str, Any]) -> Path:
    destination = str(rule.get("destination") or "").strip()
    if not destination:
        raise RuntimeError("transfer destination is required")

    source_path = Path(str(row["local_path"] or "")).resolve()
    base_name = source_path.name
    transfer_policy = str(rule.get("transferPolicy") or "GROUP_BY_CHAT").upper()

    root = Path(destination)
    if transfer_policy == "DIRECT":
        return root / base_name
    if transfer_policy == "GROUP_BY_CHAT":
        return (
            root
            / str(_int_or_default(row["telegram_id"], 0))
            / str(_int_or_default(row["chat_id"], 0))
            / base_name
        )
    if transfer_policy == "GROUP_BY_TYPE":
        return root / str(row["type"] or "file") / base_name

    raise RuntimeError(f"unsupported transfer policy: {transfer_policy}")


def execute_transfer(
    row: sqlite3.Row,
    rule: dict[str, Any],
) -> tuple[str, str | None]:
    source_path = Path(str(row["local_path"] or "")).resolve()
    if not source_path.exists() or not source_path.is_file():
        raise RuntimeError("source file not found")

    target = _transfer_target_path(row, rule).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)

    duplication_policy = str(rule.get("duplicationPolicy") or "OVERWRITE").upper()
    if target.exists():
        if duplication_policy == "SKIP":
            return "idle", str(source_path)
        if duplication_policy == "RENAME":
            target = _unique_path(target)
        elif duplication_policy == "HASH":
            if target.is_file() and _file_md5(source_path) == _file_md5(target):
                source_path.unlink(missing_ok=True)
                return "completed", str(target)
            target = _unique_path(target)
        elif duplication_policy == "OVERWRITE":
            if target.is_file():
                target.unlink(missing_ok=True)
        else:
            raise RuntimeError(f"unsupported duplication policy: {duplication_policy}")

    shutil.move(str(source_path), str(target))
    return "completed", str(target)
