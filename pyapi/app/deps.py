from __future__ import annotations

import sqlite3

from fastapi import Request


def get_db(request: Request) -> sqlite3.Connection:
    return request.app.state.db
