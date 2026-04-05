# Python Backend (Migration Phase)

This directory contains the first phase of the backend migration from Java (Vert.x) to Python (FastAPI).

## Current status

- Implemented
  - `GET /`
  - `GET /health`
  - `GET /version`
  - `GET /settings`
  - `POST /settings/create`
  - `GET /telegrams`
  - `POST /telegrams/change`
  - `POST /telegram/create`
  - `POST /telegram/{telegramId}/delete`
  - `GET /telegram/{telegramId}/chats`
  - `GET /telegram/{telegramId}/download-statistics`
  - `POST /telegram/{telegramId}/toggle-proxy`
  - `GET /telegram/{telegramId}/ping`
  - `GET /telegram/{telegramId}/test-network`
  - `GET /telegram/api/methods`
  - `GET /telegram/api/{method}/parameters`
  - `POST /telegram/api/{method}`
  - `WS /ws` (authorization + method-result events)
  - `GET /files`
  - `GET /files/count`
  - `GET /telegram/{telegramId}/chat/{chatId}/files`
  - `GET /telegram/{telegramId}/chat/{chatId}/files/count`
  - `GET /{telegramId}/file/{uniqueId}`
  - `POST /{telegramId}/file/start-download`
  - `POST /{telegramId}/file/cancel-download`
  - `POST /{telegramId}/file/toggle-pause-download`
  - `POST /{telegramId}/file/remove`
  - `POST /{telegramId}/file/update-auto-settings`
  - `POST /files/start-download-multiple`
  - `POST /files/cancel-download-multiple`
  - `POST /files/toggle-pause-download-multiple`
  - `POST /files/remove-multiple`
  - `POST /files/update-tags`
  - `POST /file/{uniqueId}/update-tags`
  - schema bootstrap for `setting_record`, `telegram_record`, `file_record`, `statistic_record`
- Worker support
  - Background preload, auto-download, and transfer loops run in the Python backend
  - Behavior is migration-phase and keeps compatibility as the primary goal

## Run locally

```bash
python -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

On Windows PowerShell, replace `.venv/bin/python` with `.venv\\Scripts\\python.exe`.

## Environment variables

- `APP_ENV` (default: `prod`)
- `APP_ROOT` (default: `./data`)
- `DB_TYPE` (default: `sqlite`, currently only supported option)
- `DATA_PATH` (default: `data.db`, relative to `APP_ROOT`)
- `APP_VERSION` (default: `0.3.0`)
- `TELEGRAM_API_ID` (required for real TDLib login)
- `TELEGRAM_API_HASH` (required for real TDLib login)
- `TELEGRAM_LOG_LEVEL` (default: `1`)
- `TDLIB_SHARED_LIB` (optional absolute path to `tdjson` shared library)

If `TDLIB_SHARED_LIB` is not set, backend will try system `tdjson` library names. If that fails,
it will fall back to Python package `tdjson` when available in the runtime environment.

## Notes

- This phase is intentionally API-compatible for settings, account metadata, and offline file routes.
- WebSocket `/ws` emits authorization and method-result events for TDLib login when TDLib is configured.
- Remaining route handlers are registered so clients receive explicit `501` responses instead of `404`.
