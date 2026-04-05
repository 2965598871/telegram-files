from __future__ import annotations

import ctypes
import json
import os
import threading
import time
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Callable
from uuid import uuid4


class TdlibConfigurationError(RuntimeError):
    pass


class TdlibRequestTimeout(TimeoutError):
    pass


def _tdjson_library_candidates(shared_lib_path: str | None) -> list[str]:
    candidates: list[str] = []
    if shared_lib_path:
        candidates.append(shared_lib_path)

    if os.name == "nt":
        candidates.extend(["tdjson.dll", "libtdjson.dll"])
    elif os.name == "posix":
        candidates.extend(["libtdjson.so", "libtdjson.dylib", "tdjson"])
    else:
        candidates.append("tdjson")

    return candidates


def _load_tdjson(shared_lib_path: str | None) -> ctypes.CDLL:
    candidates = _tdjson_library_candidates(shared_lib_path)

    last_error: Exception | None = None
    for candidate in candidates:
        try:
            return ctypes.CDLL(candidate)
        except Exception as exc:  # pragma: no cover - platform dependent
            last_error = exc

    searched = ", ".join(candidates)
    detail = f"; last error: {last_error}" if last_error else ""
    raise TdlibConfigurationError(
        f"Unable to load TDLib shared library. Tried: {searched}{detail}"
    )


class _TdJsonApi:
    def __init__(self, shared_lib_path: str | None, log_level: int) -> None:
        self._backend: _CtypesTdBackend | _PythonTdjsonBackend
        try:
            self._backend = _CtypesTdBackend(shared_lib_path, log_level)
            self.backend_name = "ctypes"
        except TdlibConfigurationError as ctypes_exc:
            try:
                self._backend = _PythonTdjsonBackend(log_level)
                self.backend_name = "tdjson"
            except Exception as tdjson_exc:
                candidates = ", ".join(_tdjson_library_candidates(shared_lib_path))
                raise TdlibConfigurationError(
                    "Unable to initialize TDLib backend. "
                    f"ctypes load failed ({ctypes_exc}); "
                    f"python tdjson fallback failed ({tdjson_exc}). "
                    f"Tried shared library candidates: {candidates}."
                ) from tdjson_exc

    def create_client(self) -> int | ctypes.c_void_p:
        return self._backend.create_client()

    def send(self, client: int | ctypes.c_void_p, request: bytes) -> None:
        self._backend.send(client, request)

    def receive(
        self,
        client: int | ctypes.c_void_p,
        timeout_seconds: float,
    ) -> bytes | None:
        return self._backend.receive(client, timeout_seconds)

    def destroy(self, client: int | ctypes.c_void_p) -> None:
        self._backend.destroy(client)


class _CtypesTdBackend:
    def __init__(self, shared_lib_path: str | None, log_level: int) -> None:
        self.lib = _load_tdjson(shared_lib_path)
        self.lib.td_json_client_create.argtypes = []
        self.lib.td_json_client_create.restype = ctypes.c_void_p

        self.lib.td_json_client_send.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
        self.lib.td_json_client_send.restype = None

        self.lib.td_json_client_receive.argtypes = [ctypes.c_void_p, ctypes.c_double]
        self.lib.td_json_client_receive.restype = ctypes.c_char_p

        self.lib.td_json_client_destroy.argtypes = [ctypes.c_void_p]
        self.lib.td_json_client_destroy.restype = None

        if hasattr(self.lib, "td_set_log_verbosity_level"):
            self.lib.td_set_log_verbosity_level.argtypes = [ctypes.c_int]
            self.lib.td_set_log_verbosity_level.restype = None
            self.lib.td_set_log_verbosity_level(int(log_level))

    def create_client(self) -> ctypes.c_void_p:
        return self.lib.td_json_client_create()

    def send(self, client: ctypes.c_void_p, request: bytes) -> None:
        self.lib.td_json_client_send(client, request)

    def receive(self, client: ctypes.c_void_p, timeout_seconds: float) -> bytes | None:
        payload = self.lib.td_json_client_receive(client, timeout_seconds)
        if not payload:
            return None
        return bytes(payload)

    def destroy(self, client: ctypes.c_void_p) -> None:
        self.lib.td_json_client_destroy(client)


class _PythonTdjsonBackend:
    def __init__(self, log_level: int) -> None:
        import tdjson as tdjson_module  # type: ignore

        self._tdjson = tdjson_module
        self._receive_lock = threading.Lock()
        self._mailbox_lock = threading.Lock()
        self._mailboxes: dict[int, Queue[bytes]] = {}

        set_log_request = json.dumps(
            {
                "@type": "setLogVerbosityLevel",
                "new_verbosity_level": int(log_level),
            },
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        self._tdjson.td_execute(set_log_request)

    def create_client(self) -> int:
        return int(self._tdjson.td_create_client_id())

    def send(self, client: int, request: bytes) -> None:
        self._tdjson.td_send(int(client), request)

    def receive(self, client: int, timeout_seconds: float) -> bytes | None:
        mailbox = self._mailbox_for(int(client))
        try:
            return mailbox.get_nowait()
        except Empty:
            pass

        deadline = time.monotonic() + timeout_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None

            with self._receive_lock:
                raw = self._tdjson.td_receive(float(remaining))

            if not raw:
                return None

            raw_bytes = bytes(raw)
            target_id = self._client_id_from_payload(raw_bytes)
            if target_id == int(client):
                return raw_bytes

            other_mailbox = self._mailbox_for(target_id)
            other_mailbox.put(raw_bytes)

    def destroy(self, client: int) -> None:
        close_request = json.dumps(
            {"@type": "close"},
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        self.send(int(client), close_request)
        with self._mailbox_lock:
            self._mailboxes.pop(int(client), None)

    def _mailbox_for(self, client_id: int) -> Queue[bytes]:
        with self._mailbox_lock:
            mailbox = self._mailboxes.get(client_id)
            if mailbox is None:
                mailbox = Queue()
                self._mailboxes[client_id] = mailbox
            return mailbox

    def _client_id_from_payload(self, payload: bytes) -> int:
        try:
            decoded = json.loads(payload.decode("utf-8"))
        except Exception:
            return 0
        return int(decoded.get("@client_id") or 0)


class _TdlibSession:
    def __init__(
        self,
        *,
        td_api: _TdJsonApi,
        on_authorization_state: Callable[[dict[str, Any]], None],
    ) -> None:
        self._td_api = td_api
        self._on_authorization_state = on_authorization_state
        self._client = self._td_api.create_client()
        if not self._client:
            raise RuntimeError("Failed to create TDLib client instance")

        self._stop_event = threading.Event()
        self._pending_lock = threading.Lock()
        self._pending: dict[str, Queue[dict[str, Any]]] = {}
        self._auth_state_lock = threading.Lock()
        self._auth_state_event = threading.Event()
        self._last_auth_state_type = ""
        self._thread = threading.Thread(
            target=self._receive_loop,
            name=f"tdlib-session-{uuid4().hex[:8]}",
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=2.0)
        self._fail_all_pending({"@type": "error", "message": "session closed"})
        self._td_api.destroy(self._client)

    def send_nowait(self, payload: dict[str, Any]) -> None:
        request = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        self._td_api.send(self._client, request.encode("utf-8"))

    def request(
        self, payload: dict[str, Any], timeout_seconds: float
    ) -> dict[str, Any]:
        request_id = uuid4().hex
        wrapped_payload = dict(payload)
        wrapped_payload["@extra"] = {"request_id": request_id}
        response_queue: Queue[dict[str, Any]] = Queue(maxsize=1)
        with self._pending_lock:
            self._pending[request_id] = response_queue

        self.send_nowait(wrapped_payload)

        try:
            return response_queue.get(timeout=timeout_seconds)
        except Empty as exc:
            with self._pending_lock:
                self._pending.pop(request_id, None)
            raise TdlibRequestTimeout(
                f"TDLib request timed out after {timeout_seconds:.1f}s"
            ) from exc

    def wait_until_initialized(self, timeout_seconds: float) -> bool:
        deadline = time.monotonic() + timeout_seconds
        while True:
            with self._auth_state_lock:
                last_state_type = self._last_auth_state_type
            if (
                last_state_type
                and last_state_type != "authorizationStateWaitTdlibParameters"
            ):
                return True

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False

            self._auth_state_event.wait(timeout=min(0.5, remaining))
            self._auth_state_event.clear()

    def _receive_loop(self) -> None:
        while not self._stop_event.is_set():
            raw = self._td_api.receive(self._client, 1.0)
            if not raw:
                continue

            try:
                payload = json.loads(bytes(raw).decode("utf-8"))
            except Exception:
                continue

            request_id = None
            extra = payload.get("@extra")
            if isinstance(extra, dict):
                extra_id = extra.get("request_id")
                if extra_id is not None:
                    request_id = str(extra_id)

            if request_id is not None:
                with self._pending_lock:
                    queue = self._pending.pop(request_id, None)
                if queue is not None:
                    queue.put(payload)
                    continue

            if payload.get("@type") == "updateAuthorizationState":
                authorization_state = payload.get("authorization_state")
                if isinstance(authorization_state, dict):
                    with self._auth_state_lock:
                        self._last_auth_state_type = str(
                            authorization_state.get("@type") or ""
                        )
                    self._auth_state_event.set()
                    try:
                        self._on_authorization_state(authorization_state)
                    except Exception:
                        continue

    def _fail_all_pending(self, payload: dict[str, Any]) -> None:
        with self._pending_lock:
            queues = list(self._pending.values())
            self._pending.clear()
        for queue in queues:
            queue.put(payload)


class TdlibAuthManager:
    def __init__(
        self,
        *,
        api_id: int,
        api_hash: str,
        application_version: str,
        log_level: int,
        shared_lib_path: str | None,
        on_authorization_state: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        if api_id <= 0:
            raise TdlibConfigurationError("TELEGRAM_API_ID must be a positive integer")
        if not api_hash:
            raise TdlibConfigurationError("TELEGRAM_API_HASH is required")

        self._api_id = api_id
        self._api_hash = api_hash
        self._application_version = application_version
        self._on_authorization_state = on_authorization_state
        self._td_api = _TdJsonApi(shared_lib_path, log_level)
        self._sessions: dict[str, _TdlibSession] = {}
        self._session_dirs: dict[str, str] = {}
        self._lock = threading.Lock()

    def ensure_session(self, account_id: str, database_directory: str) -> None:
        with self._lock:
            existing = self._sessions.get(account_id)
            if existing is not None:
                return

            self._session_dirs[account_id] = database_directory
            session = _TdlibSession(
                td_api=self._td_api,
                on_authorization_state=lambda state: self._handle_auth_state(
                    account_id, state
                ),
            )
            self._sessions[account_id] = session

        self.prepare_authorization(account_id, timeout_seconds=6.0)

    def prepare_authorization(
        self, account_id: str, timeout_seconds: float = 10.0
    ) -> bool:
        session = self._session_for(account_id)
        deadline = time.monotonic() + timeout_seconds

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False

            probe_timeout = max(0.5, min(remaining, 5.0))
            state = session.request(
                {"@type": "getAuthorizationState"},
                timeout_seconds=probe_timeout,
            )
            state_type = str(state.get("@type") or "")

            if state_type == "authorizationStateWaitTdlibParameters":
                parameters_request = self._tdlib_parameters_request(account_id)
                if parameters_request is None:
                    return False

                result = session.request(
                    parameters_request,
                    timeout_seconds=max(0.5, min(deadline - time.monotonic(), 5.0)),
                )
                if str(result.get("@type") or "") == "error":
                    return False
                continue

            if state_type == "authorizationStateWaitEncryptionKey":
                result = session.request(
                    {
                        "@type": "checkDatabaseEncryptionKey",
                        "encryption_key": "",
                    },
                    timeout_seconds=max(0.5, min(deadline - time.monotonic(), 5.0)),
                )
                if str(result.get("@type") or "") == "error":
                    return False
                continue

            return True

    def request(
        self,
        account_id: str,
        payload: dict[str, Any],
        timeout_seconds: float = 30.0,
    ) -> dict[str, Any]:
        session = self._session_for(account_id)
        return session.request(payload, timeout_seconds=timeout_seconds)

    def send_nowait(self, account_id: str, payload: dict[str, Any]) -> None:
        session = self._session_for(account_id)
        session.send_nowait(payload)

    def get_me(self, account_id: str) -> dict[str, Any]:
        return self.request(account_id, {"@type": "getMe"}, timeout_seconds=15.0)

    def remove_session(self, account_id: str) -> None:
        with self._lock:
            session = self._sessions.pop(account_id, None)
            self._session_dirs.pop(account_id, None)
        if session is not None:
            session.close()

    def close(self) -> None:
        with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
            self._session_dirs.clear()
        for session in sessions:
            session.close()

    def _session_for(self, account_id: str) -> _TdlibSession:
        with self._lock:
            session = self._sessions.get(account_id)
        if session is None:
            raise RuntimeError(f"TDLib session not found for account: {account_id}")
        return session

    def _handle_auth_state(self, account_id: str, state: dict[str, Any]) -> None:
        state_type = str(state.get("@type") or "")
        if state_type == "authorizationStateWaitTdlibParameters":
            self._send_tdlib_parameters(account_id)
        elif state_type == "authorizationStateWaitEncryptionKey":
            self.send_nowait(
                account_id,
                {
                    "@type": "checkDatabaseEncryptionKey",
                    "encryption_key": "",
                },
            )

        if self._on_authorization_state is not None:
            self._on_authorization_state(account_id, state)

    def _send_tdlib_parameters(self, account_id: str) -> None:
        payload = self._tdlib_parameters_request(account_id)
        if payload is None:
            return
        self.send_nowait(account_id, payload)

    def _tdlib_parameters_request(self, account_id: str) -> dict[str, Any] | None:
        with self._lock:
            database_directory = self._session_dirs.get(account_id)
        if not database_directory:
            return None

        db_dir = Path(database_directory).resolve()
        files_dir = db_dir / "files"
        db_dir.mkdir(parents=True, exist_ok=True)
        files_dir.mkdir(parents=True, exist_ok=True)

        return {
            "@type": "setTdlibParameters",
            "use_test_dc": False,
            "database_directory": str(db_dir),
            "files_directory": str(files_dir),
            "use_file_database": True,
            "use_chat_info_database": True,
            "use_message_database": True,
            "use_secret_chats": True,
            "api_id": self._api_id,
            "api_hash": self._api_hash,
            "system_language_code": "en",
            "device_model": "Telegram Files",
            "application_version": self._application_version,
            "enable_storage_optimizer": True,
        }
