from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _int_from_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        return default


def _load_dotenv_if_present() -> None:
    current_dir_env = Path.cwd() / ".env"
    project_dir_env = Path(__file__).resolve().parents[1] / ".env"

    for env_path in (current_dir_env, project_dir_env):
        if not env_path.exists() or not env_path.is_file():
            continue

        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            if line.startswith("export "):
                line = line[7:].strip()

            if "=" not in line:
                continue

            key_part, value_part = line.split("=", 1)
            key = key_part.strip()
            if not key or key in os.environ:
                continue

            value = value_part.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]
            os.environ[key] = value


@dataclass(frozen=True)
class AppConfig:
    app_env: str
    app_root: Path
    db_type: str
    data_path: str
    version: str
    telegram_api_id: int
    telegram_api_hash: str
    telegram_log_level: int
    tdlib_shared_lib: str

    @classmethod
    def from_env(cls) -> "AppConfig":
        _load_dotenv_if_present()
        app_root = Path(os.getenv("APP_ROOT", "./data")).resolve()
        config = cls(
            app_env=os.getenv("APP_ENV", "prod"),
            app_root=app_root,
            db_type=os.getenv("DB_TYPE", "sqlite"),
            data_path=os.getenv("DATA_PATH", "data.db"),
            version=os.getenv("APP_VERSION", "0.3.0"),
            telegram_api_id=_int_from_env("TELEGRAM_API_ID", 0),
            telegram_api_hash=os.getenv("TELEGRAM_API_HASH", "").strip(),
            telegram_log_level=_int_from_env("TELEGRAM_LOG_LEVEL", 1),
            tdlib_shared_lib=os.getenv("TDLIB_SHARED_LIB", "").strip(),
        )
        config.ensure_runtime_dirs()
        return config

    @property
    def sqlite_path(self) -> Path:
        return self.app_root / self.data_path

    def ensure_runtime_dirs(self) -> None:
        self.app_root.mkdir(parents=True, exist_ok=True)
        (self.app_root / "account").mkdir(parents=True, exist_ok=True)
        (self.app_root / "logs").mkdir(parents=True, exist_ok=True)
