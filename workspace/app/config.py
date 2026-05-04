from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict


class ConfigError(ValueError):
    """Raised when required environment values are missing or invalid."""


@dataclass(frozen=True)
class Settings:
    tushare_token: str
    db_path: Path


def _parse_env_file(env_path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not env_path.exists():
        return values

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def load_settings(env_path: str = ".env", require_token: bool = True) -> Settings:
    values: Dict[str, str] = {}
    for candidate in (Path(env_path), Path.cwd() / env_path, Path.cwd().parent / env_path):
        parsed = _parse_env_file(candidate)
        if parsed:
            values = parsed
            break

    token = os.environ.get("TUSHARE_TOKEN", values.get("TUSHARE_TOKEN", "")).strip()
    db_path_value = os.environ.get("DB_PATH", values.get("DB_PATH", "")).strip()

    if require_token and not token:
        raise ConfigError("Missing TUSHARE_TOKEN in .env")
    if not db_path_value:
        raise ConfigError("Missing DB_PATH in .env")

    db_path = Path(db_path_value).expanduser()
    if not db_path.is_absolute():
        db_path = Path.cwd() / db_path

    return Settings(tushare_token=token, db_path=db_path)
