"""Local data/session locations, read from the repo's config files.

    config.default.toml   checked in; the defaults
    config.local.toml     optional, git-ignored; keys here override the defaults

Precedence for the data folder:
  1. a script's own --data-dir / --output style flags
  2. MONARCH_DATA_DIR environment variable
  3. `data_dir` in config.local.toml, then config.default.toml
"""
import os
import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Any

REPO_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_FILE = REPO_DIR / "config.default.toml"
LOCAL_CONFIG_FILE = REPO_DIR / "config.local.toml"


def _read(path: Path) -> dict[str, Any]:
    with path.open("rb") as f:
        return tomllib.load(f)


@lru_cache(maxsize=1)
def load_config() -> dict[str, Any]:
    config = _read(DEFAULT_CONFIG_FILE)
    if LOCAL_CONFIG_FILE.exists():
        config.update(_read(LOCAL_CONFIG_FILE))
    return config


def _setting(key: str) -> Path:
    value = load_config().get(key)
    if not value:
        raise KeyError(f"'{key}' is not set in {DEFAULT_CONFIG_FILE.name} or {LOCAL_CONFIG_FILE.name}")
    return Path(value).expanduser().resolve()


def data_dir() -> Path:
    env = os.environ.get("MONARCH_DATA_DIR")
    path = Path(env).expanduser().resolve() if env else _setting("data_dir")
    path.mkdir(parents=True, exist_ok=True)
    return path


def data_path(name: str) -> Path:
    return data_dir() / name


def session_file() -> Path:
    return _setting("session_file")
