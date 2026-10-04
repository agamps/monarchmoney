"""Local data/session locations, read from ~/monarch-data/config.toml.

Precedence for the data folder:
  1. a script's own --data-dir / --output style flags
  2. MONARCH_DATA_DIR environment variable
  3. `data_dir` in the config file (MONARCH_CONFIG overrides its location)
  4. ~/monarch-data/data
"""
import os
import sys
import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Any

MONARCH_HOME = Path("~/monarch-data").expanduser()
DEFAULT_CONFIG_FILE = MONARCH_HOME / "config.toml"
DEFAULT_DATA_DIR = MONARCH_HOME / "data"
DEFAULT_SESSION_FILE = MONARCH_HOME / ".mm" / "mm_session.pickle"


def config_file() -> Path:
    return Path(os.environ.get("MONARCH_CONFIG", DEFAULT_CONFIG_FILE)).expanduser()


@lru_cache(maxsize=1)
def load_config() -> dict[str, Any]:
    path = config_file()
    if not path.exists():
        print(
            f"No config at {path}; using {DEFAULT_DATA_DIR}. "
            "Copy config.example.toml there to change it.",
            file=sys.stderr,
        )
        return {}
    with path.open("rb") as f:
        return tomllib.load(f)


def _resolve(value: str | Path) -> Path:
    return Path(value).expanduser().resolve()


def data_dir() -> Path:
    value = os.environ.get("MONARCH_DATA_DIR") or load_config().get("data_dir")
    path = _resolve(value) if value else DEFAULT_DATA_DIR
    path.mkdir(parents=True, exist_ok=True)
    return path


def data_path(name: str) -> Path:
    return data_dir() / name


def session_file() -> Path:
    value = load_config().get("session_file")
    return _resolve(value) if value else DEFAULT_SESSION_FILE
