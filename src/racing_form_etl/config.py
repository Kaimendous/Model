from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional

ENV_KEYS = [
    "THERACINGAPI_USERNAME",
    "THERACINGAPI_PASSWORD",
    "THERACINGAPI_API_KEY",
]


def load_dotenv(dotenv_path: str | Path = ".env") -> Dict[str, str]:
    loaded: Dict[str, str] = {}
    try:
        from dotenv import load_dotenv as _load_dotenv  # type: ignore

        _load_dotenv(dotenv_path=dotenv_path, override=False)
        for key in ENV_KEYS:
            val = os.getenv(key)
            if val is not None:
                loaded[key] = val
        return loaded
    except Exception:
        pass

    path = Path(dotenv_path)
    if not path.exists():
        return loaded

    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        k, v = raw.split("=", 1)
        key = k.strip()
        value = v.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value
            loaded[key] = value
    return loaded


def save_dotenv(values: Dict[str, str], dotenv_path: str | Path = ".env") -> None:
    path = Path(dotenv_path)
    existing: Dict[str, str] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            raw = line.strip()
            if not raw or raw.startswith("#") or "=" not in raw:
                continue
            k, v = raw.split("=", 1)
            existing[k.strip()] = v.strip().strip('"').strip("'")

    existing.update({k: v for k, v in values.items() if v is not None})
    lines = [f"{k}={v}" for k, v in sorted(existing.items())]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def get_secret_status() -> Dict[str, bool]:
    return {key: bool(os.getenv(key)) for key in ENV_KEYS}


def get_api_credentials() -> tuple[Optional[str], Optional[str], Optional[str]]:
    return (
        os.getenv("THERACINGAPI_USERNAME"),
        os.getenv("THERACINGAPI_PASSWORD"),
        os.getenv("THERACINGAPI_API_KEY"),
    )
