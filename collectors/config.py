"""Credential loading and authenticated client construction.

Keys are read from the environment first, then from a ``.env`` file at the
repo root (never committed). Nothing here hardcodes secrets.
"""

from __future__ import annotations

import os
from pathlib import Path

from kiwoom_rest_api import KiwoomAPI


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_keys(env_path: str | Path | None = None) -> tuple[str, str]:
    """Return (app_key, app_secret) from env vars or ``.env``.

    Raises:
        RuntimeError: if either credential is missing.
    """
    app_key = os.environ.get("KIWOOM_APP_KEY", "")
    app_secret = os.environ.get("KIWOOM_APP_SECRET", "")

    path = Path(env_path) if env_path else (repo_root() / ".env")
    if (not app_key or not app_secret) and path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            value = value.strip().strip('"').strip("'")
            if key.strip() == "KIWOOM_APP_KEY" and not app_key:
                app_key = value
            elif key.strip() == "KIWOOM_APP_SECRET" and not app_secret:
                app_secret = value

    if not app_key or not app_secret:
        raise RuntimeError(
            "KIWOOM_APP_KEY / KIWOOM_APP_SECRET 가 환경변수나 .env 에 없습니다. "
            ".env.example 를 참고해 .env 를 채우세요."
        )
    return app_key, app_secret


def make_api(is_mock: bool = True, *, login: bool = True, **kwargs) -> KiwoomAPI:
    """Build a KiwoomAPI client (per-TR rate limiting on by default) and log in.

    Args:
        is_mock: Use the mock server (True) or production (False).
        login: Issue an access token immediately.
        **kwargs: Forwarded to ``KiwoomAPI`` (e.g. rate_limit, max_retries).
    """
    app_key, app_secret = load_keys()
    api = KiwoomAPI(app_key=app_key, app_secret=app_secret, is_mock=is_mock, **kwargs)
    if login:
        api.login()
    return api
