"""Shared helpers for DAG subprocess invocation — DSN/credential injection.

Not a DAG itself (no ``@dag``-decorated function), so Airflow's DagFileProcessor
parses it and finds zero DAGs — harmless, same as any other non-DAG .py file
in the dags folder. Importable as a plain top-level module (``from _common
import ...``) because Airflow adds each dag file's own directory (here,
``dags/``) to ``sys.path`` before exec'ing it.

Previously each DAG file duplicated ``_timescale_dsn()``/``_kiwoom_env()``/
``_dart_env()``/``_run()`` verbatim — a DSN format change meant editing 10
files. Centralized here instead.
"""

from __future__ import annotations

import os
import subprocess

from airflow.models import Variable


def timescale_dsn() -> str:
    return (
        f"postgresql://{os.environ['TIMESCALE_USER']}:{os.environ['TIMESCALE_PASSWORD']}"
        f"@{os.environ['TIMESCALE_HOST']}:{os.environ.get('TIMESCALE_PORT', '5432')}"
        f"/{os.environ['TIMESCALE_DB']}"
    )


def kiwoom_env() -> dict[str, str]:
    # Credentials live only in Airflow's Fernet-encrypted Variables store,
    # not in container env — injected here for the collector subprocess only.
    env = os.environ.copy()
    env["KIWOOM_APP_KEY"] = Variable.get("KIWOOM_APP_KEY")
    env["KIWOOM_APP_SECRET"] = Variable.get("KIWOOM_APP_SECRET")
    return env


def dart_env() -> dict[str, str]:
    # DART 키는 Fernet 암호화 Variables에만 있음 — 수집 subprocess에만 주입.
    # 보조키(DART_API_KEY_2)가 있으면 함께 주입 → collector가 일한도(020) 시 로테이션.
    env = os.environ.copy()
    env["DART_API_KEY"] = Variable.get("DART_API_KEY")
    key2 = Variable.get("DART_API_KEY_2", default_var=None)
    if key2:
        env["DART_API_KEY_2"] = key2
    return env


def run_collector(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    print(f"$ {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd="/opt/airflow", env=env)
