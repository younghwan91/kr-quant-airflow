"""Daily 일봉+수급 수집 → TimescaleDB 동기화.

신용잔고는 보통 T+1~2 지연 공시라 별도 DAG(daily_short_credit)로 분리했다.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable

SQLITE_PATH = os.environ.get("KR_QUANT_SQLITE_PATH", "/opt/kr-quant/data/kr_quant.db")


def _kiwoom_env() -> dict[str, str]:
    # Credentials live only in Airflow's Fernet-encrypted Variables store,
    # not in container env — injected here for the collector subprocess only.
    env = os.environ.copy()
    env["KIWOOM_APP_KEY"] = Variable.get("KIWOOM_APP_KEY")
    env["KIWOOM_APP_SECRET"] = Variable.get("KIWOOM_APP_SECRET")
    return env


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    print(f"$ {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd="/opt/kr-quant", env=env)


@dag(
    dag_id="daily_collection",
    schedule="30 19 * * 1-5",  # 평일 19:30 KST — 장마감 데이터 확정 이후
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection"],
)
def daily_collection():

    @task
    def collect_both() -> None:
        # --prod: 실데이터. 모의서버 기본값은 실제 시세/수급이 아님
        # (kr-quant/README.md 참고).
        _run([
            sys.executable, "-m", "kr_quant.collectors.combined",
            "--market", "all", "--prod", "--rate", "0.9",
        ], env=_kiwoom_env())

    @task
    def sync_to_timescale() -> None:
        _run([
            sys.executable, "/opt/airflow/scripts/sync_to_timescale.py",
            "--sqlite", SQLITE_PATH, "--days", "7",
        ])

    collect_both() >> sync_to_timescale()


daily_collection()
