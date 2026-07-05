"""신용잔고(short_credit) 일일 수집 — daily_collection과 스케줄 분리.

거래소 신용잔고 공시는 T+1~2 지연되는 경우가 잦아, 일봉/수급과 같은 시각에
돌리면 최신 데이터가 아직 안 나온 상태로 수집될 수 있다. 다음날 오전으로
스케줄을 늦춰 잡는다.
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
    dag_id="daily_short_credit",
    schedule="0 10 * * 2-6",  # 화~토 10:00 KST (전날 공시 데이터 반영 이후)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection"],
)
def daily_short_credit():

    @task
    def collect_short_credit() -> None:
        _run([
            sys.executable, "-m", "kr_quant.collectors.short_credit",
            "--market", "all", "--prod",
        ], env=_kiwoom_env())

    @task
    def sync_to_timescale() -> None:
        _run([
            sys.executable, "/opt/airflow/scripts/sync_to_timescale.py",
            "--sqlite", SQLITE_PATH, "--days", "10",
        ])

    collect_short_credit() >> sync_to_timescale()


daily_short_credit()
