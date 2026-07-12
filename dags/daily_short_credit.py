"""신용잔고(short_credit) 일일 수집 → TimescaleDB 직접 저장.

daily_collection과 스케줄 분리 — 거래소 신용잔고 공시는 T+1~2 지연되는
경우가 잦아, 일봉/수급과 같은 시각에 돌리면 최신 데이터가 아직 안 나온
상태로 수집될 수 있다. 다음날 오전으로 스케줄을 늦춰 잡는다.

storage.py가 Postgres DSN을 받으면 TimescaleDB에 직접 upsert하므로 별도
sync 스텝이 필요 없다.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable


def _timescale_dsn() -> str:
    return (
        f"postgresql://{os.environ['TIMESCALE_USER']}:{os.environ['TIMESCALE_PASSWORD']}"
        f"@{os.environ['TIMESCALE_HOST']}:{os.environ.get('TIMESCALE_PORT', '5432')}"
        f"/{os.environ['TIMESCALE_DB']}"
    )


def _kiwoom_env() -> dict[str, str]:
    # Credentials live only in Airflow's Fernet-encrypted Variables store,
    # not in container env — injected here for the collector subprocess only.
    env = os.environ.copy()
    env["KIWOOM_APP_KEY"] = Variable.get("KIWOOM_APP_KEY")
    env["KIWOOM_APP_SECRET"] = Variable.get("KIWOOM_APP_SECRET")
    return env


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    print(f"$ {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd="/opt/airflow", env=env)


@dag(
    dag_id="daily_short_credit",
    schedule="0 10 * * 2-6",  # 화~토 10:00 KST (전날 공시 데이터 반영 이후)
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection"],
)
def daily_short_credit():

    @task
    def collect_short_credit() -> None:
        _run([
            sys.executable, "-m", "collectors.short_credit",
            "--market", "all", "--prod", "--db", _timescale_dsn(),
        ], env=_kiwoom_env())

    collect_short_credit()


daily_short_credit()
