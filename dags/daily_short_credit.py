"""신용잔고(short_credit) 일일 수집 → TimescaleDB 직접 저장.

daily_collection과 스케줄 분리 — 거래소 신용잔고 공시는 T+1~2 지연되는
경우가 잦아, 일봉/수급과 같은 시각에 돌리면 최신 데이터가 아직 안 나온
상태로 수집될 수 있다. 다음날 오전으로 스케줄을 늦춰 잡는다.

storage.py가 Postgres DSN을 받으면 TimescaleDB에 직접 upsert하므로 별도
sync 스텝이 필요 없다.
"""

from __future__ import annotations

import sys

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from _common import kiwoom_env, run_collector, timescale_dsn


@dag(
    dag_id="daily_short_credit",
    schedule="0 10 * * 2-6",  # 화~토 10:00 KST (전날 공시 데이터 반영 이후)
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection"],
)
def daily_short_credit():

    @task(retries=1, retry_delay=timedelta(minutes=10))
    def collect_short_credit() -> None:
        run_collector([
            sys.executable, "-m", "collectors.short_credit",
            "--market", "all", "--prod", "--db", timescale_dsn(),
        ], env=kiwoom_env())

    collect_short_credit()


daily_short_credit()
