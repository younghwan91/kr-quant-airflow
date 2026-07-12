"""아침 누락분 자동 복구 — 전날 daily_collection에서 실패한 종목만 재수집.

daily_collection(16:00)은 매일 전종목 일봉을 통째로 재수집해서 시간이
오래 걸리고(~45분), 도중 에러가 나면 그 시점 이후 종목들이 그날치를
아예 못 받는다(2026-07-08 트랜잭션 연쇄 실패 사례). 그 실패분을 다음날
16:00까지 기다리지 않고, 컨테이너가 켜지는 아침 시간대에 값싸게 먼저
복구한다.

``combined.py --update``는 종목별로 이미 시장 최신 거래일 데이터가
있으면 일봉 API 호출 자체를 건너뛴다 — 그래서 전날 정상 수집된 종목은
DB 조회만으로 즉시 스킵되고, 실패해서 뒤처진 종목만 실제로 재수집된다.
전종목이 이미 최신이면 전체 실행이 몇 초 안에 끝난다.
"""

from __future__ import annotations

import sys

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from _common import kiwoom_env, run_collector, timescale_dsn


@dag(
    dag_id="daily_collection_catchup",
    schedule="5 10 * * *",  # 매일 10:05 KST — 컨테이너 기동(10:00) 직후
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection"],
)
def daily_collection_catchup():

    @task(retries=1, retry_delay=timedelta(minutes=10))
    def catchup_both() -> None:
        run_collector([
            sys.executable, "-m", "collectors.combined",
            "--market", "all", "--prod", "--rate", "0.9", "--update",
            "--db", timescale_dsn(),
        ], env=kiwoom_env())

    catchup_both()


daily_collection_catchup()
