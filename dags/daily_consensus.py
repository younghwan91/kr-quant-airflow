"""애널리스트 컨센서스(목표주가·투자의견·추정실적) 일일 수집 → TimescaleDB 직접 저장.

키움 브로커 API엔 컨센서스가 없고, 네이버 금융(FnGuide 출처)에 목표주가·
투자의견·추정 EPS가 있다. 이 DAG는 매 거래일 스냅샷을 한 줄씩 upsert하여
**컨센서스 개정(revision) 시계열**을 축적한다 — 후행 PEAD가 무력한 초대형주의
재평가를 미리 잡는 forward-looking 신호(docs/pead-strategy.md).

네이버는 인증이 필요 없으므로 Kiwoom 자격증명이 불필요하다.

**DB 직접 저장(--db-table)으로 전환한 이유:** 원래는 CSV 전용이라 daily_bars·
earnings 등 다른 데이터와 SQL로 조인이 안 됐다 — "다른 데이터랑 같이"라는
프로젝트 목표(README)에 맞춰 consensus 테이블(PK: code, date)에 직접 upsert.
sql/init_timescale.sql에 스키마 추가됨.

**--all-codes(전종목) 사용 이유:** 원래 유동성 상위 800종목만 받았으나, DART와
달리 네이버는 무인증·독립 레이트리밋이라 전종목(daily_bars 기준 ~2,600개)을
매일 다 훑어도 다른 수집(특히 DART 일한도)과 자원 경합이 없다. 애널리스트
커버리지가 없는 소형주는 자연히 스킵되어(수집기가 값이 전부 None이면
행을 안 씀) 커버리지가 실제 애널리스트 커버 종목 수만큼만 쌓인다 — 무해함.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pendulum
from airflow.decorators import dag, task


def _timescale_dsn() -> str:
    return (
        f"postgresql://{os.environ['TIMESCALE_USER']}:{os.environ['TIMESCALE_PASSWORD']}"
        f"@{os.environ['TIMESCALE_HOST']}:{os.environ.get('TIMESCALE_PORT', '5432')}"
        f"/{os.environ['TIMESCALE_DB']}"
    )


@dag(
    dag_id="daily_consensus",
    schedule="0 18 * * 1-5",  # 평일 18:00 KST (장 마감 후 컨센서스 갱신 반영)
    start_date=pendulum.datetime(2026, 7, 10, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "consensus"],
)
def daily_consensus():

    @task
    def collect_consensus() -> None:
        cmd = [
            sys.executable, "-m", "collectors.naver_consensus",
            "--db-table", "--all-codes", "--db", _timescale_dsn(),
        ]
        print(f"$ {' '.join(cmd[:-2])} --db ***")
        subprocess.run(cmd, check=True, cwd="/opt/airflow")

    collect_consensus()


daily_consensus()
