"""애널리스트 컨센서스(목표주가·투자의견·추정실적) 일일 수집 → CSV 누적.

키움 브로커 API엔 컨센서스가 없고, 네이버 금융(FnGuide 출처)에 목표주가·
투자의견·추정 EPS가 있다. 이 DAG는 매 거래일 스냅샷을 한 줄씩 append하여
**컨센서스 개정(revision) 시계열**을 축적한다 — 후행 PEAD가 무력한 초대형주의
재평가를 미리 잡는 forward-looking 신호(docs/pead-strategy.md).

네이버는 인증이 필요 없으므로 Kiwoom 자격증명이 불필요하다. 출력은
``/opt/kr-quant/data/consensus.csv`` (호스트 ../kr-quant/data 에 영속).
수집기가 (date, code)로 중복을 걸러 하루 한 번만 append한다.

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

OUT = "/opt/kr-quant/data/consensus.csv"


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
        os.makedirs(os.path.dirname(OUT), exist_ok=True)
        cmd = [
            sys.executable, "-m", "kr_quant.collectors.naver_consensus",
            "--out", OUT, "--all-codes", "--db", _timescale_dsn(),
        ]
        print(f"$ {' '.join(cmd[:-2])} --db ***")
        subprocess.run(cmd, check=True, cwd="/opt/kr-quant")

    collect_consensus()


daily_consensus()
