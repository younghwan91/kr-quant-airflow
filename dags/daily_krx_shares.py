"""일별 상장주식수(KRX MDCSTAT01501) 수집 → TimescaleDB 직접 저장.

ka10001(weekly_listed_shares)은 현재 스냅샷만 주어 과거 백필이 불가하고, 그
때문에 market_cap_asof가 과거 백테스트 날짜에 '오늘의 주식수'를 소급 적용하는
lookahead 버그가 있었다. KRX MDCSTAT01501은 무인증·전종목·날짜지정(trdDd)이라
과거 임의 거래일을 백필할 수 있어 point-in-time 시총/수급비율 분모를 정확히
만든다 — 이 DAG가 그 authoritative 소스다.

- 매 거래일 장 마감·시세확정 후(18:30 KST) 당일 상장주식수를 append.
- 최초 과거 백필은 수동 트리거로 range 실행:
    docker exec kr-quant-airflow-airflow-scheduler-1 \
      python -m collectors.krx_shares \
      --from 2024-01-08 --to <today> --db <dsn>
- Kiwoom 자격증명이 불필요(네이버/DART와 동일하게 무인증).
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
    dag_id="daily_krx_shares",
    schedule="30 18 * * 1-5",  # 평일 18:30 KST — 장 마감·KRX 시세 확정 후
    start_date=pendulum.datetime(2026, 7, 10, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "shares"],
)
def daily_krx_shares():

    @task
    def collect_krx_shares() -> None:
        # KST 당일(거래일이면 데이터 존재, 휴장일이면 collector가 codes=0로 무해 처리)
        today = pendulum.now("Asia/Seoul").to_date_string()
        cmd = [
            sys.executable, "-m", "collectors.krx_shares",
            "--date", today, "--db", _timescale_dsn(),
        ]
        print(f"$ {' '.join(cmd)}")
        subprocess.run(cmd, check=True, cwd="/opt/airflow")

    collect_krx_shares()


daily_krx_shares()
