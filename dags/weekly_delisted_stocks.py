"""KRX 상장폐지종목 마스터 리스트 수집 → delisted_stocks 테이블 (생존편향 보정).

``features/universe.py``가 미해결로 남겨둔 갭: point-in-time 유니버스를 지금
거래되는 종목만으로 구성하면 상장폐지된 종목이 통째로 빠져 생존편향이 생긴다.

KRX의 일반 통계 리포트(MDCSTAT류, ``daily_krx_shares`` DAG가 쓰는 것)는 최근
회원 로그인을 요구하도록 바뀌어 막혔지만, 이 DAG가 쓰는 ``finder_listdelisu``
(종목 검색 자동완성 위젯 API)는 로그인 없이 그대로 동작한다 — 실제 라이브
호출로 확인됨. 날짜는 안 주므로 daily_bars의 종목별 마지막 거래일로
상장폐지일을 근사한다(상폐 종목은 보통 상폐일 직전까지 거래되므로 근접치).

**주간 스케줄인 이유:** 상장폐지는 매일 몇 건씩 나는 이벤트가 아니라 드물게
발생하므로, price_adjust와 같은 주간 배치로 충분하다.

무인증, Kiwoom/DART 자격증명 불필요.
"""

from __future__ import annotations

import os
import subprocess
import sys

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task


def _timescale_dsn() -> str:
    return (
        f"postgresql://{os.environ['TIMESCALE_USER']}:{os.environ['TIMESCALE_PASSWORD']}"
        f"@{os.environ['TIMESCALE_HOST']}:{os.environ.get('TIMESCALE_PORT', '5432')}"
        f"/{os.environ['TIMESCALE_DB']}"
    )


@dag(
    dag_id="weekly_delisted_stocks",
    schedule="10 5 * * 6",  # 토요일 05:10 KST — weekly_price_adjust(05:00) 직후, 같은 한가한 시간대
    start_date=pendulum.datetime(2026, 7, 12, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "maintenance", "delisted"],
)
def weekly_delisted_stocks():

    @task(retries=1, retry_delay=timedelta(minutes=10))
    def collect_delisted() -> None:
        cmd = [
            sys.executable, "-m", "collectors.krx_delisted",
            "--db", _timescale_dsn(),
        ]
        print(f"$ {' '.join(cmd[:-2])} --db ***")
        subprocess.run(cmd, check=True, cwd="/opt/airflow")

    collect_delisted()


weekly_delisted_stocks()
