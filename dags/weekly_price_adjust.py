"""daily_bars 기업행동(액면분할·무상증자) 백조정 → daily_bars_adjusted 테이블 재생성.

research/HANDOFF.md가 지적한 갭: `kr_quant.price_adjust.adjust_prices()`는
sepa_experiment.py 전략 하나에서만 호출되고 다른 여러 연구 스크립트는
daily_bars 원자료(미조정)를 직접 읽는다 — 분할이 가짜 −68% 손실로 잡혀
백테스트 절대수익률이 왜곡되는 정확히 그 버그(리더 시스템 CAGR +20.9%→
조정 +14.0%, GOAL 루프 54-59 진단)에 계속 노출된다.

이 DAG는 daily_bars_adjusted 테이블(PK: code,date)에 조정가를 채워서, 새
코드든 기존 연구 스크립트든 daily_bars 대신 이 테이블만 쓰면 자동으로
분할조정된 값을 받게 한다 — 원자료(daily_bars)는 그대로 보존.

**전체 재계산(매주) 이유:** back-adjust는 종목별 *전체* 이력을 봐야 정확하다
— 오늘 새로 감지된 분할이 그 종목의 과거 모든 조정값을 바꾼다. 그래서
증분이 아니라 매번 daily_bars 전체를 다시 읽어 재계산·upsert한다(자연키
(code,date) upsert라 기존 행은 덮어써짐). 분할은 드물어서(전체 이력 통틀어
~44건) 매일 돌릴 필요는 없고, daily_bars 규모(수백만 행)에서도 주간 배치로
충분히 저렴하다.

무인증(DB만), Kiwoom/DART 자격증명 불필요.

kr_quant.price_adjust의 핵심 로직(adjust_prices/diagnose)은 strategies/sepa_experiment.py가
in-process import하므로 kr-quant에 계속 남아 있다 — 콜렉터 이전과 무관. 그래서 이 DAG는
(daily_minervini_scan의 scanner_final.py처럼) /opt/kr-quant 마운트를 통해 계속 kr_quant를
실행한다. 다만 collectors/ 이전 이후 kr-quant의 editable pip install은 더 이상 하지
않으므로(entrypoint-wrapper.sh), PYTHONPATH로 대신 kr_quant를 찾게 한다.
"""

from __future__ import annotations

import os
import sys

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from _common import run_collector, timescale_dsn


@dag(
    dag_id="weekly_price_adjust",
    # 토요일 10:05 KST — 스택 기동(cron 0 10 * * *) 직후. 기존 05:00은 머신이
    # 꺼져 있는 시간이라(스택 가동 창 10:00~) 제 시각에 돌 수 없었다.
    schedule="5 10 * * 6",
    start_date=pendulum.datetime(2026, 7, 12, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "maintenance", "price-adjust"],
)
def weekly_price_adjust():

    @task(retries=1, retry_delay=timedelta(minutes=10))
    def rebuild_adjusted() -> None:
        run_collector(
            [
                sys.executable, "-m", "kr_quant.price_adjust",
                "--rebuild-db", "--db", timescale_dsn(),
            ],
            # editable install 없이 kr_quant 패키지를 찾도록 PYTHONPATH 주입 (src/ 레이아웃)
            env={**os.environ, "PYTHONPATH": "/opt/kr-quant/src"},
            cwd="/opt/kr-quant",
        )

    rebuild_adjusted()


weekly_price_adjust()
