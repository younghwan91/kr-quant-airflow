"""KRX 상장폐지종목 마스터 + 그 종목들의 과거 일봉 수집 (생존편향 보정).

``features/universe.py``가 미해결로 남겨둔 갭: point-in-time 유니버스를 지금
거래되는 종목만으로 구성하면 상장폐지된 종목이 통째로 빠져 생존편향이 생긴다.

KRX의 일반 통계 리포트(MDCSTAT류, ``daily_krx_shares`` DAG가 쓰는 것)는 최근
회원 로그인을 요구하도록 바뀌어 막혔지만, 이 DAG가 쓰는 ``finder_listdelisu``
(종목 검색 자동완성 위젯 API)는 로그인 없이 그대로 동작한다 — 실제 라이브
호출로 확인됨. 날짜는 안 주므로 daily_bars의 종목별 마지막 거래일로
상장폐지일을 근사한다(상폐 종목은 보통 상폐일 직전까지 거래되므로 근접치).

**두 번째 태스크(2026-08-15 추가):** 마스터 리스트만으로는 편향이 안 풀린다.
폐지 종목의 과거 시세가 daily_bars 에 있어야 백테스트 유니버스에 들어간다.
키움은 폐지 코드에 빈 응답(return_code=0)을 주므로 네이버 siseJson 으로 받는다 —
자세한 근거는 ``collectors/naver_delisted_bars.py`` docstring.

**주간 스케줄인 이유:** 상장폐지는 매일 몇 건씩 나는 이벤트가 아니라 드물게
발생하므로, price_adjust와 같은 주간 배치로 충분하다.

**price_adjust 보다 앞서 도는 이유(2026-08-15 순서 교체):** 이 DAG 가 새 폐지
종목의 시세를 daily_bars 에 넣으면, daily_bars_adjusted 는 그걸 본 뒤에
재생성돼야 한다. 반대 순서면 새로 받은 종목이 조정가 테이블에 일주일 늦게
반영된다. 그래서 delisted 10:05 → price_adjust 10:40 으로 바꿨다.

무인증, Kiwoom/DART 자격증명 불필요.
"""

from __future__ import annotations

import sys

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from _common import run_collector, timescale_dsn


@dag(
    dag_id="weekly_delisted_stocks",
    # 토요일 10:05 KST — 스택 가동 창(10:00~) 직후. price_adjust(10:40)보다 앞:
    # 새 폐지 종목 시세가 daily_bars 에 들어간 뒤 조정가가 재생성돼야 한다.
    schedule="5 10 * * 6",
    start_date=pendulum.datetime(2026, 7, 12, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "maintenance", "delisted"],
)
def weekly_delisted_stocks():

    @task(retries=1, retry_delay=timedelta(minutes=10))
    def collect_delisted() -> None:
        run_collector([
            sys.executable, "-m", "collectors.krx_delisted",
            "--db", timescale_dsn(),
        ])

    @task(retries=1, retry_delay=timedelta(minutes=20))
    def backfill_delisted_bars() -> None:
        """폐지 종목의 과거 일봉 백필 (네이버).

        이미 있는 (code, date)는 ON CONFLICT DO NOTHING 이라 매주 전종목을 훑어도
        새로 폐지된 것만 실제로 쌓인다 — 멱등이라 재실행이 안전하다. 우리 시세 구간
        이전에 폐지된 종목은 응답이 비어 비용 없이 넘어간다.
        """
        run_collector([
            sys.executable, "-m", "collectors.naver_delisted_bars",
            "--db", timescale_dsn(),
        ])

    collect_delisted() >> backfill_delisted_bars()


weekly_delisted_stocks()
