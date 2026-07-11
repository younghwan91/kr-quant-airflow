"""일일 증분 DART 분기 실적(순이익·매출·영업이익) 수집 → earnings 테이블 upsert.

DART OpenAPI ``fnlttSinglAcnt``에서 분기 순이익·매출·영업이익을 당기/전년동기
쌍으로 받아 lookahead-safe ``avail_date``(분기말+공시지연)와 함께 ``earnings``
테이블에 upsert한다. 이 데이터가 PEAD(실적 YoY)와 미너비니 SEPA **Code 33**
(EPS·매출·마진 3분기 연속 가속, ``features.fundamentals.code33_panel``)의
펀더멘털 입력이다.

**당기+전기만 갱신하는 이유:** ``--recent-quarters 2``로 전체 ~2,600개
daily_bars 종목 중 당기+전분기(N-1)만 재확인한다 — (code, period) 단위 resume가
있으므로 이미 DB에 있는 조합은 skip되고, 대부분의 실행일은 사실상 no-op에
가깝다. 실적 시즌(분기말+45~90일 공시 몰림 구간)에만 실제로 새 데이터를 받는다.
2,600 종목 × 2분기 ≈ 5,200콜/일로 DART 일 한도(2만/키)에 안전하게 들어간다 —
과거 ``weekly_earnings.py``처럼 top-500 전체 히스토리를 매주 재수집하는 방식과
달리, 매일 가볍게 최신 분기만 스치듯 확인하는 접근이다.

**별도 DAG로 분리한 이유:** ``daily_collection``과 같은 흐름/시각(평일 16:00
KST, 장마감+데이터 확정 대기)에 편입하되, 같은 DAG에 태스크로 얹지 않고 별도
DAG로 둔다 — DART 수집 실패/재시도가 Kiwoom 일봉·수급 수집의 blast radius에
섞이지 않게 하기 위함(반대 방향도 마찬가지).

DART 키는 Airflow의 Fernet 암호화 Variables에만 있고(컨테이너 평문 env 아님),
수집 subprocess에만 주입한다 — Kiwoom 자격증명과 동일 패턴(daily_collection).

**주의(퇴역 후보):** 이 DAG와 (병행 작업 중인) ``earnings_backfill.py``가 함께
``earnings`` 테이블을 커버하게 되면, 기존 ``weekly_earnings.py``(주간 전체
CSV 재생성)는 두 DAG가 검증된 이후 superseded/퇴역 후보다. 다만 삭제/수정은
별도 판단이 필요하므로 이 파일에서는 건드리지 않는다.
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


def _dart_env() -> dict[str, str]:
    # DART 키는 Fernet 암호화 Variables에만 있음 — 수집 subprocess에만 주입.
    # 보조키(DART_API_KEY_2)가 있으면 함께 주입 → collector가 일한도(020) 시 로테이션.
    env = os.environ.copy()
    env["DART_API_KEY"] = Variable.get("DART_API_KEY")
    key2 = Variable.get("DART_API_KEY_2", default_var=None)
    if key2:
        env["DART_API_KEY_2"] = key2
    return env


@dag(
    dag_id="daily_earnings",
    schedule="0 16 * * 1-5",  # 평일 16:00 KST — daily_collection과 동일 흐름/시각
    start_date=pendulum.datetime(2026, 7, 12, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "earnings"],
)
def daily_earnings():

    @task
    def collect_earnings() -> None:
        cmd = [
            sys.executable, "-m", "kr_quant.collectors.dart_earnings",
            "--db-table", "--all-codes", "--recent-quarters", "2",
            "--db", _timescale_dsn(),
        ]
        # DSN(비밀번호 포함)을 로그에 남기지 않도록 --db 인자는 마스킹해서 출력
        print(f"$ {' '.join(cmd[:-2])} --db ***")
        subprocess.run(cmd, check=True, cwd="/opt/kr-quant", env=_dart_env())

    collect_earnings()


daily_earnings()
