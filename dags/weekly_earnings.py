"""주간 DART 분기 실적(순이익·매출·영업이익) 수집 → CSV 전체 재생성.

DART OpenAPI ``fnlttSinglAcnt``에서 분기 순이익·매출·영업이익을 당기/전년동기
쌍으로 받아 lookahead-safe ``avail_date``(분기말+공시지연)와 함께 CSV로 축적한다.
이 데이터가 PEAD(실적 YoY)와 미너비니 SEPA **Code 33**(EPS·매출·마진 3분기 연속
가속, ``features.fundamentals.code33_panel``)의 펀더멘털 입력이다.

**전체 재생성(refresh) 이유:** ``dart_earnings``의 resume는 *코드 단위* 스킵이라
기존 종목의 새 분기를 append하지 못한다. 그래서 매 실행마다 임시파일로 전체를 새로
받고 성공 시 원자적으로 교체한다 — 새 분기가 확실히 반영되고, 실패 시 직전 완본이
보존된다. 실적은 분기 공시(+45~90일 지연)라 주간 실행으로 충분하며, top-500 × 2018~
≈ 1.8만 콜로 DART 일 한도(2만)에 안전하게 들어간다.

DART 키는 Airflow의 Fernet 암호화 Variables에만 있고(컨테이너 평문 env 아님),
수집 subprocess에만 주입한다 — Kiwoom 자격증명과 동일 패턴(daily_collection).
출력은 ``/opt/kr-quant/data/earnings_financials.csv`` (호스트 ../kr-quant/data 영속).
"""

from __future__ import annotations

import os
import subprocess
import sys

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

OUT = "/opt/kr-quant/data/earnings_financials.csv"
TOP_N = "500"       # 유동성 상위 N (중소형 cap-rank 100-400 커버 + DART 일한도 안전)
FROM_YEAR = "2018"


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
    dag_id="weekly_earnings",
    schedule="0 18 * * 0",  # 매주 일요일 18:00 KST (분기 공시라 주간이면 충분)
    start_date=pendulum.datetime(2026, 7, 12, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "earnings"],
)
def weekly_earnings():

    @task
    def collect_earnings() -> None:
        os.makedirs(os.path.dirname(OUT), exist_ok=True)
        tmp = OUT + ".tmp"
        if os.path.exists(tmp):
            os.remove(tmp)  # 전체 재생성: 빈 tmp에서 시작해 모든 종목·분기 재수집
        cmd = [
            sys.executable, "-m", "kr_quant.collectors.dart_earnings",
            "--out", tmp, "--top-n", TOP_N, "--from-year", FROM_YEAR,
            "--db", _timescale_dsn(),
        ]
        print(f"$ {' '.join(cmd)}")
        subprocess.run(cmd, check=True, cwd="/opt/kr-quant", env=_dart_env())
        os.replace(tmp, OUT)  # 원자적 교체 — 성공 시에만 완본 갱신
        print(f"earnings refresh 완료 → {OUT}")

    collect_earnings()


weekly_earnings()
