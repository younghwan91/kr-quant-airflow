"""DART 분기 실적(순이익·매출·영업이익) 전체 이력 백필 → ``earnings`` 테이블 upsert.

``weekly_earnings``는 top-N 유동성 종목의 CSV 전체 재생성(refresh)으로 최근 분기를
따라잡는 증분(incremental) 파이프라인이다. 반면 이 DAG는 ``--all-codes``로
daily_bars 전 종목(~2,600개)을, ``--from-year 2016``부터(주1) 전체 이력으로 한 번에
``earnings`` 테이블에 upsert하는 **백필 전용** 파이프라인이다 — 두 목적(최신 유지 vs.
과거 이력 채우기)이 달라 별도 DAG로 분리했다.

(주1) 2016년은 EARNINGS_PIPELINE_PLAN.md 핸드오프 문서의 권장치이며 아직 사용자
확인은 안 된 기본값이다 — 필요 시 조정할 것.

**schedule=None(수동 트리거) 이유:** 전 종목 × 전체 이력 조합은 DART 일일 호출
한도를 넘어서기 쉽다. 자동 스케줄로 걸어두면 한도 소진이 매일 통제 없이 반복되므로,
``airflow dags trigger earnings_backfill``로 수동 실행하거나 사용자가 원하면
주간 등으로 드물게 스케줄링한다.

**재실행 안전(resume) 이유:** ``--db-table`` 모드는 ``earnings`` 테이블에 이미 있는
(code, period) 조합을 건너뛰므로, DART 일한도(status 020, 전 키 소진)로 중단되어도
다음 실행이 자동으로 이어받는다 — 별도의 "resume" 플래그가 필요 없다. 즉 이 DAG는
수렴할 때까지 반복 트리거해도 안전(idempotent)하다.

DART 키는 Airflow의 Fernet 암호화 Variables에만 있고(컨테이너 평문 env 아님),
수집 subprocess에만 주입한다 — Kiwoom 자격증명과 동일 패턴(daily_collection),
weekly_earnings와 동일 패턴.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

FROM_YEAR = "2016"  # EARNINGS_PIPELINE_PLAN.md 권장치 — 사용자 확인 필요한 기본값


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
    dag_id="earnings_backfill",
    schedule=None,  # 백필 전용 — 수동 트리거(airflow dags trigger earnings_backfill) 또는 필요 시 드물게 스케줄링
    start_date=pendulum.datetime(2026, 7, 12, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "earnings", "backfill"],
)
def earnings_backfill():

    @task
    def collect_earnings_backfill() -> None:
        cmd = [
            sys.executable, "-m", "kr_quant.collectors.dart_earnings",
            "--db-table", "--all-codes", "--from-year", FROM_YEAR,
            "--db", _timescale_dsn(),
        ]
        # DSN(비밀번호 포함)을 로그에 남기지 않도록 --db 인자는 마스킹해서 출력
        print(f"$ {' '.join(cmd[:-2])} --db ***")
        subprocess.run(cmd, check=True, cwd="/opt/kr-quant", env=_dart_env())
        print("earnings 전체 이력 백필 실행 완료 (DB upsert, DART 일한도 도달 시 다음 트리거에서 자동 재개)")

    collect_earnings_backfill()


earnings_backfill()
