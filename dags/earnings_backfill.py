"""DART 분기 실적(순이익·매출·영업이익) 전체 이력 백필 → ``earnings`` 테이블 upsert.

``weekly_earnings``는 top-N 유동성 종목의 CSV 전체 재생성(refresh)으로 최근 분기를
따라잡는 증분(incremental) 파이프라인이다. 반면 이 DAG는 ``--all-codes``로
daily_bars 전 종목(~2,600개)을, ``--from-year 2016``부터(주1) 전체 이력으로 한 번에
``earnings`` 테이블에 upsert하는 **백필 전용** 파이프라인이다 — 두 목적(최신 유지 vs.
과거 이력 채우기)이 달라 별도 DAG로 분리했다.

(주1) 2016년은 EARNINGS_PIPELINE_PLAN.md 핸드오프 문서의 권장치이며 아직 사용자
확인은 안 된 기본값이다 — 필요 시 조정할 것.

**매일 자동 스케줄(18:00 KST, daily_consensus와 동시) 이유:** 전 종목 × 전체
이력(~114k콜, 2016~)은 하루 한도(2키 기준 4만콜)로 며칠에 걸쳐야 끝난다.
(code, period) DB resume 덕에 매일 자동으로 다시 돌려도 위험하지 않다 —
이미 채운 조합은 스킵하고 남은 것만 이어받으며, 한도(020) 도달 시 그냥
조용히 끝난다(에러 아님). 전종목 백필이 끝난 뒤로는 이 DAG가 사실상
no-op(전부 스킵)이 되므로 계속 켜둬도 무해하다. ``daily_consensus``(네이버,
무인증)는 DART API를 전혀 쓰지 않아 같은 시각(18:00)에 돌아도 자원 경합이
없다 — 반면 ``daily_earnings``(16:00)는 이 DAG와 같은 DART 키/일한도를
공유하므로 2시간 간격을 둬 분리했다. 필요하면 ``airflow dags trigger
earnings_backfill``로 즉시 수동 실행도 가능.

**재실행 안전(resume) 이유:** ``--db-table`` 모드는 ``earnings`` 테이블에 이미 있는
(code, period) 조합을 건너뛰므로, DART 일한도(status 020, 전 키 소진)로 중단되어도
다음 실행이 자동으로 이어받는다 — 별도의 "resume" 플래그가 필요 없다. 즉 이 DAG는
수렴할 때까지 반복 트리거해도 안전(idempotent)하다.

**retries=2 (30분 간격) 이유:** corp_map 부트스트랩(load_corp_map_with_rotation)이
일시적 네트워크 오류로 실패하면 재시도로 복구된다. 반면 실제 원인이 일한도(020)
소진이면 재시도해도 다시 실패할 뿐이지만 손해는 없다(어차피 다음날 18:00 스케줄이
이어받음) — 두 경우를 구분해 특수 처리하기보다 재시도 자체를 안전하게 둔다.

DART 키는 Airflow의 Fernet 암호화 Variables에만 있고(컨테이너 평문 env 아님),
수집 subprocess에만 주입한다 — Kiwoom 자격증명과 동일 패턴(daily_collection),
weekly_earnings와 동일 패턴.
"""

from __future__ import annotations

import subprocess
import sys

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from _common import dart_env, timescale_dsn

FROM_YEAR = "2016"  # EARNINGS_PIPELINE_PLAN.md 권장치 — 사용자 확인 필요한 기본값


@dag(
    dag_id="earnings_backfill",
    schedule="0 18 * * 1-5",  # 평일 18:00 KST — daily_consensus와 동시(자원 경합 없음), daily_earnings(16:00)와는 2시간 분리
    start_date=pendulum.datetime(2026, 7, 12, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection", "earnings", "backfill"],
)
def earnings_backfill():

    @task(retries=2, retry_delay=timedelta(minutes=30))
    def collect_earnings_backfill() -> None:
        cmd = [
            sys.executable, "-m", "collectors.dart_earnings",
            "--db-table", "--all-codes", "--from-year", FROM_YEAR,
            "--db", timescale_dsn(),
        ]
        # DSN(비밀번호 포함)을 로그에 남기지 않도록 --db 인자는 마스킹해서 출력
        print(f"$ {' '.join(cmd[:-2])} --db ***")
        subprocess.run(cmd, check=True, cwd="/opt/airflow", env=dart_env())
        print("earnings 전체 이력 백필 실행 완료 (DB upsert, DART 일한도 도달 시 다음 트리거에서 자동 재개)")

    collect_earnings_backfill()


earnings_backfill()
