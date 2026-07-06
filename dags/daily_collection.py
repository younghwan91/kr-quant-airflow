"""Daily 일봉+수급+업종지수 수집 → TimescaleDB 직접 저장.

storage.py가 Postgres DSN을 받으면 TimescaleDB에 직접 upsert하므로(ON
CONFLICT DO UPDATE — sqlite의 INSERT OR REPLACE와 동일한 자연키 upsert),
별도 sync 스텝이 필요 없다. 신용잔고는 보통 T+1~2 지연 공시라 별도
DAG(daily_short_credit)로 분리했다.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable


def _timescale_dsn() -> str:
    return (
        f"postgresql://{os.environ['TIMESCALE_USER']}:{os.environ['TIMESCALE_PASSWORD']}"
        f"@{os.environ['TIMESCALE_HOST']}:{os.environ.get('TIMESCALE_PORT', '5432')}"
        f"/{os.environ['TIMESCALE_DB']}"
    )


def _kiwoom_env() -> dict[str, str]:
    # Credentials live only in Airflow's Fernet-encrypted Variables store,
    # not in container env — injected here for the collector subprocess only.
    env = os.environ.copy()
    env["KIWOOM_APP_KEY"] = Variable.get("KIWOOM_APP_KEY")
    env["KIWOOM_APP_SECRET"] = Variable.get("KIWOOM_APP_SECRET")
    return env


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    print(f"$ {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd="/opt/kr-quant", env=env)


@dag(
    dag_id="daily_collection",
    schedule="30 19 * * 1-5",  # 평일 19:30 KST — 장마감 데이터 확정 이후
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection"],
)
def daily_collection():

    @task
    def collect_both() -> None:
        # --prod: 실데이터. 모의서버 기본값은 실제 시세/수급이 아님
        # (kr-quant/README.md 참고).
        _run([
            sys.executable, "-m", "kr_quant.collectors.combined",
            "--market", "all", "--prod", "--rate", "0.9", "--db", _timescale_dsn(),
        ], env=_kiwoom_env())

    @task
    def collect_sector() -> None:
        # 별도 TR(ka20003/ka20006)이라 collect_both와 레이트리밋 버킷이 안
        # 겹침. TimescaleDB는 MVCC라 두 태스크가 동시에 써도 안전(sqlite와
        # 달리 단일 writer 제약 없음) — 병렬로 둬도 됨.
        _run([
            sys.executable, "-m", "kr_quant.collectors.sector_index",
            "--prod", "--days", "10", "--db", _timescale_dsn(),
        ], env=_kiwoom_env())

    collect_both()
    collect_sector()


daily_collection()
