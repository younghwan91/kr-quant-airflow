"""주간 상장주식수(listed_shares) 수집 → TimescaleDB 직접 저장.

ka10001은 현재 시점 스냅샷만 반환하고(과거 이력 백필 불가), 상장주식수는
분할/자사주 등 기업행위가 없는 한 자주 바뀌지 않으므로 매일이 아닌 주 1회
(월요일)만 수집한다. 컨테이너 기동(10:00) 10분 후, daily_collection_catchup
(10:05)과는 5분 겹치지 않게 스태거링.

storage.py가 Postgres DSN을 받으면 TimescaleDB에 직접 upsert하므로 별도
sync 스텝이 필요 없다.
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


def _kiwoom_env() -> dict[str, str]:
    # Credentials live only in Airflow's Fernet-encrypted Variables store,
    # not in container env — injected here for the collector subprocess only.
    env = os.environ.copy()
    env["KIWOOM_APP_KEY"] = Variable.get("KIWOOM_APP_KEY")
    env["KIWOOM_APP_SECRET"] = Variable.get("KIWOOM_APP_SECRET")
    return env


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    print(f"$ {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd="/opt/airflow", env=env)


@dag(
    dag_id="weekly_listed_shares",
    schedule="10 10 * * 1",  # 매주 월요일 10:10 KST — 컨테이너 기동(10:00) 10분 후
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "collection"],
)
def weekly_listed_shares():

    @task
    def collect_listed_shares() -> None:
        _run([
            sys.executable, "-m", "collectors.listed_shares",
            "--market", "all", "--prod", "--db", _timescale_dsn(),
        ], env=_kiwoom_env())

    collect_listed_shares()


weekly_listed_shares()
