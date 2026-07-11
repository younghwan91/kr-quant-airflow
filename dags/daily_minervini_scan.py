"""일일 미너비니 규칙 스캐너 → 후보 + 레짐을 CSV에 누적 (RBA 축적).

GOAL 루프 1-14에서 수렴한 순수 규칙 시스템(research/operator_flow/minervini/)을 매
거래일 장 마감 후 실행해 (1) 시장 breadth 레짐 (2) 오늘의 진입 후보를 한 줄씩 append.
미너비니의 최종 조언 — 이론 기대치(TBA)가 아니라 실제 매매 결과(RBA)로 리스크를 설계하려면
실현 결과 데이터를 축적해야 한다 — 를 실행하는 파이프라인.

출력: /opt/kr-quant/data/minervini_scan.csv (호스트 영속), 컬럼:
  date, breadth, regime, n_candidates, codes(콤마구분)
무인증(DB만), Kiwoom 자격증명 불필요.
"""

from __future__ import annotations

import csv
import os
import subprocess
import sys

import pendulum
from airflow.decorators import dag, task

OUT = "/opt/kr-quant/data/minervini_scan.csv"


def _dsn() -> str:
    return (
        f"postgresql://{os.environ['TIMESCALE_USER']}:{os.environ['TIMESCALE_PASSWORD']}"
        f"@{os.environ['TIMESCALE_HOST']}:{os.environ.get('TIMESCALE_PORT', '5432')}"
        f"/{os.environ['TIMESCALE_DB']}"
    )


@dag(
    dag_id="daily_minervini_scan",
    schedule="40 18 * * 1-5",  # 평일 18:40 KST — 시세·수급 확정 후
    start_date=pendulum.datetime(2026, 7, 11, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "minervini", "scan"],
)
def daily_minervini_scan():

    @task
    def scan_and_log() -> None:
        os.makedirs(os.path.dirname(OUT), exist_ok=True)
        # 스캐너 결과를 sentinel 접두 한 줄로 방출 — 후보 0(약세 레짐)일 때 빈 codes로도
        # 안전하게 파싱된다. 위치 기반 4줄 파싱은 빈 줄이 필터링돼 IndexError를 냈었다.
        code = (
            "import os,psycopg2,sys;sys.path.insert(0,'/opt/kr-quant/research/operator_flow/minervini');"
            "from scanner_final import scan;"
            f"con=psycopg2.connect('{_dsn()}');"
            "a,b,c=scan(con);con.close();"
            "codes=','.join(c['code'].tolist()) if len(c) else '';"
            "print('RESULT\\t%s\\t%s\\t%d\\t%s'%(a,round(float(b),4),len(c),codes))"
        )
        r = subprocess.run([sys.executable, "-c", code], cwd="/opt/kr-quant",
                           capture_output=True, text=True, check=True)
        result = next((x for x in r.stdout.splitlines() if x.startswith("RESULT\t")), None)
        if result is None:
            raise RuntimeError(f"스캐너 결과 라인 없음. stdout={r.stdout!r} stderr={r.stderr[-500:]!r}")
        _, asof, breadth_s, n_s, codes = result.split("\t", 4)
        breadth, n = float(breadth_s), int(n_s)
        regime = "risk_on" if breadth > 0.5 else "risk_off"
        done = set()
        if os.path.exists(OUT):
            for row in csv.reader(open(OUT)):
                if row:
                    done.add(row[0])
        if asof in done:
            print(f"{asof} already logged")
            return
        with open(OUT, "a", newline="") as f:
            csv.writer(f).writerow([asof, breadth, regime, n, codes])
        print(f"{asof}: breadth={breadth:.0%} {regime} cand={n}")

    @task
    def track_rba() -> None:
        """전일까지 픽의 실현결과를 RBA 로그에 누적 (미너비니 조언)."""
        subprocess.run([sys.executable,
            "/opt/kr-quant/research/operator_flow/minervini/rba_tracker.py",
            "--db", _dsn()], cwd="/opt/kr-quant", check=False)

    scan_and_log() >> track_rba()


daily_minervini_scan()
