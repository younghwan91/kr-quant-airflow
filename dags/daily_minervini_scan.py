"""일일 미너비니 규칙 스캐너 → 후보 + 레짐을 DB에 누적 (RBA 축적).

GOAL 루프 1-14에서 수렴한 순수 규칙 시스템(research/operator_flow/minervini/)을 매
거래일 장 마감 후 실행해 (1) 시장 breadth 레짐 (2) 오늘의 진입 후보를 한 줄씩 upsert.
미너비니의 최종 조언 — 이론 기대치(TBA)가 아니라 실제 매매 결과(RBA)로 리스크를 설계하려면
실현 결과 데이터를 축적해야 한다 — 를 실행하는 파이프라인.

minervini_scan 테이블(PK: date, 일반 테이블 — 거래일당 1행뿐이라 하이퍼테이블 이점
없음)에 upsert — daily_bars·earnings 등과 SQL로 조인 가능하게(README "다른 데이터랑
같이" 목표). collectors/rba_tracker.py도 이 테이블을 직접 읽으므로 CSV 출력은 없다.

scanner_final.py(전략 로직, GOAL 루프 1-14 산물)는 kr-quant에 그대로 남아 있어
/opt/kr-quant 마운트를 통해 접근한다 — 콜렉터 이전과 무관하게 analysis 코드는 계속
private 레포에 있는다. DB 쓰기(upsert_minervini_scan)와 rba_tracker.py는 순수 DB
I/O라 kr-quant-airflow/collectors/로 이전됨.

무인증(DB만), Kiwoom 자격증명 불필요.
"""

from __future__ import annotations

import os
import subprocess
import sys

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from _common import run_collector, timescale_dsn


@dag(
    dag_id="daily_minervini_scan",
    schedule="40 18 * * 1-5",  # 평일 18:40 KST — 시세·수급 확정 후
    start_date=pendulum.datetime(2026, 7, 11, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kr-quant", "minervini", "scan"],
)
def daily_minervini_scan():

    @task(retries=1, retry_delay=timedelta(minutes=10))
    def scan_and_log() -> None:
        # 스캐너 결과를 sentinel 접두 한 줄로 방출 — 후보 0(약세 레짐)일 때 빈 codes로도
        # 안전하게 파싱된다. 위치 기반 4줄 파싱은 빈 줄이 필터링돼 IndexError를 냈었다.
        code = (
            "import os,psycopg2,sys;sys.path.insert(0,'/opt/kr-quant/research/operator_flow/minervini');"
            "from scanner_final import scan;"
            # DSN은 env로 넘긴다 — 코드 문자열에 박으면 `python -c '<DSN 포함>'`이
            # 프로세스 argv(ps)와 예외 메시지에 비밀번호째로 노출된다.
            "con=psycopg2.connect(os.environ['TS_DSN']);"
            "a,b,c=scan(con);con.close();"
            "codes=','.join(c['code'].tolist()) if len(c) else '';"
            "print('RESULT\\t%s\\t%s\\t%d\\t%s'%(a,round(float(b),4),len(c),codes))"
        )
        # 여기만 run_collector를 안 쓴다 — stdout의 RESULT 라인을 파싱해야 해서
        # 스트리밍이 아니라 capture_output이 필요하다.
        r = subprocess.run([sys.executable, "-c", code], cwd="/opt/kr-quant",
                           env={**os.environ, "TS_DSN": timescale_dsn()},
                           capture_output=True, text=True, check=True)
        result = next((x for x in r.stdout.splitlines() if x.startswith("RESULT\t")), None)
        if result is None:
            raise RuntimeError(f"스캐너 결과 라인 없음. stdout={r.stdout!r} stderr={r.stderr[-500:]!r}")
        _, asof, breadth_s, n_s, codes = result.split("\t", 4)
        breadth, n = float(breadth_s), int(n_s)
        regime = "risk_on" if breadth > 0.5 else "risk_off"

        sys.path.insert(0, "/opt/airflow")
        from collectors.storage import connect, upsert_minervini_scan
        db_con = connect(timescale_dsn())
        upsert_minervini_scan(db_con, [(asof, breadth, regime, n, codes)])  # PK(date) upsert — 재실행해도 안전
        db_con.close()

        print(f"{asof}: breadth={breadth:.0%} {regime} cand={n}")

    @task(retries=1, retry_delay=timedelta(minutes=10))
    def track_rba() -> None:
        """전일까지 픽의 실현결과를 RBA 로그에 누적 (미너비니 조언).

        **check=False였던 것을 되돌린 이유:** 실패를 통째로 삼켜서 rba_tracker가
        무슨 이유로든 죽어도 태스크가 초록불이었고, 출력도 캡처되지 않아 로그에
        아무 흔적이 없었다. ``minervini_rba`` 테이블이 0행인 채로 방치된 원인이다.
        마지막 태스크라 실패해도 앞선 스캔 결과(upsert 완료)는 보존된다 — 조용히
        틀리는 것보다 빨갛게 실패하는 편이 낫다.
        """
        run_collector([
            sys.executable, "-m", "collectors.rba_tracker",
            "--db", timescale_dsn(),
        ])

    scan_and_log() >> track_rba()


daily_minervini_scan()
