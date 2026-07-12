"""RBA 추적기 — 스캐너 픽의 실제 실현 결과를 기록 (미너비니 최종 조언).

미너비니: "이론적 가정(TBA)이 아니라 실제 매매 결과 데이터(RBA)로 리스크를 설계하라."
daily_minervini_scan DAG가 DB에 쌓은 minervini_scan 테이블(날짜별 진입후보)을 읽고,
각 픽에 대해 진입 다음 거래일 시가 기준 5% 손절 / +10%(2R) 목표 / 20일 이내 결과를
판정해 minervini_rba 테이블에 누적한다. 축적되면 실전 승률·기대값이 백테스트와
일치하는지 검증 가능.

kr-quant(전략/피처 라이브러리)가 아니라 kr-quant-airflow/collectors/에 있는 이유:
순수 DB 읽기(daily_bars, minervini_scan)/쓰기(minervini_rba)만 하고 strategies/features를
전혀 import하지 않는다 — daily_minervini_scan DAG가 직접 subprocess로 호출하는
수집/기록 파이프라인이라 콜렉터와 같은 성격.

CLI: python rba_tracker.py --db <DSN>
"""

from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

HARD = 0.05  # 5% 손절
TARGET = 0.10  # +10% = 2R
HMAX = 20  # 최대 4주


def evaluate(con, picks_by_date: dict[str, list[str]], already: set) -> list[list]:
    """각 (date, code) 픽의 실현 결과 판정. 충분한 forward 데이터가 있는 것만."""
    codes = sorted({c for cs in picks_by_date.values() for c in cs})
    if not codes:
        return []
    px = pd.read_sql(
        "SELECT code,date,open,high,low,close FROM daily_bars WHERE code = ANY(%(c)s) "
        "AND date > (SELECT MIN(date) FROM daily_bars WHERE date >= %(d0)s)",
        con, params={"c": codes, "d0": min(picks_by_date)})
    px["date"] = px["date"].astype(str)
    out = []
    for pick_date, cs in picks_by_date.items():
        for code in cs:
            key = f"{pick_date}:{code}"
            if key in already:
                continue
            g = px[px["code"] == code].sort_values("date")
            fwd = g[g["date"] > pick_date].head(HMAX + 1)
            if len(fwd) < HMAX:  # 아직 결과 미확정 → 스킵(다음 실행에)
                continue
            o = fwd["open"].to_numpy(float); hi_a = fwd["high"].to_numpy(float)
            lo_a = fwd["low"].to_numpy(float); cl_a = fwd["close"].to_numpy(float)
            raw_c = fwd["close"].to_numpy(float)  # 분할 탐지용 원본(수정 안 함)
            # 보유창 내 액면분할 전방조정 — 한국 ±30% 제한 넘는 종가변동은 분할이므로
            # 분할일 이후 가격을 entry 기준으로 되돌려(1/비율) 가짜 손절/익절을 막는다.
            fac = 1.0
            for k in range(1, len(raw_c)):
                if np.isfinite(raw_c[k]) and np.isfinite(raw_c[k - 1]) and raw_c[k - 1] > 0:
                    r = raw_c[k] / raw_c[k - 1]
                    if r < 0.70 or r > 1.4286:
                        fac *= r  # 이후 raw가격은 r배 축소 → 1/r로 복원
                o[k] /= fac; hi_a[k] /= fac; lo_a[k] /= fac; cl_a[k] /= fac
            entry = o[0]  # 다음날 시가 진입
            if not np.isfinite(entry) or entry <= 0:
                continue
            stop = entry * (1 - HARD); tgt = entry * (1 + TARGET)
            outcome = "open"; exit_px = cl_a[-1]; days = HMAX
            for k in range(1, len(fwd)):
                lo, hi = lo_a[k], hi_a[k]
                if np.isfinite(lo) and lo <= stop:
                    outcome = "stop"; exit_px = stop; days = k; break
                if np.isfinite(hi) and hi >= tgt:
                    outcome = "target_2R"; exit_px = tgt; days = k; break
            ret = exit_px / entry - 1
            out.append([pick_date, code, round(entry), round(exit_px), outcome,
                        round(ret * 100, 1), days])
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="RBA 추적기 — 스캐너 픽 실현결과 기록")
    ap.add_argument("--db", default=None)
    args = ap.parse_args()
    from .storage import connect, default_db_path, upsert_minervini_rba
    con = connect(args.db or str(default_db_path()))
    scanned = pd.read_sql_query(
        "SELECT date, codes FROM minervini_scan WHERE regime = 'risk_on' AND codes <> ''", con)
    if scanned.empty:
        print("risk_on 픽 없음(DAG 미실행 또는 전부 risk_off) — RBA 축적 대기")
        con.close()
        return 0
    picks = {row["date"]: row["codes"].split(",") for _, row in scanned.iterrows()}
    already_df = pd.read_sql_query("SELECT pick_date, code FROM minervini_rba", con)
    already = {f"{r.pick_date}:{r.code}" for r in already_df.itertuples()}
    rows = evaluate(con, picks, already)
    if rows:
        upsert_minervini_rba(con, [tuple(r) for r in rows])
    # 누적 RBA 요약
    df = pd.read_sql_query("SELECT * FROM minervini_rba", con)
    con.close()
    if not df.empty:
        wins = (df["outcome"] == "target_2R").sum(); n = len(df)
        if n:
            wr = wins / n
            print(f"RBA 누적: {n}건, 2R승률 {wr:.0%}, 평균수익 {df['ret_pct'].mean():+.1f}%, "
                  f"기대값 {3*wr-1:+.2f}R (백테스트 base 43% 대조)")
    print(f"신규 판정 {len(rows)}건 기록")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
