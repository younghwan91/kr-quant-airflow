"""Collect analyst consensus (목표주가·투자의견) from Naver Finance.

Kiwoom's broker API has no analyst consensus; Naver Finance exposes it (FnGuide-
sourced) via the mobile integration endpoint, no auth required. This is the
forward-looking signal PEAD lacks — target-price implied upside and, once a time
series accumulates, **consensus revisions** (the re-rating that drives mega-caps
where post-earnings drift is arbitraged away). See docs/pead-strategy.md.

The endpoint is a **current snapshot**, so this collector is meant to run daily,
appending a date-stamped row per code to build the revision time series over
time. ``parse_consensus`` is pure (JSON in → numbers out) and unit-tested.
Writes CSV: date,code,target_mean,recomm_mean,base_date.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
import urllib.request
from datetime import date

BASE = "https://m.stock.naver.com/api/stock"
UA = ("Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) "
      "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148")


def _to_float(s: object) -> float | None:
    txt = str(s or "").replace(",", "").strip()
    try:
        return float(txt)
    except ValueError:
        return None


def parse_consensus(payload: dict) -> tuple[float | None, float | None, str | None]:
    """Extract (target_price_mean, recomm_mean, base_date) from an integration response.

    Args:
        payload: Parsed Naver ``/stock/{code}/integration`` JSON.

    Returns:
        ``(target_mean, recomm_mean, base_date)`` from ``consensusInfo``, or
        ``(None, None, None)`` when the stock has no analyst coverage.
        ``recomm_mean`` is Naver's 1–5 scale (5 = strong buy). ``target_mean`` is
        the mean 12m target price (원). ``base_date`` is the consensus as-of date.
    """
    ci = payload.get("consensusInfo") or {}
    return (
        _to_float(ci.get("priceTargetMean")),
        _to_float(ci.get("recommMean")),
        ci.get("createDate") or None,
    )


def parse_estimate(payload: dict) -> tuple[float | None, float | None, str | None]:
    """Extract forward EPS consensus from a ``finance/annual`` response.

    Naver marks future periods with ``isConsensus == "Y"`` and fills in analyst
    estimates. This returns next year's **estimated EPS** and the most recent
    **actual EPS** (prior year), so the caller can form an *expected growth*
    signal — the forward-looking analogue of PEAD that (unlike backward earnings)
    can work in mega-caps, where the market prices future expectations.

    Returns:
        ``(fwd_eps, prev_eps, est_year)`` — estimated EPS for the consensus year,
        the latest actual EPS, and the estimate year key (e.g. "202612"); or
        ``(None, None, None)`` if no consensus year / EPS row is present.
    """
    fi = payload.get("financeInfo") or {}
    titles = fi.get("trTitleList") or []
    cons = [t.get("key") for t in titles if t.get("isConsensus") == "Y"]
    actuals = [t.get("key") for t in titles if t.get("isConsensus") != "Y"]
    if not cons:
        return None, None, None
    est_year = cons[0]

    def _row_name(r: dict) -> str:
        t = r.get("title")
        return t.get("name", "") if isinstance(t, dict) else str(t or "")

    eps_row = next((r for r in fi.get("rowList", []) if _row_name(r) == "EPS"), None)
    if eps_row is None:
        return None, None, None
    cols = eps_row.get("columns") or {}
    fwd = _to_float((cols.get(est_year) or {}).get("value"))
    prev = _to_float((cols.get(actuals[-1]) or {}).get("value")) if actuals else None
    return fwd, prev, est_year


def _get_json(url: str, *, retries: int = 3) -> dict | None:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    for _ in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode())
        except Exception:
            time.sleep(1.0)
    return None


def fetch_consensus(code: str) -> tuple[float | None, float | None, str | None]:
    """Fetch (target_mean, recomm_mean, base_date) for one code from Naver."""
    d = _get_json(f"{BASE}/{code}/integration")
    return parse_consensus(d) if d else (None, None, None)


def fetch_estimate(code: str) -> tuple[float | None, float | None, str | None]:
    """Fetch (fwd_eps, prev_eps, est_year) forward consensus for one code."""
    d = _get_json(f"{BASE}/{code}/finance/annual")
    return parse_estimate(d) if d else (None, None, None)


def _universe_query(args: argparse.Namespace) -> tuple[str, dict]:
    """SQL (+ params) selecting the code universe: all ``daily_bars`` codes or top-N liquid.

    ``--all-codes`` uses a plain ``DISTINCT code`` scan with no recency window, so a
    stock that just IPO'd today (and so has only today's row in ``daily_bars``) is
    included from day one — no special-casing needed for newly listed codes.
    """
    if args.all_codes:
        return "SELECT DISTINCT code FROM daily_bars ORDER BY code", {}
    return (
        "SELECT code FROM daily_bars WHERE date >= (SELECT MAX(date) FROM daily_bars) - INTERVAL '90 days' "
        "GROUP BY code ORDER BY AVG(trade_value) DESC LIMIT %(n)s",
        {"n": args.top_n},
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="네이버 애널리스트 컨센서스 수집 (목표주가·투자의견, 일별 스냅샷)")
    ap.add_argument("--out", default=None, help="출력 CSV (일별 append)")
    ap.add_argument("--top-n", type=int, default=800, help="유동성 상위 N종목")
    ap.add_argument("--all-codes", action="store_true", help="유동성 상위 N 대신 daily_bars 전종목 사용")
    ap.add_argument("--db-table", action="store_true", help="CSV 대신 consensus 테이블에 직접 upsert")
    ap.add_argument("--db", default=None)
    ap.add_argument("--sleep", type=float, default=0.2)
    args = ap.parse_args()
    if not args.db_table and not args.out:
        ap.error("--out is required unless --db-table is set")

    import pandas as pd
    from .storage import connect, default_db_path, upsert_consensus
    con = connect(args.db or str(default_db_path()))
    sql, params = _universe_query(args)
    top = pd.read_sql_query(sql, con, params=params)
    codes = top["code"].tolist()

    today = date.today().isoformat()
    done: set[str] = set()
    if args.db_table:
        existing = pd.read_sql_query(
            "SELECT code FROM consensus WHERE date = %(d)s", con, params={"d": today})
        done = set(existing["code"])
    else:
        con.close()
        if os.path.exists(args.out):
            for r in csv.reader(open(args.out)):
                if r and r[0] == today:
                    done.add(r[1])  # (date, code) already collected today

    f = open(args.out, "a", newline="") if args.out else None
    w = csv.writer(f) if f else None
    n = 0
    for i, code in enumerate(codes, 1):
        if code in done:
            continue
        tm, rm, bd = fetch_consensus(code)
        time.sleep(args.sleep)
        fe, pe, ey = fetch_estimate(code)
        time.sleep(args.sleep)
        if tm is None and rm is None and fe is None:
            continue

        if args.db_table:
            upsert_consensus(con, [(code, today, tm, rm, bd, fe, pe, ey)])
        else:
            def _s(x: object) -> object:
                return x if x is not None else ""
            w.writerow([today, code, _s(tm), _s(rm), bd or "", _s(fe), _s(pe), ey or ""])
        n += 1
        if i % 50 == 0:
            if f:
                f.flush()
            print(f"[{i}/{len(codes)}] rows={n}", flush=True)
    if f:
        f.close()
    if args.db_table:
        con.close()
    print(f"DONE date={today} rows={n}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
