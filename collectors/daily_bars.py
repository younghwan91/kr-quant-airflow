"""Collect per-stock daily OHLCV bars (전종목 일봉) into SQLite.

Source: ``ka10081`` (주식일봉차트요청) — one request returns up to ~600 daily
candles (about 2.5 years), so a single call backfills full history for most
stocks. Because this is a single TR, the kiwoom-client per-TR rate limiter
throttles to ~1 req/s; a full KOSPI+KOSDAQ common-stock sweep (~2,600 stocks)
takes roughly 45 minutes. Threads do **not** help — the per-TR bucket
serializes them — so the win is making each call count (600 rows/call) and
``--resume`` to skip stocks already backfilled.

CLI:
    kq-collect-daily --market all                 # mock, full history backfill
    kq-collect-daily --market all --prod          # real-server keys
    kq-collect-daily --resume                      # resume interrupted backfill
    kq-collect-daily --update                      # daily incremental (append new bars)
    kq-collect-daily --days 120                    # keep only last 120 days
"""

from __future__ import annotations

import argparse
import sqlite3
import time

from typing import Any

from kiwoom_rest_api import KiwoomAPI
from kiwoom_rest_api.base import KiwoomAPIError

from .config import make_api, mask_dsn
from .storage import (
    _is_pg,
    connect,
    default_db_path,
    to_int,
    upsert_daily_bars,
    upsert_stocks,
)
from .supply_demand import fetch_stock_list, is_common_stock

# ka10081 response: list key and per-row field map → DB columns.
_CHART_KEY = "stk_dt_pole_chart_qry"


def _fetchone(con: Any, sql: str, params: tuple) -> tuple | None:
    """sqlite3/psycopg2 양쪽에서 한 행을 읽는다 (``?`` 파라미터로 통일).

    sqlite3는 ``con.execute()``와 ``?``를, psycopg2는 ``con.cursor().execute()``와
    ``%s``를 쓴다. 이 차이 때문에 ``--update`` 경로가 Postgres에서
    ``AttributeError: 'connection' object has no attribute 'execute'``로 죽었고,
    그게 ``daily_collection_catchup``이 paused로 방치돼 있던 원인이다(2026-07-17 실측).
    """
    if _is_pg(con):
        with con.cursor() as cur:
            cur.execute(sql.replace("?", "%s"), params)
            return cur.fetchone()
    return con.execute(sql, params).fetchone()


def _has_any_rows(con: Any, code: str) -> bool:
    return _fetchone(con, "SELECT 1 FROM daily_bars WHERE code=? LIMIT 1", (code,)) is not None


def _latest_date(con: Any, code: str) -> str | None:
    """Most recent stored bar date for ``code`` (YYYYMMDD), or None if empty."""
    row = _fetchone(con, "SELECT MAX(date) FROM daily_bars WHERE code=?", (code,))
    v = row[0] if row else None
    if v is None:
        return None
    # sqlite는 date를 TEXT("20260716")로 보관하지만 Postgres는 date 타입이라
    # psycopg2가 datetime.date를 돌려준다. 호출부가 market_latest("20260716")와
    # 부등호 비교하므로 YYYYMMDD 문자열로 맞춘다 — 안 그러면 date vs str TypeError.
    return v.strftime("%Y%m%d") if hasattr(v, "strftime") else str(v)


# Liquid reference stock (삼성전자) used to learn the market's latest bar date.
_REF_CODE = "005930"


def _market_latest_date(api: KiwoomAPI, base_dt: str, ref_code: str = _REF_CODE) -> str:
    """Latest available daily-bar date on the server, via a liquid reference.

    Lets ``--update`` skip stocks already at the newest trading day so same-day
    re-runs are near-instant. Falls back to ``base_dt`` if the probe fails.
    """
    try:
        resp = api.chart.stock_daily_chart(
            stk_cd=ref_code, base_dt=base_dt, upd_stkpc_tp="1"
        )
        rows = resp.get(_CHART_KEY) or []
        if rows:
            return rows[0].get("dt", base_dt)
    except Exception:  # noqa: BLE001 — fall back to today on any probe failure
        pass
    return base_dt


def _row_to_record(code: str, row: dict) -> tuple:
    # Prices may carry a direction sign (e.g. cur_prc); store absolute price.
    return (
        code,
        row.get("dt", ""),
        abs(to_int(row.get("open_pric"))),
        abs(to_int(row.get("high_pric"))),
        abs(to_int(row.get("low_pric"))),
        abs(to_int(row.get("cur_prc"))),
        to_int(row.get("trde_qty")),
        to_int(row.get("trde_prica")),
    )


def collect(
    api: KiwoomAPI,
    con: sqlite3.Connection,
    stocks: list[dict],
    *,
    days: int = 0,
    max_pages: int = 1,
    resume: bool = False,
    update: bool = False,
    progress_every: int = 50,
) -> dict[str, int]:
    """Collect daily bars for ``stocks`` into the DB. Returns a summary dict.

    Args:
        days: Keep only the most recent N days. 0 = keep everything returned.
        max_pages: Continuation pages per stock (each ~600 bars). 1 is plenty
            for ~2.5y; raise to go deeper into history.
        resume: Skip stocks that already have at least one stored bar.
        update: Incremental mode — per stock, skip if already current and
            otherwise append only bars newer than the latest stored one
            (still one call/stock; a single call backfills multi-day gaps).
    """
    cutoff = (
        time.strftime("%Y%m%d", time.localtime(time.time() - days * 86400))
        if days > 0
        else ""
    )
    base_dt = time.strftime("%Y%m%d")
    # In update mode, learn the newest trading day once so we can skip stocks
    # already current (makes same-day re-runs near-instant).
    market_latest = _market_latest_date(api, base_dt) if update else base_dt
    if update:
        print(f"📅 시장 최신 거래일: {market_latest}")
    stats = {"done": 0, "skipped": 0, "failed": 0, "rows": 0}
    started = time.monotonic()

    for i, stock in enumerate(stocks, 1):
        code = stock["code"]
        if resume and _has_any_rows(con, code):
            stats["skipped"] += 1
            continue
        # Incremental: stop pulling at the last bar we already have; skip
        # entirely if already at the latest trading day (nothing new).
        lower = ""
        if update:
            latest = _latest_date(con, code)
            if latest is not None and latest >= market_latest:
                stats["skipped"] += 1
                continue
            lower = latest or ""
        try:
            records: list[tuple] = []
            cont_yn, next_key = "N", ""
            for _ in range(max_pages):
                resp = api.chart.stock_daily_chart(
                    cont_yn=cont_yn,
                    next_key=next_key,
                    stk_cd=code,
                    base_dt=base_dt,
                    upd_stkpc_tp="1",
                )
                stop = False
                for row in resp.get(_CHART_KEY, []) or []:
                    dt = row.get("dt", "")
                    if not dt:  # malformed row — skip, don't poison the batch insert
                        continue
                    if lower and dt <= lower:  # already stored — stop (newest-first)
                        stop = True
                        break
                    if cutoff and dt < cutoff:
                        stop = True
                        break
                    records.append(_row_to_record(code, row))
                resp_cont = resp.get("cont_yn") or resp.get("cont-yn", "N")
                resp_next = resp.get("next_key") or resp.get("next-key", "")
                if stop or resp_cont != "Y" or not resp_next:
                    break
                cont_yn, next_key = "Y", resp_next
            stats["rows"] += upsert_daily_bars(con, records)
            stats["done"] += 1
        except KiwoomAPIError as e:
            stats["failed"] += 1
            print(f"  ⚠️ {code} {stock['name']}: rc={e.code} {e.message[:50]}")
        except Exception as e:  # noqa: BLE001 — isolate per-stock failures
            stats["failed"] += 1
            print(f"  💥 {code} {stock['name']}: {type(e).__name__}: {e}")

        if i % progress_every == 0 or i == len(stocks):
            elapsed = time.monotonic() - started
            rate = i / elapsed if elapsed else 0
            eta = (len(stocks) - i) / rate / 60 if rate else 0
            print(
                f"  [{i}/{len(stocks)}] done={stats['done']} skip={stats['skipped']} "
                f"fail={stats['failed']} | {stats['rows']:,} rows | "
                f"{rate:.1f} stk/s | ETA {eta:.1f}m"
            )
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="키움 전종목 일봉 SQLite 수집기")
    parser.add_argument("--prod", action="store_true", help="실서버 사용 (기본: 모의)")
    parser.add_argument("--market", choices=["kospi", "kosdaq", "all"], default="all")
    parser.add_argument(
        "--days", type=int, default=0,
        help="최근 N일만 저장 (0=콜이 주는 전체 ~2.5년, 기본 0)",
    )
    parser.add_argument(
        "--max-pages", type=int, default=1,
        help="종목당 연속조회 페이지 수 (페이지당 ~600봉). 더 깊은 과거는 ↑",
    )
    parser.add_argument("--limit", type=int, default=0, help="앞에서 N종목만 (테스트)")
    parser.add_argument("--db", default=str(default_db_path()))
    parser.add_argument(
        "--resume", action="store_true", help="이미 봉이 있는 종목 건너뜀 (백필 재개)"
    )
    parser.add_argument(
        "--update", action="store_true",
        help="증분: 종목별 최신봉 이후만 추가, 이미 최신이면 건너뜀 (일일 갱신)",
    )
    parser.add_argument(
        "--all-kinds", action="store_true",
        help="ETF/ETN/리츠/우선주 등 모두 포함 (기본: 보통주만)",
    )
    parser.add_argument(
        "--rate", type=float, default=0.9,
        help="TR당 요청 속도(req/s). 긴 전수 수집의 429 방지를 위해 기본 0.9",
    )
    args = parser.parse_args()

    con = connect(args.db)
    api = make_api(is_mock=not args.prod, rate_limit=args.rate, max_retries=5)

    markets = ["kospi", "kosdaq"] if args.market == "all" else [args.market]
    stocks = fetch_stock_list(api, markets)
    if not args.all_kinds:
        stocks = [s for s in stocks if is_common_stock(s)]
    if args.limit:
        stocks = stocks[: args.limit]

    upsert_stocks(con, stocks)
    server = "모의" if not args.prod else "실서버"
    window = "전체(~2.5년)" if args.days == 0 else f"최근 {args.days}일"
    print(f"🔌 {server} | 시장={args.market} | 종목 {len(stocks)}개 | {window}")
    print(f"💾 {mask_dsn(args.db)}\n")

    stats = collect(
        api, con, stocks, days=args.days, max_pages=args.max_pages,
        resume=args.resume, update=args.update,
    )

    api.close()
    con.close()
    print(
        f"\n✅ 완료: done={stats['done']} skip={stats['skipped']} "
        f"fail={stats['failed']} rows={stats['rows']:,}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
