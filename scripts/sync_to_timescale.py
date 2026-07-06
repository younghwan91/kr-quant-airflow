"""One-way sync: kr-quant's local sqlite -> shared TimescaleDB.

Collectors (kr-quant) stay sqlite-only; this pushes a recent window of rows
into the LAN-exposed TimescaleDB so another host can query current data
without touching the sqlite file directly. Column lists are duplicated from
kr_quant/storage.py rather than imported, so this repo doesn't break if
kr-quant's internals change shape.

Usage:
    python sync_to_timescale.py --sqlite /path/to/kr_quant.db --days 7
    python sync_to_timescale.py --sqlite /path/to/kr_quant.db --full
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import time
from datetime import date, timedelta

import psycopg2
import psycopg2.extras

STOCKS_COLS = ["code", "name", "market", "sector", "kind"]

DAILY_BAR_COLS = ["code", "date", "open", "high", "low", "close", "volume", "trade_value"]

SUPPLY_DEMAND_COLS = [
    "code", "date", "close", "flu_rt", "acc_trde_qty",
    "individual", "foreign_", "institution", "fnnc_invt", "insrnc",
    "invtrt", "bank", "penfnd_etc", "samo_fund", "natn", "etc_corp",
]

SHORT_SELLING_COLS = [
    "code", "date", "close", "volume",
    "short_qty", "short_balance", "short_ratio", "short_avg_price", "short_value",
]

CREDIT_BALANCE_COLS = [
    "code", "date", "close",
    "new_qty", "repay_qty", "balance_qty", "balance_amt", "balance_rt", "credit_rt",
]

TABLES: dict[str, list[str]] = {
    "daily_bars": DAILY_BAR_COLS,
    "supply_demand": SUPPLY_DEMAND_COLS,
    "short_selling": SHORT_SELLING_COLS,
    "credit_balance": CREDIT_BALANCE_COLS,
}


def pg_dsn() -> str:
    return (
        f"host={os.environ['TIMESCALE_HOST']} port={os.environ.get('TIMESCALE_PORT', '5432')} "
        f"dbname={os.environ['TIMESCALE_DB']} user={os.environ['TIMESCALE_USER']} "
        f"password={os.environ['TIMESCALE_PASSWORD']}"
    )


def _to_date(value: str) -> date:
    """sqlite stores dates as 'YYYYMMDD' text; TimescaleDB columns are DATE."""
    return date(int(value[:4]), int(value[4:6]), int(value[6:8]))


def sync_stocks(sq: sqlite3.Connection, pg: psycopg2.extensions.connection) -> int:
    rows = sq.execute(f"SELECT {','.join(STOCKS_COLS)} FROM stocks").fetchall()
    if not rows:
        return 0
    update_cols = [c for c in STOCKS_COLS if c != "code"]
    with pg.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f"INSERT INTO stocks({','.join(STOCKS_COLS)}) VALUES %s "
            f"ON CONFLICT (code) DO UPDATE SET " + ",".join(f"{c}=EXCLUDED.{c}" for c in update_cols),
            rows,
        )
    pg.commit()
    return len(rows)


def sync_table(
    sq: sqlite3.Connection,
    pg: psycopg2.extensions.connection,
    table: str,
    cols: list[str],
    cutoff: str,
) -> int:
    rows = sq.execute(f"SELECT {','.join(cols)} FROM {table} WHERE date >= ?", (cutoff,)).fetchall()
    if not rows:
        return 0
    converted = [
        tuple(_to_date(v) if c == "date" else v for c, v in zip(cols, row))
        for row in rows
    ]
    update_cols = [c for c in cols if c not in ("code", "date")]
    with pg.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f"INSERT INTO {table}({','.join(cols)}) VALUES %s "
            f"ON CONFLICT (code, date) DO UPDATE SET " + ",".join(f"{c}=EXCLUDED.{c}" for c in update_cols),
            converted,
        )
    pg.commit()
    return len(converted)


def main() -> int:
    parser = argparse.ArgumentParser(description="sqlite -> TimescaleDB 증분 동기화")
    parser.add_argument("--sqlite", default=os.environ.get("KR_QUANT_SQLITE_PATH"))
    parser.add_argument(
        "--days", type=int, default=7,
        help="최근 N일만 동기화 (기본 7 — 재시도/backfill 여유 포함, upsert라 겹쳐도 안전)",
    )
    parser.add_argument("--full", action="store_true", help="전체 히스토리 동기화 (최초 1회 부트스트랩용)")
    args = parser.parse_args()

    if not args.sqlite:
        raise SystemExit("--sqlite 또는 KR_QUANT_SQLITE_PATH 환경변수가 필요합니다.")

    cutoff = "19000101" if args.full else (date.today() - timedelta(days=args.days)).strftime("%Y%m%d")

    sq = sqlite3.connect(args.sqlite)
    pg = psycopg2.connect(pg_dsn())
    started = time.monotonic()
    try:
        n_stocks = sync_stocks(sq, pg)
        totals = {table: sync_table(sq, pg, table, cols, cutoff) for table, cols in TABLES.items()}
    finally:
        sq.close()
        pg.close()

    elapsed = time.monotonic() - started
    print(f"✅ sync 완료 ({elapsed:.1f}s) stocks={n_stocks} " + " ".join(f"{t}={n}" for t, n in totals.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
