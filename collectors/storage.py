"""Storage layer for collected datasets — sqlite or Postgres/TimescaleDB (write side).

Defines the schema and upsert helpers used by ``collectors/*.py``. Collectors
produce plain records; this module persists them idempotently on natural
keys. ``connect()`` dispatches on the connection string: a
``postgresql://``/``postgres://`` DSN opens Postgres (psycopg2, imported
lazily so sqlite-only use never needs it installed); anything else opens a
local sqlite file exactly as before.

This is an intentionally independent copy of the write-side half of
kr-quant's ``kr_quant/storage.py`` (kr-quant keeps the read-side half:
``connect``/``market_cap_asof``/``market_cap_asof_bulk``) — small enough that
duplicating it is simpler than introducing a shared package between the two
repos.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

_PG_PREFIXES = ("postgresql://", "postgres://")

# ka10059 (투자자기관별종목별) net-buy fields → DB columns.
# Order matters: it defines the column order for ``supply_demand`` inserts.
INVESTOR_COLUMNS: dict[str, str] = {
    "individual": "ind_invsr",   # 개인
    "foreign_": "frgnr_invsr",   # 외국인
    "institution": "orgn",       # 기관계
    "fnnc_invt": "fnnc_invt",    # 금융투자
    "insrnc": "insrnc",          # 보험
    "invtrt": "invtrt",          # 투신
    "bank": "bank",              # 은행
    "penfnd_etc": "penfnd_etc",  # 연기금 등
    "samo_fund": "samo_fund",    # 사모펀드
    "natn": "natn",              # 국가
    "etc_corp": "etc_corp",      # 기타법인
}

SUPPLY_DEMAND_COLUMNS: list[str] = [
    "code",
    "date",
    "close",
    "flu_rt",
    "acc_trde_qty",
    *INVESTOR_COLUMNS.keys(),
]

# ka10081 (주식일봉차트) candle fields → DB columns. Order defines insert order.
DAILY_BAR_COLUMNS: list[str] = [
    "code",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_value",
]

_INVESTOR_COL_DDL = ",\n            ".join(f"{c} INTEGER" for c in INVESTOR_COLUMNS)

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS stocks (
    code   TEXT PRIMARY KEY,
    name   TEXT,
    market TEXT,
    sector TEXT,
    kind   TEXT
);
CREATE TABLE IF NOT EXISTS supply_demand (
    code         TEXT NOT NULL,
    date         TEXT NOT NULL,
    close        INTEGER,
    flu_rt       REAL,
    acc_trde_qty INTEGER,
    {_INVESTOR_COL_DDL},
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_sd_date ON supply_demand(date);
CREATE TABLE IF NOT EXISTS daily_bars (
    code        TEXT NOT NULL,
    date        TEXT NOT NULL,
    open        INTEGER,
    high        INTEGER,
    low         INTEGER,
    close       INTEGER,
    volume      INTEGER,
    trade_value INTEGER,
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_db_date ON daily_bars(date);
CREATE TABLE IF NOT EXISTS short_selling (
    code            TEXT NOT NULL,
    date            TEXT NOT NULL,
    close           INTEGER,
    volume          INTEGER,
    short_qty       INTEGER,   -- 당일 공매도 수량 (shrts_qty)
    short_balance   INTEGER,   -- 공매도 잔고 수량 (ovr_shrts_qty)
    short_ratio     REAL,      -- 공매도 비중 % (trde_wght)
    short_avg_price INTEGER,   -- 공매도 평균가 (shrts_avg_pric)
    short_value     INTEGER,   -- 공매도 거래대금 (shrts_trde_prica)
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_ss_date ON short_selling(date);
CREATE TABLE IF NOT EXISTS credit_balance (
    code        TEXT NOT NULL,
    date        TEXT NOT NULL,
    close       INTEGER,
    new_qty     INTEGER,   -- 신규 신용매수 (new)
    repay_qty   INTEGER,   -- 상환 (rpya)
    balance_qty INTEGER,   -- 신용잔고 수량 (remn)
    balance_amt INTEGER,   -- 신용잔고 금액 (amt)
    balance_rt  REAL,      -- 신용잔고율 % (remn_rt)
    credit_rt   REAL,      -- 신용비율 % (shr_rt)
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_cb_date ON credit_balance(date);
CREATE TABLE IF NOT EXISTS sector_index (
    code        TEXT NOT NULL,  -- 업종코드 (001=KOSPI 종합, 101=KOSDAQ 종합 등)
    name        TEXT,
    date        TEXT NOT NULL,
    open        INTEGER,
    high        INTEGER,
    low         INTEGER,
    close       INTEGER,
    volume      INTEGER,
    trade_value INTEGER,
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_si_date ON sector_index(date);
CREATE TABLE IF NOT EXISTS shares_outstanding_history (
    code               TEXT NOT NULL,
    date               TEXT NOT NULL,
    shares_outstanding INTEGER,  -- sqlite INTEGER is dynamically 64-bit already;
    PRIMARY KEY (code, date)     -- Postgres side (init_timescale.sql) must use BIGINT, not INTEGER(32bit) — 삼성전자(58억주) overflows it
);
CREATE INDEX IF NOT EXISTS idx_sh_date ON shares_outstanding_history(date);
CREATE TABLE IF NOT EXISTS earnings (
    code            TEXT NOT NULL,
    period          TEXT NOT NULL,   -- e.g. '2020Q1'
    avail_date      TEXT,            -- lookahead-safe availability date (period-end + filing lag)
    netinc          REAL,
    netinc_prior    REAL,
    revenue         REAL,
    revenue_prior   REAL,
    op_income       REAL,
    op_income_prior REAL,
    PRIMARY KEY (code, period)
);
CREATE INDEX IF NOT EXISTS idx_earnings_avail_date ON earnings(avail_date);
CREATE TABLE IF NOT EXISTS consensus (
    code         TEXT NOT NULL,
    date         TEXT NOT NULL,   -- 스냅샷 수집일 (오늘)
    target_mean  REAL,            -- 목표주가 평균
    recomm_mean  REAL,            -- 투자의견 평균 (1~5, 5=강력매수)
    base_date    TEXT,            -- 컨센서스 기준일(네이버 createDate)
    fwd_eps      REAL,            -- 향후 컨센서스 EPS
    prev_eps     REAL,            -- 직전 확정 EPS
    est_year     TEXT,            -- fwd_eps가 가리키는 연도(예: '202612')
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_consensus_date ON consensus(date);
CREATE TABLE IF NOT EXISTS minervini_scan (
    date        TEXT NOT NULL,
    breadth     REAL,   -- 유동주 close>MA50 비율
    regime      TEXT,   -- 'risk_on' / 'risk_off'
    n_candidates INTEGER,
    codes       TEXT,   -- 콤마구분 진입후보 코드 (없으면 빈 문자열)
    PRIMARY KEY (date)
);
CREATE TABLE IF NOT EXISTS daily_bars_adjusted (
    code        TEXT NOT NULL,
    date        TEXT NOT NULL,
    open        REAL,   -- price_adjust.adjust_prices()의 back-adjust 배수 적용 후이므로
    high        REAL,   -- daily_bars(원자료, INTEGER)와 달리 REAL — 분할비율이 실수라 정수로
    low         REAL,   -- 안 떨어짐(예: 1주→4주 분할이면 종가가 1/4배가 됨)
    close       REAL,
    volume      INTEGER,      -- 기본은 미조정 원본 거래량 그대로(adjust_volume=False)
    trade_value INTEGER,      -- 거래대금은 가격조정과 무관(가격×수량이 아니라 원 보고값)
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_dba_date ON daily_bars_adjusted(date);
CREATE TABLE IF NOT EXISTS delisted_stocks (
    code            TEXT NOT NULL,
    name            TEXT,
    market          TEXT,
    last_trade_date TEXT,   -- daily_bars 기준 마지막 거래일(상장폐지일 근사), 이력 없으면 NULL
    PRIMARY KEY (code)
);
CREATE TABLE IF NOT EXISTS minervini_rba (
    pick_date TEXT NOT NULL,  -- 스캐너가 진입후보로 뽑은 날짜
    code      TEXT NOT NULL,
    entry     REAL,
    exit_px   REAL,
    outcome   TEXT,   -- 'stop' / 'target_2R' / 'open'(20일 경과, 미확정 종료)
    ret_pct   REAL,
    days      INTEGER,
    PRIMARY KEY (pick_date, code)
);
"""


def default_db_path() -> Path:
    """Default DB location: ``<repo>/data/kr_quant.db`` (gitignored)."""
    return Path(__file__).resolve().parents[1] / "data" / "kr_quant.db"


def connect(db_path: str | Path | None = None) -> Any:
    """Open a connection with row access.

    ``db_path`` starting with ``postgresql://``/``postgres://`` opens Postgres
    (e.g. TimescaleDB) via psycopg2. Anything else is treated as a sqlite file
    path (default: ``<repo>/data/kr_quant.db``, dirs created as needed).
    """
    if isinstance(db_path, str) and db_path.startswith(_PG_PREFIXES):
        import psycopg2  # noqa: PLC0415 — optional dep, only needed for this path

        con = psycopg2.connect(db_path)
        # Schema (tables, hypertables, compression policy) is provisioned by
        # sql/init_timescale.sql, not here — init_db() only applies to the
        # sqlite path.
        return con

    path = Path(db_path) if db_path else default_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    init_db(con)
    return con


def init_db(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA)
    con.commit()


def _is_pg(con: Any) -> bool:
    return not isinstance(con, sqlite3.Connection)


def _upsert(
    con: Any,
    table: str,
    cols: list[str],
    records: list[tuple],
    *,
    pk_cols: tuple[str, ...] = ("code", "date"),
) -> int:
    """Insert/replace ``records`` (tuples ordered by ``cols``) into ``table``.

    sqlite: ``INSERT OR REPLACE``. Postgres: ``INSERT ... ON CONFLICT DO
    UPDATE`` on ``pk_cols`` — same natural-key upsert semantics either way.
    """
    if not records:
        return 0
    if _is_pg(con):
        import psycopg2.extras  # noqa: PLC0415 — optional dep, only needed for this path

        update_cols = [c for c in cols if c not in pk_cols]
        set_clause = ",".join(f"{c}=EXCLUDED.{c}" for c in update_cols)
        sql = (
            f"INSERT INTO {table}({','.join(cols)}) VALUES %s "
            f"ON CONFLICT ({','.join(pk_cols)}) DO UPDATE SET {set_clause}"
        )
        try:
            with con.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, records)
        except Exception:
            # A failed statement leaves the whole Postgres transaction aborted
            # until rolled back — without this, every later upsert on this
            # connection fails with InFailedSqlTransaction even for unrelated,
            # valid records (cascading one bad row into the entire run).
            con.rollback()
            raise
    else:
        placeholders = ",".join(["?"] * len(cols))
        sql = f"INSERT OR REPLACE INTO {table}({','.join(cols)}) VALUES({placeholders})"
        con.executemany(sql, records)
    con.commit()
    return len(records)


def to_int(s: object) -> int:
    """Kiwoom numeric strings (``'+322500'``, ``'-1979879'``, ``''``) → int."""
    text = str(s or "").replace("+", "").strip()
    try:
        return int(text)
    except ValueError:
        return 0


def to_float(s: object) -> float:
    text = str(s or "").replace("+", "").strip()
    try:
        return float(text)
    except ValueError:
        return 0.0


_STOCKS_COLS = ["code", "name", "market", "sector", "kind"]


def upsert_stocks(con: Any, stocks: list[dict]) -> int:
    """Insert/replace stock master rows. Returns the number written."""
    records = [tuple(s.get(c) for c in _STOCKS_COLS) for s in stocks]
    return _upsert(con, "stocks", _STOCKS_COLS, records, pk_cols=("code",))


def upsert_supply_demand(con: Any, records: list[tuple]) -> int:
    """Insert/replace supply_demand rows (tuples ordered by SUPPLY_DEMAND_COLUMNS)."""
    return _upsert(con, "supply_demand", SUPPLY_DEMAND_COLUMNS, records)


def upsert_daily_bars(con: Any, records: list[tuple]) -> int:
    """Insert/replace daily_bars rows (tuples ordered by DAILY_BAR_COLUMNS)."""
    return _upsert(con, "daily_bars", DAILY_BAR_COLUMNS, records)


_SHORT_SELLING_COLS = [
    "code", "date", "close", "volume",
    "short_qty", "short_balance", "short_ratio", "short_avg_price", "short_value",
]

_CREDIT_BALANCE_COLS = [
    "code", "date", "close",
    "new_qty", "repay_qty", "balance_qty", "balance_amt", "balance_rt", "credit_rt",
]


def upsert_short_selling(con: Any, records: list[tuple]) -> int:
    """Insert/replace short_selling rows."""
    return _upsert(con, "short_selling", _SHORT_SELLING_COLS, records)


def upsert_credit_balance(con: Any, records: list[tuple]) -> int:
    """Insert/replace credit_balance rows."""
    return _upsert(con, "credit_balance", _CREDIT_BALANCE_COLS, records)


_SECTOR_INDEX_COLS = [
    "code", "name", "date", "open", "high", "low", "close", "volume", "trade_value",
]


def upsert_sector_index(con: Any, records: list[tuple]) -> int:
    """Insert/replace sector_index rows."""
    return _upsert(con, "sector_index", _SECTOR_INDEX_COLS, records)


_SHARES_OUTSTANDING_COLS = ["code", "date", "shares_outstanding"]


def upsert_shares_outstanding(con: Any, records: list[tuple]) -> int:
    """Insert/replace shares_outstanding_history rows."""
    return _upsert(con, "shares_outstanding_history", _SHARES_OUTSTANDING_COLS, records)


_EARNINGS_COLS = [
    "code", "period", "avail_date",
    "netinc", "netinc_prior", "revenue", "revenue_prior", "op_income", "op_income_prior",
]


def upsert_earnings(con: Any, records: list[tuple]) -> int:
    """Insert/replace earnings rows (tuples ordered by _EARNINGS_COLS)."""
    return _upsert(con, "earnings", _EARNINGS_COLS, records, pk_cols=("code", "period"))


_CONSENSUS_COLS = [
    "code", "date", "target_mean", "recomm_mean", "base_date", "fwd_eps", "prev_eps", "est_year",
]


def upsert_consensus(con: Any, records: list[tuple]) -> int:
    """Insert/replace consensus rows (tuples ordered by _CONSENSUS_COLS)."""
    return _upsert(con, "consensus", _CONSENSUS_COLS, records)


_MINERVINI_SCAN_COLS = ["date", "breadth", "regime", "n_candidates", "codes"]


def upsert_minervini_scan(con: Any, records: list[tuple]) -> int:
    """Insert/replace minervini_scan rows (tuples ordered by _MINERVINI_SCAN_COLS)."""
    return _upsert(con, "minervini_scan", _MINERVINI_SCAN_COLS, records, pk_cols=("date",))


def upsert_daily_bars_adjusted(con: Any, records: list[tuple]) -> int:
    """Insert/replace daily_bars_adjusted rows (tuples ordered by DAILY_BAR_COLUMNS)."""
    return _upsert(con, "daily_bars_adjusted", DAILY_BAR_COLUMNS, records)


_DELISTED_STOCKS_COLS = ["code", "name", "market", "last_trade_date"]


def upsert_delisted_stocks(con: Any, records: list[tuple]) -> int:
    """Insert/replace delisted_stocks rows (tuples ordered by _DELISTED_STOCKS_COLS)."""
    return _upsert(con, "delisted_stocks", _DELISTED_STOCKS_COLS, records, pk_cols=("code",))


_MINERVINI_RBA_COLS = ["pick_date", "code", "entry", "exit_px", "outcome", "ret_pct", "days"]


def upsert_minervini_rba(con: Any, records: list[tuple]) -> int:
    """Insert/replace minervini_rba rows (tuples ordered by _MINERVINI_RBA_COLS)."""
    return _upsert(con, "minervini_rba", _MINERVINI_RBA_COLS, records, pk_cols=("pick_date", "code"))
