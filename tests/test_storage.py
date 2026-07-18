"""Storage layer (write side): schema, numeric coercion, idempotent upserts. No network.

Read-side tests (market_cap_asof, connect() dispatch) live in
kr-quant/tests/test_storage.py alongside kr_quant/storage.py's read half.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from collectors.storage import (
    DAILY_BAR_COLUMNS,
    SUPPLY_DEMAND_COLUMNS,
    _upsert,
    connect,
    to_float,
    to_int,
    upsert_daily_bars,
    upsert_minervini_scan,
    upsert_stocks,
    upsert_supply_demand,
)


def test_to_int_handles_kiwoom_strings():
    assert to_int("+322500") == 322500
    assert to_int("-1979879") == -1979879
    assert to_int("") == 0
    assert to_int(None) == 0
    assert to_int("abc") == 0


def test_to_float_handles_signs():
    assert to_float("+7.86") == 7.86
    assert to_float("") == 0.0


def test_upsert_is_idempotent(tmp_path):
    con = connect(tmp_path / "t.db")
    upsert_stocks(con, [{"code": "005930", "name": "삼성전자",
                         "market": "거래소", "sector": "전기/전자", "kind": "A"}])

    record = tuple(
        [{"code": "005930", "date": "20260612", "close": 322500, "flu_rt": 7.86,
          "acc_trde_qty": 31006148, "individual": -1979879, "foreign_": 971587,
          "institution": 1097529, "fnnc_invt": 0, "insrnc": 0, "invtrt": 0,
          "bank": 0, "penfnd_etc": 0, "samo_fund": 0, "natn": 0, "etc_corp": 0}[c]
         for c in SUPPLY_DEMAND_COLUMNS]
    )
    upsert_supply_demand(con, [record])
    upsert_supply_demand(con, [record])  # same PK again

    n = con.execute("SELECT COUNT(*) FROM supply_demand").fetchone()[0]
    assert n == 1  # INSERT OR REPLACE → no duplicate
    row = con.execute("SELECT foreign_ FROM supply_demand").fetchone()
    assert row["foreign_"] == 971587
    con.close()


def test_upsert_uses_on_conflict_for_postgres_connection():
    """Non-sqlite connections get ON CONFLICT DO UPDATE via execute_values, not INSERT OR REPLACE.

    execute_values itself is psycopg2's own (already well-tested) code, so it's
    patched out here — this test only needs to prove _upsert builds the right
    ON CONFLICT SQL and passes the records through.
    """
    fake_con = MagicMock()
    fake_cursor = MagicMock()
    fake_con.cursor.return_value.__enter__.return_value = fake_cursor
    records = [("005930", "20260706", 100)]

    with patch("psycopg2.extras.execute_values") as execute_values:
        n = _upsert(fake_con, "daily_bars", ["code", "date", "close"], records)

    assert n == 1
    execute_values.assert_called_once()
    call_args = execute_values.call_args[0]
    assert call_args[0] is fake_cursor
    sql = call_args[1]
    assert "ON CONFLICT (code,date) DO UPDATE SET close=EXCLUDED.close" in sql
    assert call_args[2] == records
    fake_con.commit.assert_called_once()


def _bar(code, date, close):
    values = {"code": code, "date": date, "open": close, "high": close,
              "low": close, "close": close, "volume": 0, "trade_value": 0}
    return tuple(values[c] for c in DAILY_BAR_COLUMNS)


def test_upsert_minervini_scan_round_trips_and_upserts_on_date(tmp_path):
    con = connect(tmp_path / "t.db")
    upsert_minervini_scan(con, [("2026-07-11", 0.62, "risk_on", 2, "005930,000660")])
    upsert_minervini_scan(con, [("2026-07-11", 0.71, "risk_on", 3, "005930,000660,035420")])

    cur = con.cursor()
    cur.execute("SELECT date, breadth, regime, n_candidates, codes FROM minervini_scan")
    rows = cur.fetchall()
    assert len(rows) == 1  # same date -> replaced, not duplicated
    assert rows[0]["breadth"] == 0.71
    assert rows[0]["n_candidates"] == 3
    con.close()
