"""KRX delisted-stock finder parsing. Pure JSON in -> tuples out (no network)."""

from __future__ import annotations

from collectors import storage
from collectors.krx_delisted import parse_delisted_finder


def test_parse_delisted_finder_extracts_code_name_market():
    payload = {"block1": [
        {"short_code": "037730", "codeName": "3R", "marketName": "코스닥"},
        {"short_code": "038120", "codeName": "AD모터스", "marketName": "코스닥"},
    ]}
    assert parse_delisted_finder(payload) == [
        ("037730", "3R", "코스닥"),
        ("038120", "AD모터스", "코스닥"),
    ]


def test_parse_delisted_finder_skips_rows_with_no_code():
    payload = {"block1": [{"short_code": "", "codeName": "빈코드"}]}
    assert parse_delisted_finder(payload) == []


def test_parse_delisted_finder_empty_or_missing_block():
    assert parse_delisted_finder({}) == []
    assert parse_delisted_finder({"block1": []}) == []


def test_upsert_delisted_stocks_round_trips_with_and_without_last_trade_date():
    con = storage.connect(":memory:")
    storage.upsert_delisted_stocks(con, [
        ("037730", "3R", "코스닥", "2020-05-15"),
        ("999999", "이력없음", "KOSPI", None),  # daily_bars에 이력이 전혀 없던 종목
    ])

    import sqlite3
    cur = con.cursor()
    cur.execute("SELECT code, name, market, last_trade_date FROM delisted_stocks ORDER BY code")
    rows = cur.fetchall()
    assert rows[0]["code"] == "037730"
    assert rows[0]["last_trade_date"] == "2020-05-15"
    assert rows[1]["code"] == "999999"
    assert rows[1]["last_trade_date"] is None
    con.close()
