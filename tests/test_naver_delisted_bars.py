"""``parse_sise`` — siseJson 본문 파싱 (순수함수, 네트워크 불요)."""

from __future__ import annotations

from collectors.naver_delisted_bars import DAILY_BAR_SOURCE_COLUMNS, parse_sise

# 실제 응답 형태: JSON 이 아니라 작은따옴표 헤더 + 개행/탭이 섞인 JS 배열 리터럴.
SAMPLE = """ [['날짜', '시가', '고가', '저가', '종가', '거래량', '외국인소진율'],

\t
\t\t
["20160104", 141980, 148434, 141980, 145853, 6493, 11.21],
\t\t
["20160105", 145853, 156179, 140690, 151662, 13352, 11.2],
\t\t
["20160106", 151661, 156179, 146498, 147144, 0, 11.24]
]
"""


def test_parses_every_row():
    rows = parse_sise(SAMPLE, "060240")
    assert len(rows) == 3
    assert [r[1] for r in rows] == ["2016-01-04", "2016-01-05", "2016-01-06"]


def test_column_order_matches_contract():
    (row,) = parse_sise('["20160104", 100, 110, 90, 105, 1000, 1.0]', "000020")
    assert len(row) == len(DAILY_BAR_SOURCE_COLUMNS)
    named = dict(zip(DAILY_BAR_SOURCE_COLUMNS, row))
    assert named["code"] == "000020"
    assert named["date"] == "2016-01-04"
    assert (named["open"], named["high"], named["low"], named["close"]) == (100, 110, 90, 105)
    assert named["volume"] == 1000
    assert named["source"] == "naver"


def test_trade_value_is_close_times_volume_in_millions():
    """테이블 규약이 백만원 단위 — 원 단위로 넣으면 ADV 필터가 1e6 배 틀린다."""
    (row,) = parse_sise('["20160104", 100, 110, 90, 50000, 2000000, 1.0]', "000020")
    named = dict(zip(DAILY_BAR_SOURCE_COLUMNS, row))
    assert named["trade_value"] == round(50000 * 2000000 / 1e6)  # 100,000 백만원 = 1000억


def test_zero_volume_rows_are_kept():
    """거래정지일도 '상장돼 있었다'는 사실이라 남긴다 — 지우면 생존편향 스멜을 교란한다."""
    rows = parse_sise(SAMPLE, "060240")
    assert dict(zip(DAILY_BAR_SOURCE_COLUMNS, rows[-1]))["volume"] == 0


def test_nonpositive_close_rows_are_dropped():
    body = '["20160104", 0, 0, 0, 0, 0, 0.0],\n["20160105", 100, 110, 90, 105, 10, 1.0]'
    rows = parse_sise(body, "000020")
    assert len(rows) == 1
    assert rows[0][1] == "2016-01-05"


def test_halt_day_ohlc_normalized_to_close():
    """네이버는 정지일을 OHLC=0 으로 주지만 키움은 OHLC=종가로 저장한다.

    0을 그대로 넣으면 고가/저가를 읽는 로직이 같은 테이블 안에서 소스에 따라 다른
    값을 보게 된다 — 터지지 않고 수치만 틀리는 종류.
    """
    (row,) = parse_sise('["20210113", 0, 0, 0, 2410, 0, 6.27]', "036180")
    n = dict(zip(DAILY_BAR_SOURCE_COLUMNS, row))
    assert (n["open"], n["high"], n["low"], n["close"]) == (2410, 2410, 2410, 2410)
    assert n["volume"] == 0


def test_close_outside_range_widens_the_bar():
    """소스가 종가를 고가 밖으로 주는 행이 있다(정리매매 동전주). 봉 정의상 불가능."""
    (row,) = parse_sise('["20210727", 20, 20, 20, 21, 1709794, 0.04]', "152550")
    n = dict(zip(DAILY_BAR_SOURCE_COLUMNS, row))
    assert n["high"] == 21 and n["low"] == 20 and n["close"] == 21
    assert n["low"] <= n["close"] <= n["high"]


def test_every_parsed_bar_is_internally_consistent():
    rows = parse_sise(SAMPLE, "060240")
    for r in rows:
        n = dict(zip(DAILY_BAR_SOURCE_COLUMNS, r))
        assert n["low"] <= n["open"] <= n["high"]
        assert n["low"] <= n["close"] <= n["high"]


def test_empty_or_garbage_body_yields_nothing():
    for body in ("", "  ", "<html>error</html>", "[['날짜','시가']]"):
        assert parse_sise(body, "000020") == []


def test_schema_has_source_and_upsert_preserves_existing(tmp_path):
    """스키마에 source 가 있고, 백필이 기존 행을 덮지 않는지.

    이 둘이 한 테스트인 이유: 컬럼이 빠지면 백필이 sqlite 신규 DB 에서 죽고
    (실제로 마이그레이션만 넣고 CREATE TABLE 을 빠뜨려 그랬다), on_conflict 가
    update 로 새면 키움 실측 거래대금이 네이버 근사치로 덮인다.
    """
    from collectors.naver_delisted_bars import DAILY_BAR_SOURCE_COLUMNS, _insert_bars
    from collectors.storage import connect

    con = connect(tmp_path / "t.db")
    cols = DAILY_BAR_SOURCE_COLUMNS
    assert cols[-1] == "source"

    kiwoom = ("000020", "2020-01-02", 100, 110, 90, 105, 1000, 105, "kiwoom")
    con.executemany(
        f"INSERT INTO daily_bars({','.join(cols)}) VALUES({','.join(['?'] * len(cols))})",
        [kiwoom])
    con.commit()

    naver = ("000020", "2020-01-02", 9, 9, 9, 9, 9, 9, "naver")
    new_day = ("000020", "2020-01-03", 1, 2, 1, 2, 5, 1, "naver")
    _insert_bars(con, [naver, new_day])

    rows = dict(con.execute("SELECT date, source FROM daily_bars").fetchall())
    assert rows["2020-01-02"] == "kiwoom", "기존 키움 행이 덮였다"
    assert rows["2020-01-03"] == "naver", "새 날짜는 들어와야 한다"
    con.close()
