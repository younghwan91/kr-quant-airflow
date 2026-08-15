"""폐지 종목 상장주식수 백필 — 파싱·대상선정·기록 (네트워크 불요).

시가총액의 분모라 틀리면 유니버스 편입이 통째로 어긋난다. 특히 **어느 필드를 쓰느냐**가
조용한 오차의 원천이다 — 유통주식수(자기주식 제외)를 쓰면 시총이 과소 계상된다.
"""

from __future__ import annotations

import collectors.dart_shares as ds
from collectors.storage import connect

# 삼성전자 2025 사업보고서 형태(발췌). 합계 행에는 우선주가 섞여 있다.
PAYLOAD = {
    "status": "000",
    "list": [
        {"se": "보통주", "rcept_no": "20260310002820", "stlm_dt": "2025-12-31",
         "isu_stock_totqy": "20,000,000,000", "istc_totqy": "5,919,637,922",
         "tesstk_co": "91,828,987", "distb_stock_co": "5,827,808,935"},
        {"se": "우선주", "rcept_no": "20260310002820", "stlm_dt": "2025-12-31",
         "isu_stock_totqy": "5,000,000,000", "istc_totqy": "822,886,700",
         "tesstk_co": "20,515,497", "distb_stock_co": "802,371,203"},
        {"se": "합계", "rcept_no": "20260310002820", "stlm_dt": "2025-12-31",
         "istc_totqy": "6,742,524,622", "distb_stock_co": "6,630,180,138"},
    ],
}


def test_uses_issued_shares_not_distributed():
    """유통주식수(자기주식 제외)를 쓰면 시가총액이 과소 계상된다."""
    shares, stlm = ds.parse_shares(PAYLOAD)
    assert shares == 5_919_637_922      # istc_totqy (발행주식총수)
    assert shares != 5_827_808_935      # distb_stock_co (유통주식수)
    assert stlm == "2025-12-31"


def test_ignores_preferred_and_total_rows():
    """합계 행은 우선주를 포함한다 — 보통주 기준 유니버스의 시총을 부풀린다."""
    shares, _ = ds.parse_shares(PAYLOAD)
    assert shares != 6_742_524_622


def test_receipt_date_is_the_disclosure_day_not_the_reference_day():
    """기준일과 공시일이 다르다 — PIT 를 물으면 공시일을 봐야 한다."""
    assert ds.receipt_date(PAYLOAD) == "2026-03-10"
    assert ds.parse_shares(PAYLOAD)[1] == "2025-12-31"


def test_error_and_empty_payloads_yield_nothing():
    for p in ({}, {"status": "013", "message": "조회된 데이타가 없습니다."},
              {"status": "000", "list": []},
              {"status": "000", "list": [{"se": "보통주", "istc_totqy": "-"}]}):
        assert ds.parse_shares(p) == (None, None)


def test_latest_shares_walks_back_until_it_finds_a_report(monkeypatch):
    """폐지 직전 해엔 사업보고서가 없는 경우가 많다 — 분기·직전연도까지 훑어야 한다."""
    calls = []

    def fake(key, cc, year, rc, **kw):
        calls.append((year, rc))
        # 폐지연도는 전부 비어 있고, 직전연도 3분기에만 값이 있다.
        if year == 2022 and rc == "11014":
            return PAYLOAD
        return {"status": "013"}

    monkeypatch.setattr(ds, "fetch", fake)
    got = ds.latest_shares("k", "00126380", 2023, sleep=0)
    assert got is not None
    shares, stlm, rcept = got
    assert shares == 5_919_637_922 and rcept == "2026-03-10"
    assert calls[0] == (2023, "11011"), "최신 연도·사업보고서부터 훑어야 한다"


def test_latest_shares_gives_up_after_the_lookback(monkeypatch):
    monkeypatch.setattr(ds, "fetch", lambda *a, **k: {"status": "013"})
    assert ds.latest_shares("k", "x", 2023, sleep=0) is None


def test_targets_skip_codes_that_already_have_shares(tmp_path):
    """재실행 안전 — 이미 주식수가 있는 코드는 DART 를 다시 부르지 않는다."""
    con = connect(tmp_path / "t.db")
    bars = [("A", "2020-01-02", 1, 1, 1, 1, 1, 1, "naver"),
            ("B", "2020-01-02", 1, 1, 1, 1, 1, 1, "naver"),
            ("C", "2020-01-02", 1, 1, 1, 1, 1, 1, "kiwoom")]
    con.executemany(
        "INSERT INTO daily_bars(code,date,open,high,low,close,volume,trade_value,source)"
        " VALUES(?,?,?,?,?,?,?,?,?)", bars)
    con.execute("INSERT INTO shares_outstanding_history(code,date,shares_outstanding)"
                " VALUES('A','2019-12-31',100)")
    con.commit()

    got = dict(ds._targets(con))
    assert set(got) == {"B"}, "A=이미 있음, C=상장 종목이라 대상 아님"
    con.close()


def test_write_preserves_existing_rows(tmp_path):
    """기존 키움/KRX 행을 DART 값으로 덮어쓰면 안 된다."""
    con = connect(tmp_path / "t.db")
    con.execute("INSERT INTO shares_outstanding_history"
                "(code,date,shares_outstanding,source) VALUES('A','2020-01-02',100,'kiwoom')")
    con.commit()
    ds._write(con, [("A", "2020-01-02", 999, "2020-03-10", "dart"),
                    ("A", "2021-01-02", 200, "2021-03-10", "dart")])
    rows = dict(con.execute(
        "SELECT date, shares_outstanding FROM shares_outstanding_history").fetchall())
    assert rows["2020-01-02"] == 100, "기존 행이 덮였다"
    assert rows["2021-01-02"] == 200
    con.close()
