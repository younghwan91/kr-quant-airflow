"""폐지 종목 수급 부분 백필 — 파싱·NULL 보존·기존 행 보호 (네트워크 불요).

이 수집기는 **일부 컬럼만** 채운다. 위험은 두 가지다:
  1. 위치 기반 파싱이라 셀 개수가 어긋나면 값이 밀린다(기관↔외국인이 바뀐다).
  2. 못 채운 컬럼을 0 으로 넣으면 "순매매 없음"으로 읽혀 신호가 조용히 왜곡된다.
"""

from __future__ import annotations

from collectors.naver_supply_demand import SD_NAVER_COLS, parse_flow

# 실제 frgn.naver 행 형태(삼성전자 2026-08-12, 실측값).
ROW = """
<tr>
  <td class="tc"><span class="tah p10 gray03">2026.08.12</span></td>
  <td class="num"><span class="tah p11">255,500</span></td>
  <td class="num"><span class="tah p11 red02">16,000</span></td>
  <td class="num"><span class="tah p11">27,102,479</span></td>
  <td class="num"><span class="tah p11 red02">+776,871</span></td>
  <td class="num"><span class="tah p11 red02">+5,802,466</span></td>
  <td class="num"><span class="tah p11">2,730,723,101</span></td>
</tr>
"""


def _named(row):
    return dict(zip(SD_NAVER_COLS, row))


def test_columns_are_read_in_the_right_order():
    """위치 기반 파싱 — 기관과 외국인이 바뀌면 부호까지 뒤집힌 신호가 된다."""
    (row,) = parse_flow(ROW, "005930")
    n = _named(row)
    assert n["date"] == "2026-08-12"
    assert n["close"] == 255_500
    assert n["acc_trde_qty"] == 27_102_479
    assert n["institution"] == 776_871
    assert n["foreign_"] == 5_802_466
    assert n["source"] == "naver"


def test_only_partial_columns_are_written():
    """individual·기관세부·etc_corp 는 아예 안 쓴다 — NULL 로 남아야 '모름'이 된다.

    0 으로 채우면 폐지 종목이 "그날 개인이 안 샀다"로 읽힌다.
    """
    assert "individual" not in SD_NAVER_COLS
    assert "etc_corp" not in SD_NAVER_COLS
    for c in ("fnnc_invt", "insrnc", "invtrt", "bank", "penfnd_etc", "samo_fund", "natn"):
        assert c not in SD_NAVER_COLS
    assert "flu_rt" not in SD_NAVER_COLS


def test_negative_flows_keep_their_sign():
    html = ROW.replace("+776,871", "-776,871").replace("+5,802,466", "-5,802,466")
    n = _named(parse_flow(html, "005930")[0])
    assert n["institution"] == -776_871
    assert n["foreign_"] == -5_802_466


def test_rows_before_since_are_dropped():
    assert parse_flow(ROW, "005930", since="2026-09-01") == []
    assert len(parse_flow(ROW, "005930", since="2016-09-09")) == 1


def test_short_rows_are_skipped_not_misaligned():
    """셀이 모자란 행을 그냥 읽으면 값이 밀린다 — 버리는 게 맞다."""
    broken = """
    <tr><td><span class="tah p10 gray03">2026.08.12</span></td>
    <td><span class="tah p11">255,500</span></td></tr>
    """
    assert parse_flow(broken, "005930") == []


def test_zero_or_missing_close_is_dropped():
    html = ROW.replace(">255,500<", ">0<")
    assert parse_flow(html, "005930") == []


def test_garbage_html_yields_nothing():
    for h in ("", "<html>error</html>", "<tr><td>없음</td></tr>"):
        assert parse_flow(h, "005930") == []


def test_existing_kiwoom_rows_are_never_overwritten(tmp_path):
    """키움 값과 네이버 값은 외국인 정의가 다르다 — 섞이면 조용히 틀린다."""
    from collectors.naver_supply_demand import _write
    from collectors.storage import connect

    con = connect(tmp_path / "t.db")
    con.execute(
        "INSERT INTO supply_demand(code,date,close,individual,foreign_,institution,source)"
        " VALUES('A','2020-01-02',100,-500,300,200,'kiwoom')")
    con.commit()

    _write(con, [("A", "2020-01-02", 999, 1, 2, 3, "naver"),
                 ("A", "2020-01-03", 110, 1000, 10, 20, "naver")])

    rows = {r[0]: r[1:] for r in con.execute(
        "SELECT date, close, source, individual FROM supply_demand").fetchall()}
    assert rows["2020-01-02"] == (100, "kiwoom", -500), "기존 키움 행이 덮였다"
    assert rows["2020-01-03"][1] == "naver"
    assert rows["2020-01-03"][2] is None, "네이버 행의 individual 은 NULL 이어야 한다"
    con.close()
