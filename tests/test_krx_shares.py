"""KRX shares snapshot parsing. Pure CSV in -> (code, shares) out (no network)."""

from __future__ import annotations

from collectors.krx_shares import parse_snapshot

_CSV = (
    '"종목코드","종목명","시장구분","소속부","종가","대비","등락률",'
    '"시가","고가","저가","거래량","거래대금","시가총액","상장주식수"\n'
    '"005930","삼성전자","KOSPI","-","70000","0","0.00",'
    '"70000","70000","70000","1000","70000000","417000000000000","5969782550"\n'
    '"000660","SK하이닉스","KOSPI","-","150000","0","0.00",'
    '"150000","150000","150000","500","75000000","109000000000000","728002365"\n'
)


def test_parse_snapshot_extracts_code_and_shares():
    rows = dict(parse_snapshot(_CSV))
    assert rows["005930"] == 5969782550
    assert rows["000660"] == 728002365


def test_parse_snapshot_skips_missing_shares():
    csv_text = (
        '"종목코드","상장주식수"\n'
        '"005930","5969782550"\n'
        '"111111","-"\n'
        '"222222",""\n'
    )
    assert parse_snapshot(csv_text) == [("005930", 5969782550)]


def test_parse_snapshot_empty_or_missing_columns():
    assert parse_snapshot("") == []
    assert parse_snapshot('"종목코드","종목명"\n"005930","삼성전자"\n') == []


def test_parse_snapshot_handles_comma_thousands():
    csv_text = '"종목코드","상장주식수"\n"005930","5,969,782,550"\n'
    assert parse_snapshot(csv_text) == [("005930", 5969782550)]


def test_login_wall_raises_instead_of_returning_empty(monkeypatch):
    """KRX 가 OTP 자리에 'LOGOUT' 을 주면 터져야 한다 — 조용한 0행이 최악이다.

    실제로 이 응답을 빈 리스트로 삼키는 바람에 daily_krx_shares 가 22회 실행 내내
    rows=0 이면서 전부 성공으로 기록됐다. 초록불이 아무것도 보장하지 않았다.
    """
    import pytest

    from collectors import krx_shares as ks

    monkeypatch.setattr(ks, "_post", lambda url, data, **kw: b"LOGOUT")
    with pytest.raises(RuntimeError, match="LOGOUT"):
        ks.fetch_snapshot("STK", "20260813")


def test_truncated_otp_also_raises(monkeypatch):
    """정상 OTP 는 길다 — 짧은 응답은 발급 실패다."""
    import pytest

    from collectors import krx_shares as ks

    monkeypatch.setattr(ks, "_post", lambda url, data, **kw: b"nope")
    with pytest.raises(RuntimeError):
        ks.fetch_snapshot("STK", "20260813")
