"""collectors/sharadar_build.py — ② BUILD · ③ GATE · ④ PUBLISH.

이 계층의 존재 이유는 "빠른 적재"가 아니라 **나쁜 스토어를 공개하지 않는 것**이다.
벤더 파일이 잘려 오거나 빌드가 중간에 죽었을 때, 그걸 그대로 공개하면 연구가
조용히 틀린 데이터 위에서 돈다. 게이트를 통과 못 하면 기존 스토어가 계속 서비스된다.
"""

from __future__ import annotations

import duckdb
import pytest

from collectors.sharadar_build import (
    TABLE_KINDS,
    GateFailure,
    build_command,
    publish,
    validate,
)


def _store(path, tables: dict[str, int], *, newest: str = "2026-08-14"):
    """{테이블: 행수} 로 최소 스토어를 만든다."""
    conn = duckdb.connect(str(path))
    for name, rows in tables.items():
        conn.execute(f"CREATE TABLE {name} (ticker VARCHAR, date DATE)")
        for i in range(rows):
            conn.execute(f"INSERT INTO {name} VALUES ('T{i}', DATE '{newest}')")
    conn.close()
    return path


# ------------------------------------------------------------------ 빌드 커맨드


def test_build_command_uses_the_vetted_csv_reader():
    """SQL 로 재구현하지 않고 `--provider csv` 를 쓴다.

    `_csv_daily` 의 백만달러 환산, `_csv_tickers` 의 is_delisted 리네임,
    `_csv_fundamentals` 의 PIT 위반 제외는 전부 실제 버그에서 나온 코드다.
    우회하면 그 버그가 되살아난다(설계문서 '절대 어기면 안 되는 제약').
    """
    cmd = build_command("stocks", raw="/raw/stocks.csv.zip", store="/out/new.duckdb")

    assert "--provider" in cmd and cmd[cmd.index("--provider") + 1] == "csv"
    assert cmd[cmd.index("--kind") + 1] == "prices"
    assert cmd[cmd.index("--csv") + 1] == "/raw/stocks.csv.zip"
    assert cmd[cmd.index("--store") + 1] == "/out/new.duckdb"


def test_every_mapped_kind_is_one_the_cli_accepts():
    accepted = {
        "fundamentals", "prices", "institutions", "insiders",
        "tickers", "actions", "sp500", "daily",
    }

    assert set(TABLE_KINDS.values()) <= accepted


def test_sep_maps_to_prices_not_to_its_vendor_name():
    """벌크 파일명은 stocks 인데 CLI 의 kind 는 prices 다 — 헷갈리기 쉬운 지점."""
    assert TABLE_KINDS["stocks"] == "prices"
    assert TABLE_KINDS["holdings_ticker"] == "institutions"


# ------------------------------------------------------------------ 게이트


def test_gate_passes_when_the_new_store_grew(tmp_path):
    current = _store(tmp_path / "cur.duckdb", {"prices": 10, "fundamentals": 5})
    new = _store(tmp_path / "new.duckdb", {"prices": 12, "fundamentals": 6})

    validate(new, current, expected=("prices", "fundamentals"))  # 예외 없으면 통과


def test_gate_passes_on_first_build_with_no_current_store(tmp_path):
    new = _store(tmp_path / "new.duckdb", {"prices": 12})

    validate(new, tmp_path / "does-not-exist.duckdb", expected=("prices",))


def test_gate_rejects_an_empty_table(tmp_path):
    """벤더 파일이 비었거나 파싱이 통째로 실패한 경우."""
    new = _store(tmp_path / "new.duckdb", {"prices": 12, "fundamentals": 0})

    with pytest.raises(GateFailure, match="fundamentals"):
        validate(new, tmp_path / "none.duckdb", expected=("prices", "fundamentals"))


def test_gate_rejects_a_missing_table(tmp_path):
    new = _store(tmp_path / "new.duckdb", {"prices": 12})

    with pytest.raises(GateFailure, match="tickers"):
        validate(new, tmp_path / "none.duckdb", expected=("prices", "tickers"))


def test_gate_rejects_a_large_row_count_regression(tmp_path):
    """벤더가 잘린 파일을 준 경우 — 행수가 뒷걸음질친다.

    이게 게이트의 핵심이다. 조용히 공개되면 연구가 사라진 데이터 위에서 돈다.
    """
    current = _store(tmp_path / "cur.duckdb", {"prices": 100})
    new = _store(tmp_path / "new.duckdb", {"prices": 80})  # -20%

    with pytest.raises(GateFailure, match="prices"):
        validate(new, current, expected=("prices",))


def test_gate_tolerates_a_tiny_regression(tmp_path):
    """벤더가 중복·오류 행을 정정하면 소폭 감소는 정상이다."""
    current = _store(tmp_path / "cur.duckdb", {"prices": 100})
    new = _store(tmp_path / "new.duckdb", {"prices": 99})  # -1%

    validate(new, current, expected=("prices",))


def test_gate_rejects_a_store_whose_prices_went_backwards(tmp_path):
    """낡은 raw 아카이브로 빌드한 경우 — 행수는 멀쩡한데 날짜가 뒤로 간다.

    2026-08-15 실측으로 발견한 구멍이다. 8/12 자 raw 로 빌드하니 prices 최신일이
    08-14 → 08-10 으로 후퇴했는데 행수 게이트(−0.07%)는 그대로 통과했다.
    이걸 공개하면 연구가 나흘치를 잃고도 모른다.
    """
    current = _store(tmp_path / "cur.duckdb", {"prices": 100}, newest="2026-08-14")
    new = _store(tmp_path / "new.duckdb", {"prices": 100}, newest="2026-08-10")

    with pytest.raises(GateFailure, match="후퇴"):
        validate(new, current, expected=("prices",))


def test_gate_allows_an_unchanged_date_for_a_non_trading_day(tmp_path):
    """휴장일 재빌드는 최신일이 그대로다 — 이건 정상이라 막으면 안 된다."""
    current = _store(tmp_path / "cur.duckdb", {"prices": 100}, newest="2026-08-14")
    new = _store(tmp_path / "new.duckdb", {"prices": 101}, newest="2026-08-14")

    validate(new, current, expected=("prices",))


# ------------------------------------------------------------------ 공개


def test_publish_replaces_atomically(tmp_path):
    dest = _store(tmp_path / "us.duckdb", {"prices": 1})
    new = _store(tmp_path / "new.duckdb", {"prices": 99})

    publish(new, dest)

    conn = duckdb.connect(str(dest), read_only=True)
    assert conn.execute("select count(*) from prices").fetchone()[0] == 99
    conn.close()
    assert not new.exists(), "공개 후 임시 산출물이 남았다"


def test_publish_keeps_the_previous_store_for_rollback(tmp_path):
    dest = _store(tmp_path / "us.duckdb", {"prices": 1})
    new = _store(tmp_path / "new.duckdb", {"prices": 99})

    publish(new, dest, keep=2)

    backups = sorted(tmp_path.glob("us.duckdb.prev*"))
    assert backups, "롤백용 직전 세대가 없다"
    conn = duckdb.connect(str(backups[-1]), read_only=True)
    assert conn.execute("select count(*) from prices").fetchone()[0] == 1
    conn.close()


def test_publish_works_when_there_is_no_existing_store(tmp_path):
    new = _store(tmp_path / "new.duckdb", {"prices": 99})
    dest = tmp_path / "us.duckdb"

    publish(new, dest)

    assert dest.exists()
