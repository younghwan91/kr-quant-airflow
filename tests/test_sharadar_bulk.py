"""collectors/sharadar_bulk.py — 벤더 벌크 다운로드(① RAW 계층).

여기서 지키는 건 세 가지다:

1. **안 바뀐 건 다시 받지 않는다** — 벤더 대역폭이 약 4.4MB/s 라 전량이 17분이다.
   `modified` 비교가 유일한 낭비 차단 장치다.
2. **받다 죽어도 반쪽 파일을 남기지 않는다** — 반쪽 zip 을 다음 실행이 "있다"고
   판단하면 그 테이블은 영구히 낡은 채로 남는다.
3. **키가 로그에 안 나온다** — 재발급 불가 상태이고 레포가 공개다.
"""

from __future__ import annotations

import json

import pytest

from collectors.sharadar_bulk import (
    DAILY_TABLES,
    MONTHLY_TABLES,
    WEEKLY_TABLES,
    bulk_url,
    needs_download,
    plan_downloads,
    read_manifest,
    write_manifest,
)


# --------------------------------------------------------------- modified 비교


def test_downloads_when_nothing_local_yet(tmp_path):
    assert needs_download(tmp_path / "sep.csv.zip", "2026-08-15T03:56:19Z", manifest={})


def test_skips_when_vendor_timestamp_unchanged(tmp_path):
    path = tmp_path / "sep.csv.zip"
    path.write_bytes(b"PK\x03\x04zip")
    manifest = {"sep.csv.zip": {"modified": "2026-08-15T03:56:19Z", "size": path.stat().st_size}}

    assert not needs_download(path, "2026-08-15T03:56:19Z", manifest=manifest)


def test_downloads_when_vendor_publishes_a_newer_drop(tmp_path):
    path = tmp_path / "sep.csv.zip"
    path.write_bytes(b"PK\x03\x04zip")
    manifest = {"sep.csv.zip": {"modified": "2026-08-15T03:56:19Z", "size": path.stat().st_size}}

    assert needs_download(path, "2026-08-16T03:51:02Z", manifest=manifest)


def test_redownloads_when_file_vanished_even_if_manifest_says_current(tmp_path):
    """매니페스트만 믿으면, 파일이 지워진 걸 모르고 영원히 스킵한다."""
    manifest = {"sep.csv.zip": {"modified": "2026-08-15T03:56:19Z", "size": 5}}

    assert needs_download(tmp_path / "sep.csv.zip", "2026-08-15T03:56:19Z", manifest=manifest)


def test_redownloads_when_size_disagrees_with_manifest(tmp_path):
    """반쪽 파일 탐지 — 중단된 다운로드가 '최신'으로 남는 걸 막는다."""
    path = tmp_path / "sep.csv.zip"
    path.write_bytes(b"PK\x03")  # 3바이트, 매니페스트는 5바이트라고 주장
    manifest = {"sep.csv.zip": {"modified": "2026-08-15T03:56:19Z", "size": 5}}

    assert needs_download(path, "2026-08-15T03:56:19Z", manifest=manifest)


# --------------------------------------------------------------- 주기별 계획


def test_daily_plan_excludes_quarterly_and_static_tables():
    """13F(542MB)를 매일 계획에 넣으면 분기당 1회면 될 걸 매일 확인하게 된다."""
    listing = {t: "2026-08-15T03:00:00Z" for t in DAILY_TABLES + WEEKLY_TABLES + MONTHLY_TABLES}

    plan = plan_downloads(listing, cadence="daily")

    assert set(plan) == set(DAILY_TABLES)
    assert "holdings" not in plan
    assert "descriptions" not in plan


def test_weekly_plan_is_the_quarterly_tables():
    listing = {t: "2026-08-15T03:00:00Z" for t in DAILY_TABLES + WEEKLY_TABLES + MONTHLY_TABLES}

    assert set(plan_downloads(listing, cadence="weekly")) == set(WEEKLY_TABLES)


def test_plan_skips_tables_the_vendor_did_not_list():
    """벤더 목록에 없는 테이블을 계획에 넣으면 404 로 실패한다."""
    listing = {"stocks": "2026-08-15T03:00:00Z"}

    assert list(plan_downloads(listing, cadence="daily")) == ["stocks"]


def test_every_paid_dataset_lands_in_exactly_one_cadence():
    """구독분 중 어느 것도 빠지지 않고, 중복 다운로드도 없다."""
    paid = {
        "stocks", "daily", "fundamentals", "actions", "sp500", "tickers",
        "insiders", "holdings_ticker", "funds", "events", "metrics",
        "holdings", "holdings_investor", "descriptions",
    }
    scheduled = DAILY_TABLES + WEEKLY_TABLES + MONTHLY_TABLES

    assert set(scheduled) == paid
    assert len(scheduled) == len(set(scheduled)), "같은 테이블이 두 주기에 있다"


# --------------------------------------------------------------- URL / 시크릿


def test_url_carries_the_bulk_flag():
    """bulk=true 가 빠지면 조용히 limit 10,000 짜리 JSON 이 온다 — 절단이다."""
    url = bulk_url("sep", api_key="SECRET")

    assert "bulk=true" in url
    assert "format=csv" in url
    assert "/data/sep?" in url


def test_repr_of_url_never_shows_the_key():
    """로그에 URL 을 찍는 순간을 대비한다 — 키는 재발급이 불가능하다."""
    from collectors.config import mask_secrets

    assert "SECRET" not in mask_secrets(bulk_url("sep", api_key="SECRET"))


def test_api_key_never_reaches_the_task_log():
    """requests 의 HTTPError 는 실패한 URL 을 통째로 담는다 — 키가 쿼리에 있다.

    2026-08-15 실행에서 실제로 400 하나에 키가 평문으로 로그에 남았다. 키는
    재발급이 불가능하고 이 레포는 공개다 — 스트리밍 길목에서 반드시 걸러야 한다.
    """
    from collectors.config import mask_secrets

    err = (
        "400 Client Error for url: "
        "https://api.sharadar.com/v1.0/data/sep?api_key=abc123SECRET&format=json&ticker=AAPL"
    )

    masked = mask_secrets(err)

    assert "abc123SECRET" not in masked
    assert "api_key=***" in masked
    assert "ticker=AAPL" in masked  # 진단에 필요한 건 남아야 한다


def test_masking_still_covers_dsn_passwords():
    """기존 DSN 마스킹을 깨지 않았는지 — 콜렉터 전체가 이 경로를 공유한다."""
    from collectors.config import mask_secrets

    fake = "💾 postgresql://airflow:hunter2@timescaledb:5432/quant"  # allowlist-secret
    masked = mask_secrets(fake)

    assert "hunter2" not in masked
    assert "timescaledb:5432/quant" in masked


# --------------------------------------------------------------- 매니페스트


def test_manifest_roundtrip(tmp_path):
    entries = {"sep.csv.zip": {"modified": "2026-08-15T03:56:19Z", "size": 123}}
    write_manifest(tmp_path, entries)

    assert read_manifest(tmp_path) == entries


def test_missing_manifest_reads_as_empty_not_an_error(tmp_path):
    """첫 실행에는 매니페스트가 없다 — 그게 정상이다."""
    assert read_manifest(tmp_path) == {}


def test_corrupt_manifest_reads_as_empty(tmp_path):
    """깨진 매니페스트 때문에 전체가 멈추면 안 된다 — 다시 받으면 그만이다."""
    (tmp_path / "manifest.json").write_text("{ this is not json")

    assert read_manifest(tmp_path) == {}


def test_manifest_is_written_atomically(tmp_path):
    """빌드 중 죽어도 반쪽 매니페스트가 남으면 안 된다."""
    write_manifest(tmp_path, {"a.csv.zip": {"modified": "x", "size": 1}})
    write_manifest(tmp_path, {"b.csv.zip": {"modified": "y", "size": 2}})

    assert list(read_manifest(tmp_path)) == ["b.csv.zip"]
    assert not list(tmp_path.glob("*.tmp")), "임시 파일이 남았다"


@pytest.mark.parametrize("cadence", ["daily", "weekly", "monthly"])
def test_manifest_json_is_human_readable(tmp_path, cadence):
    """운영 중에 사람이 읽고 판단하는 파일이다 — 한 줄로 뭉치면 안 된다."""
    write_manifest(tmp_path, {f"{cadence}.csv.zip": {"modified": "x", "size": 1}})

    text = (tmp_path / "manifest.json").read_text()

    assert "\n" in text
    assert json.loads(text)
