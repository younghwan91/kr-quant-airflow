"""Sharadar 벌크 스냅샷 다운로더 — 파이프라인의 ① RAW 계층.

설계 근거는 `docs/superpowers/specs/2026-08-15-sharadar-bulk-rebuild-design.md`.
요약하면: 증분 API 수집(종목 22,000개를 30개씩 ~730회 순회)은 벤더의 티커
200자 제한과 소켓 타임아웃 때문에 구조적으로 깨진다. 벌크는 테이블당 요청
1회라 둘 다 해당이 없고, SEP 전체 이력(4,626만 행) 적재가 37초다 — 같은
데이터를 증분으로 받다 70분 쓰고 실패한 것과 대비된다.

**벌크에는 필터가 안 먹는다.** `lastupdated.gte` 를 붙여도 전량이 온다(실측).
그래서 이 모듈은 "증분 다운로드"를 하지 않는다. 대신 벤더 목록의 `modified`
타임스탬프를 로컬 매니페스트와 비교해 **안 바뀐 파일을 아예 안 받는다** — 벤더
대역폭이 약 4.4MB/s 라 전량이 17분이고, 이게 유일한 낭비 차단 장치다.

실행:
    python -m collectors.sharadar_bulk --cadence daily --raw-dir /opt/us-data/sharadar/raw
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
import urllib.parse
import urllib.request

from pathlib import Path

_API_ROOT = "https://api.sharadar.com/v1.0"

# 주기는 데이터가 실제로 변하는 빈도로 정했다. `modified` 비교가 있으니 주기를
# 촘촘히 잡아도 전송은 안 늘지만, 목록 조회와 판정 비용은 든다.
DAILY_TABLES = (
    "stocks",           # SEP  — 주가. 953MB
    "daily",            # DAILY— 시총/EV. 733MB
    "fundamentals",     # SF1  — 분기 재무. 626MB
    "funds",            # SFP  — ETF·펀드 가격. 286MB
    "insiders",         # SF2  — Form 4. 234MB
    "holdings_ticker",  # SF3A — 13F 티커 집계. 18MB
    "events",           # 11MB
    "actions",          # 9MB
    "metrics",          # 1.4MB
    "sp500",            # 270KB
    "tickers",          # 4.8MB
)
# 13F 원자료는 분기 공시다. 542MB 를 매일 확인할 이유가 없다.
WEEKLY_TABLES = ("holdings", "holdings_investor")
# 필드 사전. 2026-07-31 이후 안 바뀌었다.
MONTHLY_TABLES = ("descriptions",)

_CADENCES = {"daily": DAILY_TABLES, "weekly": WEEKLY_TABLES, "monthly": MONTHLY_TABLES}

MANIFEST_NAME = "manifest.json"


def bulk_url(table: str, *, api_key: str) -> str:
    """벌크 zip 다운로드 URL.

    `bulk=true` 가 빠지면 벤더는 조용히 `limit` 기본값(10,000행)짜리 JSON 을
    돌려준다 — 실패가 아니라 **절단**이라 알아채기 어렵다.
    """
    query = urllib.parse.urlencode(
        {"api_key": api_key, "format": "csv", "bulk": "true"}
    )
    return f"{_API_ROOT}/data/{table}?{query}"


def fetch_listing(*, api_key: str, opener=urllib.request.urlopen) -> dict[str, str]:
    """`{테이블: modified}` — 전체 이력 파일만. 5Y/10Y 변형은 안 쓴다."""
    url = f"{_API_ROOT}/bulk?{urllib.parse.urlencode({'api_key': api_key})}"
    with opener(url, timeout=60) as resp:
        payload = json.load(resp)
    return {
        item["table"]: item["modified"]
        for item in payload.get("items", [])
        if item.get("history") == "full"
    }


def plan_downloads(listing: dict[str, str], *, cadence: str) -> dict[str, str]:
    """이번 주기에 볼 테이블 중 벤더가 실제로 제공하는 것만."""
    wanted = _CADENCES[cadence]
    return {table: listing[table] for table in wanted if table in listing}


# ----------------------------------------------------------------- 매니페스트


def read_manifest(raw_dir: Path) -> dict[str, dict]:
    """직전 다운로드 기록. 없거나 깨졌으면 빈 dict — 다시 받으면 그만이다."""
    path = Path(raw_dir) / MANIFEST_NAME
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def write_manifest(raw_dir: Path, entries: dict[str, dict]) -> None:
    """원자적으로 쓴다 — 빌드 중 죽어도 반쪽 매니페스트가 남으면 안 된다."""
    raw_dir = Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=raw_dir, prefix=".manifest-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(entries, fh, indent=2, sort_keys=True, ensure_ascii=False)
            fh.write("\n")
        os.chmod(tmp, 0o644)  # mkstemp 는 0600 — 운영 중 사람이 읽는 파일이다
        os.replace(tmp, raw_dir / MANIFEST_NAME)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def needs_download(path: Path, vendor_modified: str, *, manifest: dict[str, dict]) -> bool:
    """이 파일을 (다시) 받아야 하는가.

    세 가지를 모두 본다. 매니페스트만 믿으면 파일이 지워진 걸 모르고 영원히
    스킵하고, 파일 존재만 믿으면 중단된 반쪽 zip 을 '최신'으로 오인한다.
    """
    path = Path(path)
    if not path.exists():
        return True
    record = manifest.get(path.name)
    if not record:
        return True
    if record.get("modified") != vendor_modified:
        return True
    return record.get("size") != path.stat().st_size


def download(table: str, dest: Path, *, api_key: str, opener=urllib.request.urlopen) -> int:
    """벌크 zip 을 받아 원자적으로 배치한다. 반환값은 바이트 수.

    임시 파일에 받고 `os.replace` 로 옮긴다 — 중간에 죽으면 목적지 파일은
    아예 안 생긴다. 반쪽 zip 이 남으면 다음 실행이 그걸 정상으로 보고
    그 테이블만 영구히 낡은 채 남는다.
    """
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=dest.parent, prefix=f".{table}-", suffix=".part")
    written = 0
    try:
        with os.fdopen(fd, "wb") as out, opener(bulk_url(table, api_key=api_key), timeout=1800) as resp:
            while True:
                block = resp.read(1 << 22)
                if not block:
                    break
                out.write(block)
                written += len(block)
        # mkstemp 는 0600 으로 만든다. raw 아카이브는 컨테이너(airflow)가 쓰고
        # 호스트의 연구 도구가 읽으므로, 읽기는 열어둔다.
        os.chmod(tmp, 0o644)
        os.replace(tmp, dest)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return written


def sync(raw_dir: Path, *, cadence: str, api_key: str) -> dict[str, dict]:
    """이번 주기 테이블을 최신 상태로 맞춘다. 반환값은 갱신된 매니페스트."""
    raw_dir = Path(raw_dir)
    listing = fetch_listing(api_key=api_key)
    plan = plan_downloads(listing, cadence=cadence)
    manifest = read_manifest(raw_dir)

    if not plan:
        print(f"⚠️  '{cadence}' 주기에 해당하는 테이블을 벤더 목록에서 못 찾았습니다", flush=True)
        return manifest

    skipped, fetched, total_bytes = [], [], 0
    for table, modified in plan.items():
        dest = raw_dir / f"{table}.csv.zip"
        if not needs_download(dest, modified, manifest=manifest):
            skipped.append(table)
            continue
        started = time.monotonic()
        size = download(table, dest, api_key=api_key)
        elapsed = time.monotonic() - started
        rate = size / elapsed / 1e6 if elapsed else 0
        print(
            f"⬇  {table:18s} {size/1e6:8.1f}MB  {elapsed:6.1f}s  {rate:5.1f}MB/s  ({modified})",
            flush=True,
        )
        manifest[dest.name] = {"modified": modified, "size": size}
        fetched.append(table)
        total_bytes += size
        # 매 파일마다 기록한다 — 17분짜리 작업이 중간에 죽어도 받은 것까지는 남는다.
        write_manifest(raw_dir, manifest)

    print(
        f"✅ {cadence}: 새로 받음 {len(fetched)}개 ({total_bytes/1e6:.0f}MB), "
        f"변경 없어 건너뜀 {len(skipped)}개",
        flush=True,
    )
    if skipped:
        print(f"   스킵: {', '.join(skipped)}", flush=True)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cadence", required=True, choices=sorted(_CADENCES))
    parser.add_argument(
        "--raw-dir",
        default=os.environ.get("US_RAW_DIR", "/opt/us-data/sharadar/raw"),
    )
    args = parser.parse_args(argv)

    api_key = os.environ.get("SHARADAR_API_KEY")
    if not api_key:
        print("❌ SHARADAR_API_KEY 가 없습니다", file=sys.stderr)
        return 2

    sync(Path(args.raw_dir), cadence=args.cadence, api_key=api_key)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
