# Sharadar 동기화 검증 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 구독 중인 Sharadar 데이터셋 14개 전부가 매일 벤더 최신본과 동기화되어 있음을 매 실행마다 확인하고 보고한다.

**Architecture:** `collectors/sharadar_bulk.py` 한 파일에서 끝난다. 주기 3분할(daily/weekly/monthly)을 없애 구독 목록 하나로 합치고, 매니페스트에 `vendor_modified`·`checked_at`·`unchanged_streak`·`sha256` 네 필드를 추가해 "오늘 확인했다"와 "얼마나 오래 안 바뀌었다"와 "파일이 성하다"를 기록한다. 매 실행 끝에 14개 상태를 한 표로 찍는다.

**Tech Stack:** Python 3.11+ 표준 라이브러리만 (`hashlib`, `zipfile`, `json`, `urllib`). 외부 의존 추가 없음. 테스트는 pytest.

**Spec:** `docs/superpowers/specs/2026-08-16-sharadar-sync-verification-design.md`

## Global Constraints

- **낡음은 막지 않는다.** 벤더가 아직 안 올린 테이블은 보고만 하고 진행한다. 빌드 중단은 손상(절단·행수 급감·데이터 후퇴)에만 해당한다.
- **기존 매니페스트를 무효화하지 않는다.** 키는 지금처럼 파일명(`"stocks.csv.zip"`)을 쓴다. `sha256`이 없는 기존 항목은 재다운로드를 유발해서는 안 된다 — 유발하면 첫 실행에 4.6GB를 전량 다시 받는다.
- **`modified` 필드의 의미를 바꾸지 않는다.** `modified`는 *로컬에 있는 파일*의 벤더 타임스탬프다. `needs_download`가 이걸 보고 판단한다. 새로 추가하는 `vendor_modified`는 *마지막으로 목록에서 본* 타임스탬프이며 둘은 다르다 — 섞으면 파일을 안 받고도 최신으로 착각해 영구히 스킵한다.
- **API 키가 로그에 나오면 안 된다.** 재발급 불가 상태이고 레포가 공개다. URL을 찍는 모든 경로는 `collectors.config.mask_secrets`를 거친다.
- **표준 라이브러리만.** 이 모듈은 Airflow 컨테이너에서 `python -m`으로 도는 순수 모듈이다.
- **ruff 룰셋** `["E4", "E7", "E9", "F"]` 통과. CI는 `ruff==0.14.0` 핀.

## 테스트 환경

프로젝트 `.venv`에는 pytest가 없다. 아래로 CI와 같은 집합을 만든다(1회):

```bash
uv venv --python 3.11 /tmp/sharadar-tv
VIRTUAL_ENV=/tmp/sharadar-tv uv pip install -r docker/requirements.txt pytest 'ruff==0.14.0'
```

이후 모든 테스트 실행은 `/tmp/sharadar-tv/bin/python -m pytest ...` 다.
전체 스위트 기준선은 **127 passed** (2026-08-16 확인).

## File Structure

| 파일 | 책임 | 변경 |
|---|---|---|
| `collectors/sharadar_bulk.py` | 벤더 목록 대조 · 다운로드 · 매니페스트 · 상태 보고 | 수정 (전 태스크) |
| `tests/test_sharadar_bulk.py` | 위 동작 고정 | 수정 (전 태스크) |
| `dags/daily_sharadar.py` | 스케줄 경계 | 수정 (Task 5) |
| `README.md` | 미국 파이프라인 서술 | 수정 (Task 5) |
| `docs/superpowers/specs/2026-08-15-…-design.md` | 이전 설계 | 개정 표시 (Task 5) |

새 파일은 만들지 않는다. `sharadar_bulk.py`는 현재 225줄이고 이 작업 후 약 340줄이 된다 — 한 파일이 감당할 범위이고, 쪼개면 "목록 대조 → 다운로드 → 기록 → 보고"라는 하나의 흐름이 흩어진다.

---

### Task 1: 주기 구분을 없애고 구독 목록을 하나로 합친다

`DAILY_TABLES`/`WEEKLY_TABLES`/`MONTHLY_TABLES`/`_CADENCES`를 `SUBSCRIBED_TABLES` 하나로 대체하고, `plan_downloads`를 `plan_sync`로 바꿔 **벤더 목록에서 빠진 테이블을 반환값으로 드러낸다.**

이것으로 설계 문서의 결함 1(`holdings`·`holdings_investor`·`descriptions`가 한 번도 안 받아짐)과 결함 2(목록 누락이 조용히 지나감)가 동시에 사라진다.

**Files:**
- Modify: `collectors/sharadar_bulk.py:33-53` (테이블 상수·`_CADENCES`), `:82-85` (`plan_downloads`)
- Test: `tests/test_sharadar_bulk.py:69-107` (주기별 계획 테스트 4개를 대체)

**Interfaces:**
- Consumes: 없음 (첫 태스크)
- Produces:
  - `SUBSCRIBED_TABLES: tuple[str, ...]` — 구독 중인 14개 테이블명
  - `plan_sync(listing: dict[str, str]) -> tuple[dict[str, str], tuple[str, ...]]` — `(계획, 벤더_목록에_없는_테이블)`

- [ ] **Step 1: 실패하는 테스트를 쓴다**

`tests/test_sharadar_bulk.py`의 import 블록에서 `DAILY_TABLES`, `MONTHLY_TABLES`, `WEEKLY_TABLES`, `plan_downloads`를 지우고 `SUBSCRIBED_TABLES`, `plan_sync`를 넣는다:

```python
from collectors.sharadar_bulk import (
    SUBSCRIBED_TABLES,
    bulk_url,
    needs_download,
    plan_sync,
    read_manifest,
    write_manifest,
)
```

그리고 `# --------- 주기별 계획` 절(69-107행)의 테스트 4개(`test_daily_plan_excludes_quarterly_and_static_tables`, `test_weekly_plan_is_the_quarterly_tables`, `test_plan_skips_tables_the_vendor_did_not_list`, `test_every_paid_dataset_lands_in_exactly_one_cadence`)를 통째로 아래로 교체한다:

```python
# --------------------------------------------------------------- 구독 목록 대조


def test_every_paid_dataset_is_checked_every_run():
    """구독분 14개가 전부 매일 대조 대상이다.

    주기를 나눠두면 weekly/monthly 를 부르는 DAG 이 없을 때 그 테이블은
    영원히 안 받아진다 — 실제로 holdings·holdings_investor·descriptions 가
    그 상태였다. 요구사항은 '항상 동기화' 이므로 주기 개념 자체가 위반이다.
    """
    paid = {
        "stocks", "daily", "fundamentals", "actions", "sp500", "tickers",
        "insiders", "holdings_ticker", "funds", "events", "metrics",
        "holdings", "holdings_investor", "descriptions",
    }

    assert set(SUBSCRIBED_TABLES) == paid
    assert len(SUBSCRIBED_TABLES) == len(set(SUBSCRIBED_TABLES))


def test_plan_covers_everything_the_vendor_offers():
    listing = {t: "2026-08-16T03:00:00Z" for t in SUBSCRIBED_TABLES}

    plan, missing = plan_sync(listing)

    assert set(plan) == set(SUBSCRIBED_TABLES)
    assert missing == ()


def test_plan_reports_tables_the_vendor_did_not_list():
    """조용히 빠지면 '변경 없어 건너뜀' 집계에도 안 잡혀 영원히 안 보인다."""
    listing = {"stocks": "2026-08-16T03:00:00Z"}

    plan, missing = plan_sync(listing)

    assert list(plan) == ["stocks"]
    assert "holdings" in missing
    assert "descriptions" in missing
    assert len(missing) == len(SUBSCRIBED_TABLES) - 1


def test_plan_ignores_tables_we_do_not_subscribe_to():
    """벤더가 새 테이블을 열어도 구독 목록에 없으면 받지 않는다."""
    listing = {"stocks": "2026-08-16T03:00:00Z", "somethingnew": "2026-08-16T03:00:00Z"}

    plan, _ = plan_sync(listing)

    assert "somethingnew" not in plan
```

같은 파일 아래쪽 `test_manifest_json_is_human_readable`의 `@pytest.mark.parametrize("cadence", ["daily", "weekly", "monthly"])`는 주기 개념을 참조하므로 아래로 바꾼다:

```python
@pytest.mark.parametrize("table", ["stocks", "holdings", "descriptions"])
def test_manifest_json_is_human_readable(tmp_path, table):
    """운영 중에 사람이 읽고 판단하는 파일이다 — 한 줄로 뭉치면 안 된다."""
    write_manifest(tmp_path, {f"{table}.csv.zip": {"modified": "x", "size": 1}})

    text = (tmp_path / "manifest.json").read_text()

    assert "\n" in text
    assert json.loads(text)
```

- [ ] **Step 2: 실패를 확인한다**

Run: `/tmp/sharadar-tv/bin/python -m pytest tests/test_sharadar_bulk.py -q`
Expected: FAIL — `ImportError: cannot import name 'SUBSCRIBED_TABLES'`

- [ ] **Step 3: 최소 구현**

`collectors/sharadar_bulk.py`의 33-53행(`DAILY_TABLES` 주석부터 `_CADENCES`까지)을 아래로 교체한다:

```python
# 구독 중인 전량. **주기를 나누지 않는다** — 요구사항이 "항상 동기화" 이고,
# 나눠두면 그 주기를 부르는 DAG 이 없을 때 해당 테이블은 영원히 안 받아진다
# (실제로 holdings·holdings_investor·descriptions 가 그 상태였다).
#
# 매일 14개를 다 확인해도 전송은 안 늘어난다 — `modified` 가 그대로면 0바이트다.
# 늘어나는 비용은 목록 조회 1회뿐이고, 13F 542MB 는 실제로 바뀌는 분기당
# 한 번만 내려온다. 주석의 크기는 2026-08-15 실측이다.
SUBSCRIBED_TABLES = (
    "stocks",           # SEP  — 주가. 953MB
    "daily",            # DAILY— 시총/EV. 733MB
    "fundamentals",     # SF1  — 분기 재무. 626MB
    "funds",            # SFP  — ETF·펀드 가격. 286MB
    "insiders",         # SF2  — Form 4. 234MB
    "holdings",         # SF3  — 13F 원자료. 542MB
    "holdings_investor", # SF3B — 13F 투자자별
    "holdings_ticker",  # SF3A — 13F 티커 집계. 18MB
    "events",           # 11MB
    "actions",          # 9MB
    "metrics",          # 1.4MB
    "sp500",            # 270KB
    "tickers",          # 4.8MB
    "descriptions",     # 필드 사전. 2026-07-31 이후 무변경
)
```

이어서 `plan_downloads`(82-85행)를 교체한다:

```python
def plan_sync(listing: dict[str, str]) -> tuple[dict[str, str], tuple[str, ...]]:
    """`(받을_계획, 벤더가_안_준_테이블)`.

    누락을 **반환값으로** 드러낸다. 예전에는 `if table in listing` 로 조용히
    걸러서, stocks 하나가 목록에서 빠져도 로그 어디에도 안 남았다 — 그리고
    다음 빌드는 어제 zip 으로 정상 완주했다.
    """
    plan = {table: listing[table] for table in SUBSCRIBED_TABLES if table in listing}
    missing = tuple(table for table in SUBSCRIBED_TABLES if table not in listing)
    return plan, missing
```

`sync()`(164-202행)에서 주기를 참조하는 부분을 임시로 맞춘다 — 시그니처에서 `cadence`를 빼고 본문 앞부분을 아래로:

```python
def sync(raw_dir: Path, *, api_key: str) -> dict[str, dict]:
    """구독 14개를 벤더 최신본과 맞춘다. 반환값은 갱신된 매니페스트."""
    raw_dir = Path(raw_dir)
    listing = fetch_listing(api_key=api_key)
    plan, missing = plan_sync(listing)
    manifest = read_manifest(raw_dir)

    if missing:
        print(f"⚠️  벤더 목록에 없는 테이블: {', '.join(missing)}", flush=True)
    if not plan:
        print("⚠️  벤더 목록에서 구독 테이블을 하나도 못 찾았습니다", flush=True)
        return manifest
```

그리고 같은 함수 끝의 출력에서 `{cadence}: `를 지운다:

```python
    print(
        f"✅ 새로 받음 {len(fetched)}개 ({total_bytes/1e6:.0f}MB), "
        f"변경 없어 건너뜀 {len(skipped)}개",
        flush=True,
    )
```

`main()`(205-220행)에서 `--cadence` 인자와 그 전달을 지운다:

```python
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--raw-dir",
        default=os.environ.get("US_RAW_DIR", "/opt/us-data/sharadar/raw"),
    )
    args = parser.parse_args(argv)

    api_key = os.environ.get("SHARADAR_API_KEY")
    if not api_key:
        print("❌ SHARADAR_API_KEY 가 없습니다", file=sys.stderr)
        return 2

    sync(Path(args.raw_dir), api_key=api_key)
    return 0
```

모듈 docstring의 실행 예시(15행)도 고친다:

```
    python -m collectors.sharadar_bulk --raw-dir /opt/us-data/sharadar/raw
```

- [ ] **Step 4: 통과를 확인한다**

Run: `/tmp/sharadar-tv/bin/python -m pytest tests/test_sharadar_bulk.py -q`
Expected: PASS (전체 스위트도 127 passed 유지)

Run: `/tmp/sharadar-tv/bin/ruff check collectors/ tests/`
Expected: `All checks passed!`

- [ ] **Step 5: 커밋**

```bash
git add collectors/sharadar_bulk.py tests/test_sharadar_bulk.py
git commit -m "$(cat <<'EOF'
fix(sharadar): 주기 3분할을 없앤다 — 안 받아지던 3개가 여기서 살아난다

holdings·holdings_investor·descriptions 는 코드에 정의돼 있는데 한 번도
받아진 적이 없다. weekly/monthly 주기를 부르는 DAG 이 없기 때문이다. 구독
목록 하나로 합쳐 매일 14개를 다 대조한다 — modified 비교가 있으니 안 바뀐
건 여전히 0바이트다.

plan_downloads 를 plan_sync 로 바꿔 벤더 목록에서 빠진 테이블을 반환값으로
드러낸다. 예전에는 조용히 걸러져서 stocks 가 목록에서 빠져도 로그에 안 남았다.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: 확인 시각과 정체 횟수를 기록한다

"오늘 벤더와 대조했다"는 증거(`checked_at`)와 "얼마나 오래 안 바뀌었다"(`unchanged_streak`)를 매니페스트에 남긴다. 전송이 없어도 갱신되는 것이 핵심이다 — 이게 요구사항의 "확인되어야 한다"에 대응한다.

**`modified`를 건드리지 않는다.** 새 필드 `vendor_modified`가 "마지막으로 목록에서 본 값"이고, `modified`는 계속 "로컬 파일의 값"이다. 섞으면 파일을 안 받고도 최신으로 착각해 영구히 스킵한다.

**Files:**
- Modify: `collectors/sharadar_bulk.py` (상수 `STALE_AFTER`·`DEFAULT_STALE_AFTER`, 함수 `record_check`·`stale_threshold`·`is_stale` 추가)
- Test: `tests/test_sharadar_bulk.py` (새 절 추가)

**Interfaces:**
- Consumes: `SUBSCRIBED_TABLES` (Task 1)
- Produces:
  - `record_check(entry: dict | None, vendor_modified: str, *, now: str) -> dict` — 확인 시각·정체 횟수를 갱신한 **새** 항목 반환 (입력을 변형하지 않음)
  - `stale_threshold(table: str) -> int`
  - `is_stale(table: str, entry: dict) -> bool`
  - `STALE_AFTER: dict[str, int]`, `DEFAULT_STALE_AFTER: int`

- [ ] **Step 1: 실패하는 테스트를 쓴다**

`tests/test_sharadar_bulk.py` 끝에 절을 추가한다:

```python
# ------------------------------------------------------------ 확인 시각 · 정체


def test_first_check_starts_the_streak_at_zero():
    entry = record_check(None, "2026-08-16T03:56:19Z", now="2026-08-16T17:30:00Z")

    assert entry["vendor_modified"] == "2026-08-16T03:56:19Z"
    assert entry["checked_at"] == "2026-08-16T17:30:00Z"
    assert entry["unchanged_streak"] == 0


def test_streak_grows_while_the_vendor_timestamp_stands_still():
    entry = record_check(None, "2026-08-16T03:56:19Z", now="2026-08-16T17:30:00Z")
    entry = record_check(entry, "2026-08-16T03:56:19Z", now="2026-08-17T17:30:00Z")
    entry = record_check(entry, "2026-08-16T03:56:19Z", now="2026-08-18T17:30:00Z")

    assert entry["unchanged_streak"] == 2
    assert entry["checked_at"] == "2026-08-18T17:30:00Z"


def test_streak_resets_when_the_vendor_publishes():
    entry = {"vendor_modified": "2026-08-16T03:56:19Z", "unchanged_streak": 5}

    entry = record_check(entry, "2026-08-17T03:51:02Z", now="2026-08-17T17:30:00Z")

    assert entry["unchanged_streak"] == 0


def test_checked_at_advances_even_when_nothing_was_downloaded():
    """전송이 없어도 '오늘 확인했다' 는 남아야 한다 — 이게 동기화의 증거다."""
    entry = {"modified": "2026-08-16T03:56:19Z", "size": 953, "sha256": "abc"}

    updated = record_check(entry, "2026-08-16T03:56:19Z", now="2026-08-17T17:30:00Z")

    assert updated["checked_at"] == "2026-08-17T17:30:00Z"
    assert updated["modified"] == "2026-08-16T03:56:19Z"  # 로컬 파일 정보는 그대로
    assert updated["size"] == 953
    assert updated["sha256"] == "abc"


def test_record_check_does_not_mutate_the_input():
    """매니페스트를 제자리에서 고치면 실패 시 되돌릴 게 없다."""
    entry = {"vendor_modified": "2026-08-16T03:56:19Z", "unchanged_streak": 1}

    record_check(entry, "2026-08-16T03:56:19Z", now="2026-08-17T17:30:00Z")

    assert entry["unchanged_streak"] == 1


def test_local_modified_is_never_overwritten_by_a_vendor_sighting():
    """`modified` 는 로컬 파일의 값이다. 목록에서 본 값으로 덮으면 그 파일을
    영원히 안 받는다 — needs_download 가 `modified` 로 판단하기 때문이다."""
    entry = {"modified": "2026-08-16T03:56:19Z", "size": 953}

    updated = record_check(entry, "2026-08-17T03:51:02Z", now="2026-08-17T17:30:00Z")

    assert updated["modified"] == "2026-08-16T03:56:19Z"
    assert updated["vendor_modified"] == "2026-08-17T03:51:02Z"


# ------------------------------------------------------------------- 정체 판정


def test_daily_tables_are_stale_after_two_idle_checks():
    assert not is_stale("stocks", {"unchanged_streak": 1})
    assert is_stale("stocks", {"unchanged_streak": 2})


def test_quarterly_tables_tolerate_long_silence():
    """13F 원자료는 분기 공시다 — 8회 정체는 정상이다."""
    assert not is_stale("holdings", {"unchanged_streak": 7})
    assert is_stale("holdings", {"unchanged_streak": 8})


def test_the_static_field_dictionary_is_allowed_to_never_change():
    """descriptions 는 2026-07-31 이후 안 바뀌었다 — 정상이다."""
    assert not is_stale("descriptions", {"unchanged_streak": 29})


def test_an_unknown_table_falls_back_to_the_daily_threshold():
    assert stale_threshold("something_new") == DEFAULT_STALE_AFTER


def test_a_never_checked_entry_is_not_stale():
    """첫 실행에는 정체가 있을 수 없다."""
    assert not is_stale("stocks", {})


def test_every_subscribed_table_has_a_threshold():
    """임계값을 안 정한 테이블이 조용히 기본값으로 새면 안 된다."""
    for table in SUBSCRIBED_TABLES:
        assert stale_threshold(table) > 0
```

import 블록에 추가한다:

```python
from collectors.sharadar_bulk import (
    DEFAULT_STALE_AFTER,
    SUBSCRIBED_TABLES,
    bulk_url,
    is_stale,
    needs_download,
    plan_sync,
    read_manifest,
    record_check,
    stale_threshold,
    write_manifest,
)
```

- [ ] **Step 2: 실패를 확인한다**

Run: `/tmp/sharadar-tv/bin/python -m pytest tests/test_sharadar_bulk.py -q`
Expected: FAIL — `ImportError: cannot import name 'record_check'`

- [ ] **Step 3: 최소 구현**

`collectors/sharadar_bulk.py`의 `MANIFEST_NAME = "manifest.json"` 아래에 추가한다:

```python
# `modified` 가 몇 번 연속으로 그대로면 눈에 띄게 표시할지. **빌드를 막지
# 않는다** — 낡음은 벌크가 매번 전체 이력을 주므로 다음 실행에 저절로 채워진다.
# 막는 건 손상(절단·행수 급감·데이터 후퇴)뿐이고 그건 게이트의 몫이다.
#
# 미국 거래일 캘린더를 두는 대신 정체 횟수로 근사한다 — 연휴에는 정체가
# 자연히 길어져 관용적이 된다. 대신 "벤더가 상시 하루씩 늦다" 는 만성 지연은
# 이 방식으로 못 잡는다. 알려진 한계다.
DEFAULT_STALE_AFTER = 2
STALE_AFTER: dict[str, int] = {
    "insiders": 4,           # Form 4 — 공시가 없는 날이 있다
    "holdings_ticker": 4,    # SF3A — 13F 집계
    "holdings": 8,           # SF3  — 13F 원자료는 분기 공시다
    "holdings_investor": 8,  # SF3B
    "descriptions": 30,      # 필드 사전. 2026-07-31 이후 무변경
}


def stale_threshold(table: str) -> int:
    return STALE_AFTER.get(table, DEFAULT_STALE_AFTER)


def is_stale(table: str, entry: dict) -> bool:
    return int(entry.get("unchanged_streak", 0)) >= stale_threshold(table)


def record_check(entry: dict | None, vendor_modified: str, *, now: str) -> dict:
    """이번 실행에서 벤더와 대조한 사실을 기록한 **새** 항목을 반환한다.

    `modified`/`size`/`sha256` 은 **로컬 파일**의 정보라 여기서 건드리지 않는다.
    그것들은 실제로 받았을 때만 바뀐다. 목록에서 본 값은 `vendor_modified` 다
    — 둘을 섞으면 파일을 안 받고도 최신으로 착각해 영원히 스킵한다.
    """
    updated = dict(entry or {})
    if updated.get("vendor_modified") == vendor_modified:
        updated["unchanged_streak"] = int(updated.get("unchanged_streak", 0)) + 1
    else:
        updated["unchanged_streak"] = 0
    updated["vendor_modified"] = vendor_modified
    updated["checked_at"] = now
    return updated
```

- [ ] **Step 4: 통과를 확인한다**

Run: `/tmp/sharadar-tv/bin/python -m pytest tests/test_sharadar_bulk.py -q`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add collectors/sharadar_bulk.py tests/test_sharadar_bulk.py
git commit -m "$(cat <<'EOF'
feat(sharadar): 확인 시각과 정체 횟수를 매니페스트에 남긴다

전송이 없어도 checked_at 이 갱신된다 — "오늘 벤더와 대조했다" 의 증거가
이 필드다. 지금까지는 안 바뀐 파일이 로그에 "건너뜀" 으로만 남아, 확인한
것인지 확인을 안 한 것인지 구별할 수 없었다.

vendor_modified 를 modified 와 분리한다. modified 는 로컬 파일의 값이고
needs_download 가 그걸로 판단하므로, 목록에서 본 값으로 덮으면 그 파일을
영원히 안 받는다.

정체 임계값은 표시용이다 — 낡음은 막지 않는다. 벌크가 매번 전체 이력을
주므로 다음 실행에 저절로 채워진다.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: 체크섬과 zip 유효성으로 손상을 잡는다

크기만으로는 손상을 못 잡는다. 다운로드 직후 sha256을 계산해 남기고, zip 중앙 디렉터리를 읽어 절단을 거른다. 다음 실행에서 `modified`가 같아도 sha256이 어긋나면 다시 받는다.

**Files:**
- Modify: `collectors/sharadar_bulk.py` (`file_sha256`·`verify_zip` 추가, `needs_download`·`download` 수정)
- Test: `tests/test_sharadar_bulk.py`

**Interfaces:**
- Consumes: Task 1·2의 함수들
- Produces:
  - `file_sha256(path: Path) -> str` — 소문자 hex
  - `verify_zip(path: Path) -> None` — 손상이면 `CorruptDownload` 예외
  - `CorruptDownload(Exception)`
  - `download(...) -> tuple[int, str]` — **반환값이 `(바이트수, sha256)`으로 바뀐다**

- [ ] **Step 1: 실패하는 테스트를 쓴다**

`tests/test_sharadar_bulk.py` 끝에 추가한다:

```python
# ------------------------------------------------------------ 체크섬 · zip 검사


def _real_zip(path):
    """유효한 zip 을 만든다 — 테스트가 진짜 zip 구조를 통과하는지 봐야 한다."""
    import zipfile

    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("stocks.csv", "ticker,date,close\nAAPL,2026-08-16,100\n")
    return path


def test_sha256_is_stable_and_lowercase_hex(tmp_path):
    path = tmp_path / "a.bin"
    path.write_bytes(b"sharadar")

    digest = file_sha256(path)

    assert digest == file_sha256(path)
    assert len(digest) == 64
    assert digest == digest.lower()


def test_sha256_differs_for_different_content(tmp_path):
    a, b = tmp_path / "a.bin", tmp_path / "b.bin"
    a.write_bytes(b"sharadar")
    b.write_bytes(b"sharadar ")

    assert file_sha256(a) != file_sha256(b)


def test_a_valid_zip_passes_verification(tmp_path):
    verify_zip(_real_zip(tmp_path / "ok.csv.zip"))  # 예외 없으면 통과


def test_a_truncated_zip_is_rejected(tmp_path):
    """벤더 대역폭이 느려 17분짜리 전송이 끊기는 일이 실제로 있었다."""
    path = _real_zip(tmp_path / "cut.csv.zip")
    data = path.read_bytes()
    path.write_bytes(data[: len(data) // 2])

    with pytest.raises(CorruptDownload):
        verify_zip(path)


def test_a_non_zip_payload_is_rejected(tmp_path):
    """벤더가 에러 JSON 을 200 으로 돌려주는 경우 — zip 이 아니다."""
    path = tmp_path / "err.csv.zip"
    path.write_bytes(b'{"error":"rate limited"}')

    with pytest.raises(CorruptDownload):
        verify_zip(path)


def test_redownloads_when_the_checksum_disagrees(tmp_path):
    """크기가 같아도 내용이 상하면 다시 받아야 한다."""
    path = tmp_path / "stocks.csv.zip"
    path.write_bytes(b"PK\x03\x04AAAA")
    manifest = {
        "stocks.csv.zip": {
            "modified": "2026-08-16T03:56:19Z",
            "size": path.stat().st_size,
            "sha256": "0" * 64,
        }
    }

    assert needs_download(path, "2026-08-16T03:56:19Z", manifest=manifest)


def test_matching_checksum_still_skips(tmp_path):
    path = tmp_path / "stocks.csv.zip"
    path.write_bytes(b"PK\x03\x04AAAA")
    manifest = {
        "stocks.csv.zip": {
            "modified": "2026-08-16T03:56:19Z",
            "size": path.stat().st_size,
            "sha256": file_sha256(path),
        }
    }

    assert not needs_download(path, "2026-08-16T03:56:19Z", manifest=manifest)


def test_a_legacy_entry_without_a_checksum_does_not_trigger_a_redownload(tmp_path):
    """기존 매니페스트에는 sha256 이 없다. 그걸로 재다운로드가 걸리면
    첫 실행에 4.6GB 를 전량 다시 받는다 — 그럴 이유가 없다."""
    path = tmp_path / "stocks.csv.zip"
    path.write_bytes(b"PK\x03\x04AAAA")
    manifest = {
        "stocks.csv.zip": {"modified": "2026-08-16T03:56:19Z", "size": path.stat().st_size}
    }

    assert not needs_download(path, "2026-08-16T03:56:19Z", manifest=manifest)


def test_download_returns_the_checksum_of_what_it_wrote(tmp_path):
    import io

    payload = _real_zip(tmp_path / "src.zip").read_bytes()

    def fake_opener(url, timeout=None):
        return io.BytesIO(payload)

    dest = tmp_path / "out" / "stocks.csv.zip"
    size, digest = download("stocks", dest, api_key="K", opener=fake_opener)

    assert size == len(payload)
    assert digest == file_sha256(dest)


def test_download_refuses_to_place_a_corrupt_payload(tmp_path):
    """반쪽 zip 이 목적지에 남으면 다음 실행이 그걸 정상으로 본다."""
    import io

    def fake_opener(url, timeout=None):
        return io.BytesIO(b"not a zip at all")

    dest = tmp_path / "out" / "stocks.csv.zip"

    with pytest.raises(CorruptDownload):
        download("stocks", dest, api_key="K", opener=fake_opener)

    assert not dest.exists()
    assert not list((tmp_path / "out").glob("*.part"))
```

import 블록에 `CorruptDownload`, `download`, `file_sha256`, `verify_zip`를 추가한다.

- [ ] **Step 2: 실패를 확인한다**

Run: `/tmp/sharadar-tv/bin/python -m pytest tests/test_sharadar_bulk.py -q`
Expected: FAIL — `ImportError: cannot import name 'CorruptDownload'`

- [ ] **Step 3: 최소 구현**

`collectors/sharadar_bulk.py` import 블록에 `hashlib`과 `zipfile`을 추가한다(알파벳 순: `argparse`, `hashlib`, `json`, `os`, `sys`, `tempfile`, `time`, `urllib.parse`, `urllib.request`, `zipfile`).

`MANIFEST_NAME` 아래(Task 2 상수들 근처)에 추가한다:

```python
class CorruptDownload(Exception):
    """받은 파일이 온전한 zip 이 아니다 — 목적지에 두면 안 된다."""


def file_sha256(path: Path, *, chunk: int = 1 << 22) -> str:
    """파일 해시. 4.6GB 전량이라도 10초 안쪽이라 매 실행 계산해도 된다."""
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_zip(path: Path) -> None:
    """중앙 디렉터리를 읽어 절단을 잡는다.

    `testzip()` 은 전량 압축 해제라 953MB 에 쓸 수 없다. 중앙 디렉터리는
    zip 끝에 있으므로, 읽히면 전송이 끝까지 온 것이다.
    """
    try:
        with zipfile.ZipFile(path) as zf:
            if not zf.namelist():
                raise CorruptDownload(f"빈 zip 입니다: {path}")
    except zipfile.BadZipFile as exc:
        raise CorruptDownload(f"온전한 zip 이 아닙니다: {path} ({exc})") from exc
```

`needs_download`(118-132행)의 마지막 반환을 아래로 교체한다:

```python
    if record.get("size") != path.stat().st_size:
        return True
    recorded = record.get("sha256")
    # 기존 매니페스트에는 sha256 이 없다. 없다고 다시 받으면 첫 실행에
    # 4.6GB 를 전량 재전송한다 — 미검증으로 두고 다음에 받을 때 채운다.
    if recorded and file_sha256(path) != recorded:
        return True
    return False
```

docstring도 "세 가지" → "네 가지"로 고치고 체크섬 문장을 넣는다.

`download`(135-161행)를 아래로 교체한다:

```python
def download(
    table: str, dest: Path, *, api_key: str, opener=urllib.request.urlopen
) -> tuple[int, str]:
    """벌크 zip 을 받아 원자적으로 배치한다. 반환값은 `(바이트수, sha256)`.

    임시 파일에 받고, **온전한 zip 인지 확인한 뒤에만** `os.replace` 한다 —
    반쪽 zip 이 목적지에 남으면 다음 실행이 그걸 정상으로 보고 그 테이블만
    영구히 낡은 채로 남는다.
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
        verify_zip(Path(tmp))
        digest = file_sha256(Path(tmp))
        # mkstemp 는 0600 으로 만든다. raw 아카이브는 컨테이너(airflow)가 쓰고
        # 호스트의 연구 도구가 읽으므로, 읽기는 열어둔다.
        os.chmod(tmp, 0o644)
        os.replace(tmp, dest)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return written, digest
```

`sync()`의 호출부를 반환값 변경에 맞춘다:

```python
        size, digest = download(table, dest, api_key=api_key)
        elapsed = time.monotonic() - started
        rate = size / elapsed / 1e6 if elapsed else 0
        print(
            f"⬇  {table:18s} {size/1e6:8.1f}MB  {elapsed:6.1f}s  {rate:5.1f}MB/s  ({modified})",
            flush=True,
        )
        manifest[dest.name] = {
            **manifest.get(dest.name, {}),
            "modified": modified,
            "size": size,
            "sha256": digest,
        }
```

- [ ] **Step 4: 통과를 확인한다**

Run: `/tmp/sharadar-tv/bin/python -m pytest tests/test_sharadar_bulk.py -q`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add collectors/sharadar_bulk.py tests/test_sharadar_bulk.py
git commit -m "$(cat <<'EOF'
feat(sharadar): 체크섬과 zip 검사로 손상을 잡는다

크기만으로는 손상을 못 잡는다. 다운로드 직후 sha256 을 남기고, 다음 실행에서
modified 가 같아도 해시가 어긋나면 다시 받는다.

zip 은 중앙 디렉터리만 읽어 검사한다 — testzip() 은 전량 압축 해제라 953MB
에 쓸 수 없고, 중앙 디렉터리가 읽히면 전송이 끝까지 온 것이다. 검사를 통과한
뒤에만 os.replace 하므로 반쪽 zip 이 목적지에 남지 않는다.

기존 매니페스트에는 sha256 이 없다. 없다고 재다운로드를 걸면 첫 실행에
4.6GB 를 전량 다시 받으므로, 미검증으로 두고 다음에 받을 때 채운다.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: 동기화 상태 표를 만든다

14개 전부의 상태를 한 표로 렌더한다. 요구사항의 "확인되어야 한다"에 대응하는 산출물이다.

**Files:**
- Modify: `collectors/sharadar_bulk.py` (`render_report` 추가)
- Test: `tests/test_sharadar_bulk.py`

**Interfaces:**
- Consumes: `SUBSCRIBED_TABLES`, `is_stale` (Task 1·2)
- Produces: `render_report(manifest: dict[str, dict], *, missing: tuple[str, ...], fetched: set[str], now: str) -> str`

- [ ] **Step 1: 실패하는 테스트를 쓴다**

```python
# ------------------------------------------------------------------- 상태 보고


def _manifest(**tables):
    return {f"{t}.csv.zip": entry for t, entry in tables.items()}


def test_report_lists_every_subscribed_table():
    """14개 전부가 보여야 한다 — 빠진 줄이 곧 안 보이는 구멍이다."""
    text = render_report({}, missing=(), fetched=set(), now="2026-08-16T17:34:00Z")

    for table in SUBSCRIBED_TABLES:
        assert table in text


def test_report_marks_what_was_downloaded():
    manifest = _manifest(
        stocks={"vendor_modified": "2026-08-16T03:56:19Z", "size": 953210472,
                "unchanged_streak": 0}
    )

    text = render_report(manifest, missing=(), fetched={"stocks"}, now="2026-08-16T17:34:00Z")

    assert "새로 받음" in text


def test_report_flags_a_stalled_table():
    manifest = _manifest(
        stocks={"vendor_modified": "2026-08-10T03:56:19Z", "size": 1, "unchanged_streak": 4}
    )

    text = render_report(manifest, missing=(), fetched=set(), now="2026-08-16T17:34:00Z")

    assert "⚠️" in text
    assert "4회" in text


def test_report_does_not_flag_a_quietly_static_table():
    """descriptions 는 안 바뀌는 게 정상이다 — 매일 경고가 뜨면 아무도 안 본다."""
    manifest = _manifest(
        descriptions={"vendor_modified": "2026-07-31T02:10:44Z", "size": 1,
                      "unchanged_streak": 12}
    )

    text = render_report(manifest, missing=(), fetched=set(), now="2026-08-16T17:34:00Z")

    line = next(ln for ln in text.splitlines() if ln.startswith("descriptions"))
    assert "⚠️" not in line


def test_report_shows_tables_the_vendor_did_not_list():
    text = render_report({}, missing=("metrics",), fetched=set(), now="2026-08-16T17:34:00Z")

    line = next(ln for ln in text.splitlines() if ln.startswith("metrics"))
    assert "목록에 없음" in line


def test_report_totals_add_up_to_the_subscription():
    """최신 + 주의 + 누락 = 14. 안 맞으면 어딘가 빠진 것이다."""
    manifest = _manifest(
        stocks={"vendor_modified": "2026-08-16T03:56:19Z", "size": 1, "unchanged_streak": 0},
        holdings={"vendor_modified": "2026-07-15T04:02:11Z", "size": 1, "unchanged_streak": 9},
    )

    text = render_report(manifest, missing=("metrics",), fetched={"stocks"},
                         now="2026-08-16T17:34:00Z")

    assert f"{len(SUBSCRIBED_TABLES)}개 중" in text
    assert "주의 1" in text
    assert "누락 1" in text


def test_report_never_leaks_the_api_key():
    """운영 중 사람이 읽고 로그에도 남는 출력이다."""
    manifest = _manifest(
        stocks={"vendor_modified": "2026-08-16T03:56:19Z", "size": 1, "unchanged_streak": 0}
    )

    text = render_report(manifest, missing=(), fetched=set(), now="2026-08-16T17:34:00Z")

    assert "api_key" not in text
```

import 블록에 `render_report`를 추가한다.

- [ ] **Step 2: 실패를 확인한다**

Run: `/tmp/sharadar-tv/bin/python -m pytest tests/test_sharadar_bulk.py -q`
Expected: FAIL — `ImportError: cannot import name 'render_report'`

- [ ] **Step 3: 최소 구현**

`collectors/sharadar_bulk.py`의 `sync()` 바로 위에 추가한다:

```python
def render_report(
    manifest: dict[str, dict],
    *,
    missing: tuple[str, ...],
    fetched: set[str],
    now: str,
) -> str:
    """14개 전부의 동기화 상태를 한 표로.

    요구사항은 "매일 최신인지 **확인**되어야 한다" 이고, 확인의 산출물이
    이 표다. 줄이 빠지면 그게 곧 안 보이는 구멍이므로 구독 목록 전체를
    무조건 훑는다 — 매니페스트에 있는 것만 찍지 않는다.
    """
    lines = [
        f"=== Sharadar 동기화 상태 ({now}) ===",
        f"{'테이블':<20}{'벤더 modified':<24}{'크기':>10}{'정체':>6}  판정",
    ]
    fresh = warn = 0
    for table in SUBSCRIBED_TABLES:
        entry = manifest.get(f"{table}.csv.zip", {})
        if table in missing:
            lines.append(f"{table:<20}{'—':<24}{'—':>10}{'—':>6}  ⚠️ 벤더 목록에 없음")
            continue
        streak = int(entry.get("unchanged_streak", 0))
        size_mb = f"{int(entry.get('size', 0)) / 1e6:.1f}MB"
        if table in fetched:
            verdict = "⬇ 새로 받음"
            fresh += 1
        elif is_stale(table, entry):
            verdict = f"⚠️ {streak}회 연속 정체"
            warn += 1
        else:
            verdict = "✓ 최신"
            fresh += 1
        lines.append(
            f"{table:<20}{str(entry.get('vendor_modified', '—')):<24}"
            f"{size_mb:>10}{streak:>6}  {verdict}"
        )
    lines.append(
        f"{len(SUBSCRIBED_TABLES)}개 중 최신 {fresh} · 주의 {warn} · 누락 {len(missing)}"
    )
    return "\n".join(lines)
```

- [ ] **Step 4: 통과를 확인한다**

Run: `/tmp/sharadar-tv/bin/python -m pytest tests/test_sharadar_bulk.py -q`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add collectors/sharadar_bulk.py tests/test_sharadar_bulk.py
git commit -m "$(cat <<'EOF'
feat(sharadar): 매 실행 끝에 14개 동기화 상태를 표로 찍는다

요구사항은 "매일 최신인지 확인되어야 한다" 이고, 확인의 산출물이 이 표다.
구독 목록 전체를 무조건 훑는다 — 매니페스트에 있는 것만 찍으면 빠진 줄이
곧 안 보이는 구멍이 된다.

정체는 테이블 성격에 맞춘 임계를 넘을 때만 표시한다. descriptions 처럼
안 바뀌는 게 정상인 테이블에 매일 경고가 뜨면 아무도 표를 안 본다.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: sync()에 통합하고 DAG·문서를 맞춘다

앞의 조각들을 실제 실행 경로에 연결한다. 전송이 없는 테이블도 `record_check`를 거치게 하는 것이 이 태스크의 핵심이다 — 안 그러면 `checked_at`이 안 남아 "확인했다"의 증거가 없다.

**Files:**
- Modify: `collectors/sharadar_bulk.py:164-202` (`sync`), 모듈 docstring
- Modify: `dags/daily_sharadar.py:64-72` (`--cadence` 제거)
- Modify: `README.md` (미국 파이프라인 절)
- Modify: `docs/superpowers/specs/2026-08-15-sharadar-bulk-rebuild-design.md` (스케줄 표 개정 표시)
- Test: `tests/test_sharadar_bulk.py`

**Interfaces:**
- Consumes: `plan_sync`, `record_check`, `render_report`, `download` (Task 1-4)
- Produces: `sync(raw_dir: Path, *, api_key: str, now: str | None = None, opener=urllib.request.urlopen) -> dict[str, dict]`

- [ ] **Step 1: 실패하는 테스트를 쓴다**

```python
# --------------------------------------------------------------- sync 통합


def _fake_vendor(tables):
    """벤더 목록 + 벌크 zip 을 흉내내는 opener.

    `opener` 는 반드시 인자로 넘긴다 — 기본값이 정의 시점에 바인딩되므로
    `monkeypatch.setattr("...urllib.request.urlopen", ...)` 로는 안 바뀐다.
    """
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("data.csv", "ticker,date\nAAPL,2026-08-16\n")
    payload = buf.getvalue()

    def opener(url, timeout=None):
        if "/bulk?" in url:
            items = [{"table": t, "modified": m, "history": "full"} for t, m in tables.items()]
            return io.BytesIO(json.dumps({"items": items}).encode())
        return io.BytesIO(payload)

    return opener


def test_sync_records_a_check_for_tables_it_did_not_download(tmp_path):
    """전송이 없어도 checked_at 이 남아야 한다 — 이게 동기화의 증거다."""
    opener = _fake_vendor({t: "2026-08-16T03:00:00Z" for t in SUBSCRIBED_TABLES})

    sync(tmp_path, api_key="K", now="2026-08-16T17:30:00Z", opener=opener)
    manifest = sync(tmp_path, api_key="K", now="2026-08-17T17:30:00Z", opener=opener)

    entry = manifest["stocks.csv.zip"]
    assert entry["checked_at"] == "2026-08-17T17:30:00Z"
    assert entry["unchanged_streak"] == 1


def test_sync_does_not_retransmit_when_nothing_changed(tmp_path):
    opener = _fake_vendor({t: "2026-08-16T03:00:00Z" for t in SUBSCRIBED_TABLES})

    sync(tmp_path, api_key="K", now="2026-08-16T17:30:00Z", opener=opener)
    before = (tmp_path / "stocks.csv.zip").stat().st_mtime_ns

    sync(tmp_path, api_key="K", now="2026-08-17T17:30:00Z", opener=opener)

    assert (tmp_path / "stocks.csv.zip").stat().st_mtime_ns == before


def test_sync_fetches_all_fourteen_on_a_cold_start(tmp_path):
    """주기 분할 시절 3개가 영원히 안 받아졌다 — 그 회귀를 고정한다."""
    opener = _fake_vendor({t: "2026-08-16T03:00:00Z" for t in SUBSCRIBED_TABLES})

    sync(tmp_path, api_key="K", now="2026-08-16T17:30:00Z", opener=opener)

    for table in SUBSCRIBED_TABLES:
        assert (tmp_path / f"{table}.csv.zip").exists(), f"{table} 가 안 받아졌다"


def test_sync_reports_a_table_the_vendor_dropped(tmp_path, capsys):
    opener = _fake_vendor(
        {t: "2026-08-16T03:00:00Z" for t in SUBSCRIBED_TABLES if t != "metrics"}
    )

    sync(tmp_path, api_key="K", now="2026-08-16T17:30:00Z", opener=opener)

    out = capsys.readouterr().out
    assert "metrics" in out
    assert "목록에 없음" in out


def test_sync_prints_the_status_table(tmp_path, capsys):
    opener = _fake_vendor({t: "2026-08-16T03:00:00Z" for t in SUBSCRIBED_TABLES})

    sync(tmp_path, api_key="K", now="2026-08-16T17:30:00Z", opener=opener)

    out = capsys.readouterr().out
    assert "Sharadar 동기화 상태" in out
    assert f"{len(SUBSCRIBED_TABLES)}개 중" in out
```

import 블록에 `sync`를 추가한다.

- [ ] **Step 2: 실패를 확인한다**

Run: `/tmp/sharadar-tv/bin/python -m pytest tests/test_sharadar_bulk.py -q`
Expected: FAIL — `sync() got an unexpected keyword argument 'now'`

- [ ] **Step 3: 최소 구현**

`sync()`를 아래로 교체한다:

```python
def sync(
    raw_dir: Path,
    *,
    api_key: str,
    now: str | None = None,
    opener=urllib.request.urlopen,
) -> dict[str, dict]:
    """구독 14개를 벤더 최신본과 맞춘다. 반환값은 갱신된 매니페스트.

    **전송이 없는 테이블도 반드시 `record_check` 를 거친다** — 안 그러면
    "오늘 확인했다" 가 안 남아, 확인한 것인지 확인 자체를 안 한 것인지
    구별할 수 없다. 요구사항의 절반이 여기 걸려 있다.
    """
    raw_dir = Path(raw_dir)
    now = now or time.strftime("%Y-%m-%dT%H:%M:%S%z")
    listing = fetch_listing(api_key=api_key, opener=opener)
    plan, missing = plan_sync(listing)
    manifest = read_manifest(raw_dir)

    fetched: set[str] = set()
    total_bytes = 0
    for table, modified in plan.items():
        dest = raw_dir / f"{table}.csv.zip"
        if needs_download(dest, modified, manifest=manifest):
            started = time.monotonic()
            size, digest = download(table, dest, api_key=api_key, opener=opener)
            elapsed = time.monotonic() - started
            rate = size / elapsed / 1e6 if elapsed else 0
            print(
                f"⬇  {table:18s} {size/1e6:8.1f}MB  {elapsed:6.1f}s  "
                f"{rate:5.1f}MB/s  ({modified})",
                flush=True,
            )
            manifest[dest.name] = {
                **manifest.get(dest.name, {}),
                "modified": modified,
                "size": size,
                "sha256": digest,
            }
            fetched.add(table)
            total_bytes += size
        manifest[dest.name] = record_check(manifest.get(dest.name), modified, now=now)
        # 매 테이블마다 기록한다 — 17분짜리 작업이 중간에 죽어도 여기까지는 남는다.
        write_manifest(raw_dir, manifest)

    print(render_report(manifest, missing=missing, fetched=fetched, now=now), flush=True)
    print(
        f"✅ 전송 {total_bytes/1e6:.0f}MB · 새로 받음 {len(fetched)}개 · "
        f"확인만 {len(plan) - len(fetched)}개",
        flush=True,
    )
    return manifest
```

`main()`에서 `sync(Path(args.raw_dir), api_key=api_key)` 그대로 둔다(Task 1에서 이미 고침).

모듈 docstring의 요약 문단(9-12행)을 아래로 고친다:

```
**벌크에는 필터가 안 먹는다.** `lastupdated.gte` 를 붙여도 전량이 온다(실측).
그래서 이 모듈은 "증분 다운로드" 를 하지 않는다. 대신 벤더 목록의 `modified`
타임스탬프를 로컬 매니페스트와 비교해 **안 바뀐 파일을 아예 안 받는다**.

**확인은 매일 14개 전부, 전송은 바뀐 것만.** 주기를 나누면 그 주기를 부르는
DAG 이 없을 때 해당 테이블이 영원히 안 받아진다 — 실제로 holdings·
holdings_investor·descriptions 가 그 상태였다. 확인 사실은 `checked_at` 으로
남고, 매 실행 끝에 14개 상태가 표로 출력된다.
```

`dags/daily_sharadar.py`의 `download` 태스크(64-72행)에서 `--cadence` 인자를 뺀다:

```python
    @task(retries=2, retry_delay=timedelta(minutes=10))
    def download() -> None:
        """구독 14개를 벤더와 대조. `modified` 가 그대로면 받지 않는다."""
        run_collector(
            [
                sys.executable, "-m", "collectors.sharadar_bulk",
                "--raw-dir", RAW_DIR,
            ],
            env=sharadar_env(),
        )
```

- [ ] **Step 4: 통과를 확인한다**

Run: `/tmp/sharadar-tv/bin/python -m pytest -q`
Expected: PASS — 전체 스위트 (기준선 127 + 이 계획에서 추가한 테스트)

Run: `/tmp/sharadar-tv/bin/ruff check collectors/ dags/ tests/ scripts/`
Expected: `All checks passed!`

- [ ] **Step 5: 문서를 맞춘다**

`README.md`의 미국 파이프라인 절에서 주기 서술을 고친다. `①  RAW` 줄을 아래로:

```
① RAW      /opt/us-data/sharadar/raw/  ← 매일 14개 전부 대조, `modified` 가 그대로면 안 받는다
```

그리고 같은 절의 불릿에 한 줄을 추가한다:

```
- **동기화 확인**: 구독 14개를 매일 전부 벤더 목록과 대조하고, 매 실행 끝에
  상태 표(마지막 확인 시각·벤더 타임스탬프·정체 횟수)를 찍는다. 낡음은 막지
  않는다 — 벌크가 매번 전체 이력을 주므로 다음 실행에 저절로 채워진다. 막는
  것은 손상(절단·행수 급감·최신일 후퇴)뿐이다. 설계는
  [`2026-08-16-sharadar-sync-verification-design.md`](docs/superpowers/specs/2026-08-16-sharadar-sync-verification-design.md).
```

`docs/superpowers/specs/2026-08-15-sharadar-bulk-rebuild-design.md`의 "## 스케줄" 절 표 바로 위에 개정 표시를 넣는다:

```markdown
> **2026-08-16 개정**: 아래 주기 3분할은 폐기됐다. weekly·monthly 를 부르는
> DAG 이 없어 `holdings`·`holdings_investor`·`descriptions` 가 한 번도 받아지지
> 않았다. 지금은 매일 14개 전부를 대조하고 `modified` 가 바뀐 것만 받는다.
> 후속 설계: `2026-08-16-sharadar-sync-verification-design.md`.
```

- [ ] **Step 6: 커밋**

```bash
git add collectors/sharadar_bulk.py dags/daily_sharadar.py tests/test_sharadar_bulk.py README.md docs/
git commit -m "$(cat <<'EOF'
feat(sharadar): 확인은 매일 14개 전부, 전송은 바뀐 것만

sync 가 전송이 없는 테이블도 record_check 를 거치게 한다 — 안 그러면
checked_at 이 안 남아 "확인했다" 와 "확인 자체를 안 했다" 를 구별할 수 없다.
요구사항의 절반이 여기 걸려 있다.

DAG 에서 --cadence 를 뺀다. 주기 개념이 사라졌으므로 인자도 없다.

README 와 이전 설계 문서(2026-08-15)의 주기 표에 개정 표시를 단다.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Self-Review

**1. Spec coverage**

| 설계 항목 | 태스크 |
|---|---|
| ① 주기 구분 폐지 | Task 1 |
| ② 목록 대조 명시화 (`plan_sync`) | Task 1 |
| ③ 정체를 센다 (`checked_at`·`unchanged_streak`·임계표) | Task 2, Task 5 |
| ④ 손상을 잡는다 (sha256·zip 유효성·매니페스트 마이그레이션) | Task 3 |
| ⑤ 동기화 상태 보고 | Task 4, Task 5 |
| 안 하는 것 (낡음 미차단·캘린더 없음·스토어 미변경·정규화 미이관) | 전 태스크에서 코드 변경 없음으로 충족. Task 2가 임계값을 표시 전용으로 못 박고, `sharadar_build.py`는 이 계획에서 건드리지 않는다 |

**2. Placeholder scan** — TBD/TODO 없음. 모든 코드 단계에 실제 코드 블록이 있다.

**3. Type consistency**

- `plan_sync` 반환은 Task 1에서 `(dict, tuple)`로 정의하고 Task 5에서 `plan, missing`으로 언패킹한다 — 일치.
- `download` 반환은 Task 3에서 `(int, str)`로 바뀌고 Task 5의 `size, digest`가 이를 받는다 — 일치. Task 1에서 임시로 남긴 `size = download(...)` 형태는 Task 3에서 교체된다.
- `record_check(entry, vendor_modified, *, now)`의 인자 순서가 Task 2 정의와 Task 5 호출에서 동일.
- `render_report(manifest, *, missing, fetched, now)`가 Task 4 정의와 Task 5 호출에서 동일.
- `is_stale(table, entry)`가 Task 2 정의와 Task 4 사용에서 동일.
