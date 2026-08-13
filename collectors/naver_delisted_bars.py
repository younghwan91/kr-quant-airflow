"""Backfill daily bars for **delisted** stocks from Naver — closes the survivorship gap.

``daily_bars`` is collected from Kiwoom ``ka10081`` over ``fetch_stock_list()``, which
returns only **currently listed** codes. A stock that went bankrupt or got merged never
enters that loop, so its price history is absent and every backtest silently measures
only the companies that survived — the single biggest hidden return inflator
(``features/universe.py``, GUARDRAILS §3).

**Kiwoom cannot fill this gap.** Measured 2026-08-13: ``ka10081`` on a delisted code
answers ``return_code: 0`` ("정상적으로 처리되었습니다") with one row of empty strings.
It does not error — a naive backfill loop would write nothing and still look green.

Naver's ``siseJson`` endpoint does serve delisted history, back to at least 2003, and
returns the **whole date range in one request** rather than page-by-page. Its OHLCV was
verified identical to our stored Kiwoom bars on overlapping listed codes, including
across Samsung's 2018-05 50:1 split (both vendors serve split-adjusted series).

**One honest gap:** Naver gives no 거래대금. ``trade_value`` is therefore approximated as
``close * volume / 1e6`` (the table's 백만원 unit). Measured against 20,000 real rows the
error is 0.70% median / 3.55% p95 / 7.83% p99 — immaterial for the ADV liquidity floor
this column feeds, but the rows are tagged ``source='naver'`` so the approximation is
never mistaken for a reported figure.

CLI:
    python -m collectors.naver_delisted_bars --db <DSN>            # 전체 폐지종목
    python -m collectors.naver_delisted_bars --db <DSN> --limit 20 # 표본
    python -m collectors.naver_delisted_bars --db <DSN> --dry-run  # 조회만, 미기록
"""

from __future__ import annotations

import argparse
import re
import time
import urllib.error
import urllib.request

from .storage import connect, default_db_path

SISE_URL = "https://api.finance.naver.com/siseJson.naver"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
REFERER = "https://finance.naver.com/"

# 우리 일봉 이력의 시작. 이보다 앞선 폐지 종목은 백필해도 겹치는 구간이 없다.
HISTORY_START = "20160909"

# 응답은 JSON 이 아니라 작은따옴표/개행이 섞인 JS 배열 리터럴이라 정규식으로 읽는다.
# ["20160104", 141980, 148434, 141980, 145853, 6493, 11.21]
_ROW_RE = re.compile(
    r'\["(\d{8})",\s*(-?[\d.]+),\s*(-?[\d.]+),\s*(-?[\d.]+),\s*(-?[\d.]+),\s*(-?\d+)'
)

DAILY_BAR_SOURCE_COLUMNS = [
    "code", "date", "open", "high", "low", "close", "volume", "trade_value", "source",
]


def parse_sise(body: str, code: str) -> list[tuple]:
    """siseJson 본문 → ``DAILY_BAR_SOURCE_COLUMNS`` 순서의 행 리스트 (순수함수).

    거래량 0인 날(거래정지 등)은 버리지 않는다 — 상장은 되어 있었다는 사실 자체가
    유니버스 판정에 필요하고, 여기서 지우면 "사라진 것처럼" 보여 생존편향 스멜테스트를
    오히려 교란한다. 다만 종가가 0/음수인 행은 값이 없는 것이므로 버린다.

    **거래정지일 정규화.** 네이버는 정지일을 ``시가=고가=저가=0, 종가=기준가`` 로
    주는데, 키움은 같은 날을 ``OHLC=종가`` 로 저장한다(실측: 거래량 0인 키움 행
    118,072건 중 118,070건이 OHLC=종가). 0을 그대로 넣으면 고가/저가를 읽는 돌파·ATR·
    손절 로직이 한 테이블 안에서 소스에 따라 다른 값을 보게 되므로 키움 규약에 맞춘다.
    """
    out: list[tuple] = []
    for dt, o, h, low, c, v in _ROW_RE.findall(body):
        close = float(c)
        if close <= 0:
            continue
        o, h, low = float(o), float(h), float(low)
        if o <= 0 and h <= 0 and low <= 0:      # 정지일 — 키움 규약(OHLC=종가)으로
            o = h = low = close
        # 소스 자체가 종가를 고가/저가 밖으로 주는 행이 드물게 있다(정리매매 동전주 등).
        # 봉의 정의상 불가능한 값이라 범위만 종가까지 넓힌다(종가는 실제 체결가라 보존).
        h, low = max(h, close), min(low, close)
        volume = int(v)
        date = f"{dt[:4]}-{dt[4:6]}-{dt[6:]}"
        # 거래대금 근사: 종가×거래량, 백만원 단위(테이블 규약). 실측 오차 중앙값 0.7%.
        trade_value = int(round(close * volume / 1e6))
        out.append((
            code, date, int(o), int(h), int(low), int(close),
            volume, trade_value, "naver",
        ))
    return out


def fetch_sise(code: str, start: str = HISTORY_START, end: str | None = None,
               *, retries: int = 3, timeout: int = 30) -> str:
    """siseJson 원문을 가져온다. 실패 시 빈 문자열(호출부가 '데이터 없음'과 동일 취급)."""
    end = end or time.strftime("%Y%m%d")
    url = (f"{SISE_URL}?symbol={code}&requestType=1"
           f"&startTime={start}&endTime={end}&timeframe=day")
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": REFERER})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310 — 고정 호스트
                return r.read().decode("utf-8", "ignore")
        except (urllib.error.URLError, TimeoutError, OSError):
            if attempt == retries - 1:
                return ""
            time.sleep(1.5 * (attempt + 1))
    return ""


def _delisted_codes(con) -> list[tuple[str, str]]:
    """백필 대상 ``(code, name)``.

    6자리 숫자 코드이면서 유가증권/코스닥인 것만 — 폐지 목록에는 채권·ELW 등
    비표준 코드가 섞여 있고(약 1,800건), 코넥스는 우리 유니버스가 아니다.
    """
    sql = (
        "SELECT code, name FROM delisted_stocks "
        "WHERE code ~ '^[0-9]{6}$' AND market IN ('유가증권', '코스닥') "
        "ORDER BY code"
    )
    with con.cursor() as cur:
        cur.execute(sql)
        return [(r[0], r[1]) for r in cur.fetchall()]


def _insert_bars(con, records: list[tuple]) -> int:
    """폐지 종목 행 삽입. 이미 있는 (code, date)는 건드리지 않는다.

    DO NOTHING 인 이유: 겹치는 구간이 있다면 그건 키움이 상장 중에 수집한 실측치이고,
    네이버 근사 거래대금으로 덮어쓸 이유가 없다.
    """
    if not records:
        return 0
    import psycopg2.extras

    cols = ", ".join(DAILY_BAR_SOURCE_COLUMNS)
    ph = "(" + ", ".join(["%s"] * len(DAILY_BAR_SOURCE_COLUMNS)) + ")"
    sql = (f"INSERT INTO daily_bars ({cols}) VALUES %s "  # noqa: S608 — 컬럼은 모듈 상수
           "ON CONFLICT (code, date) DO NOTHING")
    with con.cursor() as cur:
        # page_size 를 전체 길이로 — 기본값(100)이면 execute_values 가 여러 문장으로
        # 쪼개 실행하고 cur.rowcount 는 **마지막 배치만** 반영해서, 실제로 4,628행이
        # 들어갔는데 328 로 보고하는 일이 생긴다(실측). 종목당 최대 ~2,500행이라
        # 한 문장으로 보내도 문제 없다.
        psycopg2.extras.execute_values(cur, sql, records, template=ph,
                                       page_size=max(len(records), 100))
        written = cur.rowcount
    con.commit()
    return written


def main() -> int:
    ap = argparse.ArgumentParser(description="폐지 종목 일봉 백필 (네이버)")
    ap.add_argument("--db", default=None, help="DSN (미지정 시 기본 SQLite 경로)")
    ap.add_argument("--limit", type=int, default=0, help="상위 N종목만 (0=전체)")
    ap.add_argument("--sleep", type=float, default=0.25, help="요청 간 대기(초)")
    ap.add_argument("--start", default=HISTORY_START, help="시작일 YYYYMMDD")
    ap.add_argument("--dry-run", action="store_true", help="조회만 하고 기록하지 않음")
    args = ap.parse_args()

    from .config import mask_dsn

    con = connect(args.db or default_db_path())
    codes = _delisted_codes(con)
    if args.limit:
        codes = codes[: args.limit]

    print(f"🔌 {mask_dsn(args.db)} | 대상 {len(codes)}종목 | start={args.start}"
          f"{' | DRY-RUN' if args.dry_run else ''}")

    stats = {"codes": 0, "with_data": 0, "in_window": 0, "empty": 0, "rows": 0, "written": 0}
    t0 = time.time()
    for i, (code, name) in enumerate(codes, 1):
        stats["codes"] += 1
        body = fetch_sise(code, start=args.start)
        rows = parse_sise(body, code) if body else []
        if not rows:
            stats["empty"] += 1
        else:
            stats["with_data"] += 1
            stats["in_window"] += 1
            stats["rows"] += len(rows)
            if not args.dry_run:
                stats["written"] += _insert_bars(con, rows)
        if i % 100 == 0 or i == len(codes):
            el = time.time() - t0
            rate = i / el if el else 0
            print(f"  [{i}/{len(codes)}] 데이터있음={stats['with_data']} 없음={stats['empty']} "
                  f"행={stats['rows']:,} 기록={stats['written']:,} "
                  f"| {rate:.1f}종목/s ETA {(len(codes)-i)/rate/60 if rate else 0:.1f}분",
                  flush=True)
        time.sleep(args.sleep)

    con.close()
    print(f"DONE {stats}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
