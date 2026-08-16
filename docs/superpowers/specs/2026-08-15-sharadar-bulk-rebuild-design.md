# Sharadar 미국 데이터 — 벌크 스냅샷 재구축 파이프라인

**결정일**: 2026-08-15
**상태**: 승인됨, 구현 착수
**대체 대상**: 같은 날 만든 증분 API 수집(`collectors/sharadar_us.py`) — 아래 근거로 폐기

## 왜 바꾸는가

증분 API 수집(종목 22,000개를 30개씩 ~730회 나눠 호출)을 실제로 돌려본 결과,
세 가지가 **구조적으로** 깨져 있다. 셋 다 "유니버스를 쪼개 순회한다"는 전제에서 나온다.

| 결함 | 실측 |
|---|---|
| 티커 청크 기준이 개수(30)인데 벤더 제한은 **문자열 200자** | 우선주 티커 30개는 240자 → `400 Invalid ticker parameter`. 재현 3/3. **fundamentals 는 매 실행 실패** |
| `ReadTimeout` 이 재시도되지 않음 (tenacity 가 429/5xx 만 잡음) | prices 단계가 70분 돌다 소켓 타임아웃 하나로 전멸 |
| `PITStore` 에 read-only 모드가 없어 읽기 작업도 배타 락 | 사용자의 `opt-factor optimize` 가 스토어를 잠근 동안 수집 전면 실패(실측) |

그리고 별개로, 구독 중인 14개 데이터셋 중 **6개(funds/SFP · holdings/SF3 ·
holdings_investor/SF3B · events · metrics · descriptions)는 어댑터에 테이블
자체가 없어** 돈만 내고 안 쓰고 있다.

## 벌크가 답인 근거 (실측)

`GET /v1.0/data/{table}?api_key=…&format=csv&bulk=true` → zip. **API 키만으로 인증되고,
14개 테이블 전부** 받아진다(`sfp`→funds, `sf3`→holdings 같은 별칭도 통함).

```
SEP 전체 이력 적재:  46,257,136행 (1997~2026, 21,960종목) / 37.4초
같은 데이터 증분 수집: 70분 소요 후 실패
```

압축 해제 21초를 더해도 1분이다. 약 70배이고, 결정론적이다.

**중요 — 벌크에는 필터가 안 먹는다.** `lastupdated.gte` / `from` 을 붙여도 전량이 온다
(실측: `daily&lastupdated.gte=2026-08-13` → 768MB / 40,074,254행). 그러므로 벌크는
**전체 스냅샷 전용**이고, 설계는 "증분"이 아니라 "재구축"이어야 한다.

## 아키텍처

```
① RAW (불변)       벤더 zip → data/sharadar/raw/<vendor_modified>/<table>.csv.zip
                    /v1.0/bulk 목록의 modified 와 비교해 안 바뀐 건 다운로드 스킵
        ↓
② BUILD (결정론)    raw → us_micro.<build_id>.duckdb
                    같은 raw 입력이면 같은 산출물. PIT 정규화는 여기서만.
        ↓
③ GATE (검증)       통과 못 하면 공개하지 않는다. 기존 스토어가 계속 서비스된다.
        ↓
④ PUBLISH (원자적)  os.replace() → us_micro.duckdb
        ↓
⑤ MANIFEST (계보)   vendor modified · 체크섬 · 행수 · 소요시간 · 빌더 git sha
```

**각 단계가 무엇을 고치는가**

- ①②가 티커 200자 제한과 타임아웃을 없앤다 — 요청이 테이블당 1회다.
- ④가 락 충돌을 없앤다. POSIX `rename` 은 원자적이고, 기존 리더는 옛 inode 를
  계속 안전하게 읽는다. 연구를 돌리는 중에도 배포가 된다.
- 전량 재구축이라 벤더의 소급 재조정(2026-08-15 실행에서 171건 관측)이
  정의상 항상 반영된다 — `lastupdated` 창을 얼마로 잡을지 고민할 필요가 없다.

## 절대 어기면 안 되는 제약

**기존 `_csv_*` 정규화를 우회하지 말 것.** `opt_portfolio/factor/data/sharadar.py` 의
`_csv_fundamentals` / `_csv_daily` / `_csv_tickers` / `_aggregate_insiders` 등은
실제 버그에서 나온 지식을 담고 있다:

- `_csv_daily`: mcap/ev 가 **백만 달러 단위** → 미환산 시 배수가 10⁶배 왜곡
- `_csv_tickers`: `isdelisted` → `is_delisted` 리네임 누락 시 폐지 여부가 통째로 NULL
- `_csv_fundamentals`: `datekey < reportperiod` PIT 위반 행 제외 (+1% 초과면 중단)
- `_drop_raw_close`: `close`/`closeadj` 충돌 처리

속도를 위해 DuckDB SQL 로 재구현하고 싶어지는 지점인데, **동등성 테스트 없이는 금지**한다.
성능이 부족하면 그때 SQL 로 옮기되, 같은 입력에 대해 pandas 경로와 산출이 일치하는지
검증하는 테스트를 함께 넣는다.

## 스케줄

> **2026-08-16 개정**: 아래 주기 3분할은 폐기됐다. weekly·monthly 를 부르는
> DAG 이 없어 `holdings`·`holdings_investor`·`descriptions` 가 한 번도 받아지지
> 않았다. 지금은 매일 14개 전부를 대조하고 `modified` 가 바뀐 것만 받는다.
> 후속 설계: `2026-08-16-sharadar-sync-verification-design.md`.

| 데이터셋 | 주기 | 근거 |
|---|---|---|
| stocks(SEP) · daily · fundamentals(SF1) · actions · sp500 · tickers · insiders(SF2) · holdings_ticker(SF3A) · funds(SFP) · events · metrics | 매일 | 매일 갱신됨 |
| holdings(SF3) · holdings_investor(SF3B) | 주간 | 13F 는 분기 공시. `modified` 비교로 실제 전송은 분기당 1회 |
| descriptions | 월간 | 정적 (2026-07-31 이후 미변경) |

**운영 이슈 — 벤더 드롭 시각.** 벌크 파일은 03:56 UTC(= 12:56 KST)에 갱신된다.
현재 스택 가동 창은 10:00 KST 시작이라, 그대로 두면 항상 전날 드롭을 받는다
(= 하루 추가 지연). 실행을 13:15 KST 이후로 옮기려면 cron 가동 창을 늘려야 한다.
**미결 — 사용자 결정 대기.**

## 비용

| 항목 | 값 |
|---|---|
| 다운로드 | 4.6GB @ 약 4.4MB/s(벤더 스로틀) ≈ 17분. `modified` 스킵으로 평시엔 훨씬 적음 |
| 빌드 | 전 테이블 4~5분 (SEP 37초 실측 외삽) |
| 디스크 | raw 7~14일 + 스토어 3세대 ≈ 50~80GB (여유 372GB) |

## 레포 분담

- **opt_portfolio** — 스키마의 주인이다. 새 테이블 4개(funds·holdings·holdings_investor·events)
  스키마와 upsert, `PITStore(read_only=)`, `opt-factor build`(raw→스토어+검증).
- **quant-airflow** — 스케줄 경계만. 다운로드 → 빌드 호출 → 게이트 → 공개 DAG.
  오늘 만든 DAG 골격·마운트·시크릿 마스킹·CI 시크릿 스캔은 그대로 재사용한다.

## 시크릿

Sharadar 직판 키는 **재발급 불가 상태**(2026-08-15 사용자 확인)인데 두 레포 모두 공개다.
2026-08-15 실행에서 `requests` 의 `HTTPError` 가 실패 URL 을 통째로 담아 키가 로그에
평문으로 찍힌 전례가 있다. 방어는 두 겹이다:

1. 자식 프로세스 출력을 흘리는 모든 지점에서 `collectors.config.mask_secrets` 적용
   (Airflow 경로뿐 아니라 `python -m` 직접 실행 경로도 — 전례가 후자였다)
2. CI 시크릿 스캔 — 추적 파일에 키 형태 문자열이 들어오면 빌드 실패

## 폐기되는 것

`collectors/sharadar_us.py` 의 `resolve_since` / 스테이지 증분 로직. 스킵 가드와
마스킹 래퍼의 발상은 새 파이프라인으로 옮긴다.
