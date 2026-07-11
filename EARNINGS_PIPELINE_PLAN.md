# 실적 수집 파이프라인 재구성 — 핸드오프 계획서

> **대상:** 실적(DART) 수집 파이프라인을 담당할 다른 터미널의 에이전트.
> **범위:** kr-quant `collectors/dart_earnings.py`·`storage.py` + kr-quant-airflow DAG.
> (SEPA 실험 모델링 — `strategies/minervini_*`, `features/*`, 하니스 — 은 **본 세션이 계속 담당**하니 건드리지 말 것.)
> **목표:** 실적을 **전종목(~2,600) · TimescaleDB · 매일 증분**으로, 다른 데이터(일봉·수급)와 동일 파이프라인에 편입.

## 왜 (사용자 의도)

현재는 top-500 · CSV · 주간 전체재수집인데, 사용자가 원하는 최종형은:
- **전체 ~2,600 종목** (top-N 유동성 서브셋 아님 → 현재유동성 선택편향 제거)
- **TimescaleDB에 저장** (일봉·수급처럼 upsert, 조인 가능, "다른 데이터랑 같이")
- **초기 백필은 수일에 걸쳐**(DART 한도), 이후 **매일 증분 업데이트**

## 현재 상태 (이미 되어 있는 것 — 재사용하라)

- `collectors/dart_earnings.py`:
  - `parse_financials(payload)` → (netinc, netinc_prior, revenue, revenue_prior, op_income, op_income_prior). **매출·영업이익 파싱 완료** (Code33용). 하위호환 `parse_net_income` 유지.
  - `collect_keys()` + `_fetch_with_rotation(keys, ki, ...)`: **다중키 로테이션 완료** — DART_API_KEY[_2/_3/_4]를 순차, status 020(일한도) 시 다음 키. 실효 40k콜/일. (테스트 있음: `tests/test_dart_earnings.py`)
  - `main()`: 현재 **CSV append** + **코드단위 resume**(done 세트) + top-N 유니버스(daily_bars AVG(trade_value)).
  - `available_date`(fundamentals) 기반 lookahead-safe avail_date. 미래 분기 스킵.
- kr-quant-airflow `dags/weekly_earnings.py`: 주간 CSV **전체재수집(tmp→원자적 교체)**. DART 키는 Fernet Variable→subprocess 주입(Kiwoom 패턴). `.env.example`·`docker-compose.yml`에 DART_API_KEY[_2] 슬롯·시딩 배선 완료. **두 키 Variable 시딩됨**(컨테이너에 DART_API_KEY, DART_API_KEY_2 존재, 둘 다 status 000 검증).
- **진행 중 백필**: `airflow tasks test weekly_earnings collect_earnings`가 top-500·2018~을 `/opt/kr-quant/data/earnings_financials.csv`(호스트 `data/earnings_financials.csv`)로 수집 중. **완주시켜 SEPA 하니스 즉시검증용으로 쓰면 됨** — 곧 DB가 대체.
- 인프라: TimescaleDB 컨테이너 `kr-quant-airflow-timescaledb-1`(localhost:5432, healthy), daily_bars 518만행(2016-09~2026-07). DSN은 airflow `.env`의 TIMESCALE_* 또는 컨테이너 env(TIMESCALE_HOST 등). kr-quant repo는 컨테이너 `/opt/kr-quant`에 마운트(코드 즉시 반영).

## 목표 아키텍처 (구현할 것)

### 1. `storage.py` — earnings 테이블 + upsert
```sql
CREATE TABLE IF NOT EXISTS earnings (
    code TEXT, period TEXT,          -- 자연키 (예: '000660', '2020Q1')
    avail_date DATE,                 -- 공시가능일 (lookahead 게이트)
    netinc DOUBLE PRECISION, netinc_prior DOUBLE PRECISION,
    revenue DOUBLE PRECISION, revenue_prior DOUBLE PRECISION,
    op_income DOUBLE PRECISION, op_income_prior DOUBLE PRECISION,
    PRIMARY KEY (code, period)
);
```
- 기존 `_upsert`(ON CONFLICT DO UPDATE / sqlite INSERT OR REPLACE) 패턴 그대로. `upsert_earnings(con, rows)` 추가.

### 2. `dart_earnings.py` — DB 쓰기 + DB기준 resume + 증분 모드
- `--db-table` 모드: CSV 대신 earnings 테이블에 upsert.
- **resume = DB 조회**: 시작 시 `SELECT code, period FROM earnings`로 이미 있는 (code,period) 집합 → 스킵. (코드단위 아님 — code+period 단위여야 새 분기 잡음.)
- **유니버스 = 전종목**: `--all-codes`(daily_bars의 DISTINCT code, ~2,600) 옵션. 기존 top-N도 유지.
- **증분 모드** `--recent-quarters N`: 전종목 × 최근 N분기(현재+직전)만 fetch·upsert. 일일 DAG용(≈2,600~5,200콜/일).
- 로테이션(`_fetch_with_rotation`) 그대로 사용 → 40k/일.

### 3. airflow DAG 재구성
- **백필 트리거**(수일): 전종목·전이력. DB resume라 매일 실행해도 남은 것만 이어받아 완성(한도 걸리면 020→로테이션→소진 시 종료, 다음날 재개). `max_active_runs=1`.
- **일일 증분 DAG**(또는 `daily_collection`에 태스크 추가): 매일 `--recent-quarters 2 --all-codes --db-table`. 실적 시즌에만 실제 갱신, 평소 저렴. `daily_collection`(평일 16:00)과 같은 흐름/시각에 편입 = "다른 데이터랑 같이".
- 기존 `weekly_earnings`(CSV 전체재수집)는 폐기 또는 백필 트리거로 전환.

## 결정 필요 (사용자에게 확인)

1. **이력 깊이**: **2016~**(Code33 2017부터 유효, 표본↑) vs 2018~. 사용자 선호는 2016~ 쪽(미확정).
2. **top-500 CSV 백필 처분**: 완주(하니스용) 후 DB로 이관 vs 즉시 중단하고 DB로 새로. 권장: 완주.

## 게이트/한도 산수

- 전종목 2,600 × (2016~2026 = 11년) × 4분기 ≈ **114k콜**. 40k/일(2키) → **~3일**. 2018~면 ~94k → ~2.4일.
- 일일 증분: 2,600 × 2분기 ≈ 5,200콜/일 → 여유.
- **주의**: 분기 전체재수집을 매일 하면 94k+콜/일 = 한도 폭파. **반드시 증분(최근 분기만)**.

## 함정 (반드시 인지)

1. **코드단위 resume는 새 분기를 놓친다** — 현재 `main()`의 `done`(코드 세트)은 기존 종목의 신규 분기를 스킵. DB resume는 **(code, period) 단위**여야 함.
2. **avail_date lookahead** — 분기말+45(연간+90)일 이후만 사용. `fetch_financials`는 `avail>today` 스킵 이미 함. DB upsert 시에도 유지.
3. **DART 키·비번 노출 금지** — DART 키는 채팅공유 승인됨이나, TimescaleDB 비번은 argv/로그 노출 금지(DAG는 Variable/env 경유). 커밋 대상엔 어떤 키도 넣지 말 것(.env는 gitignore).
4. **SEPA 모델링 파일 건드리지 말 것** — `strategies/minervini_sepa.py`(하니스), `minervini_exits.py`, `minervini_sizing.py`, `features/{code33 via fundamentals, rs_rating, vcp, universe, base_count}`는 본 세션 담당. 겹치는 건 `dart_earnings.py`·`storage.py`뿐.

## 소비 측 (참고 — 바꾸지 말 것)

- `features.fundamentals.code33_panel(financials, trading_dates)`가 이 데이터를 소비. 입력 컬럼: code, avail_date, netinc, netinc_prior, revenue, revenue_prior, op_income, op_income_prior. DB 테이블 컬럼을 이 스키마로 맞추면 CSV/DB 어느쪽이든 바로 먹음.
