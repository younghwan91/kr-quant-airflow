# kr-quant-airflow

한국 주식(코스피·코스닥) 시세·수급·실적·컨센서스 데이터를 매일 자동 수집해 TimescaleDB에 적재하는 Airflow 오케스트레이션 레포. 데이터 수집 로직(`collectors/`)을 자체 보유한다.

## 역할 분리

- **kr-quant-airflow** (이 레포, public): 데이터 수집(`collectors/` — DART/키움/KRX/네이버) + 스케줄링(`dags/`) + TimescaleDB 적재를 자체 보유.
- **[kr-quant](https://github.com/younghwan91/kr-quant)** (private): 전략/피처 분석 라이브러리(백테스트, SEPA, PEAD, minervini 등). TimescaleDB를 읽기 전용으로 사용.

수집 로직이 이 레포에 있는 이유: (1) 분석 세션에서 실수로 수집기를 직접 실행해 DB 정합성이 깨지는 사고를 방지 — 분석(kr-quant)과 수집(이 레포)이 완전히 분리된 프로세스/레포, (2) 수집 로직을 오픈소스로 공유. `collectors/`는 `kr_quant` 패키지에 대한 런타임 의존이 전혀 없다(자체 `collectors/storage.py`, `collectors/config.py` 보유).

예외적으로 kr-quant는 여전히 `/opt/kr-quant`로 마운트된다 — `daily_minervini_scan.py`(scanner_final.py 전략 로직)와 `weekly_price_adjust.py`(kr_quant.price_adjust 백조정 로직) 2개 DAG가 in-place로 kr-quant의 analysis 코드를 실행하기 때문(PYTHONPATH/sys.path 기반, 패키지 설치 아님).

```
spare PC (Ubuntu, 이 레포)                       main PC
┌───────────────────────────────┐
│ Airflow (LocalExecutor)        │
│  dags/*.py                     │
│   └─ subprocess -m collectors.X│         ┌──────────────┐
│       → TimescaleDB 직접 upsert │◄────────│  분석/백테스트  │
│                                 │  psql   │  (kr-quant)   │
│ TimescaleDB (5432, LAN 오픈)    │         └──────────────┘
└───────────────────────────────┘
```

## 사전 준비 (스페어 PC, Ubuntu)

```bash
git clone <this-repo> kr-quant-airflow
git clone https://github.com/younghwan91/kr-quant.git ../kr-quant   # sibling — daily_minervini_scan/weekly_price_adjust 2개 DAG만 필요
cd kr-quant-airflow
cp .env.example .env   # KIWOOM_APP_KEY/SECRET, DART_API_KEY(_2/_3), POSTGRES_* 채우기
docker compose up -d
```

Airflow 웹서버: http://<spare-pc-ip>:8080

## DAG 목록

스케줄은 모두 스택 가동 창(cron이 10:00 기동, `wait_and_stop.sh`가 그날 DAG 종료 후 종료) 안에 둔다.

**매일/평일 (증분):**

| DAG | 스케줄(KST) | 수집 대상 |
|---|---|---|
| `daily_collection` | 평일 16:00 | 일봉+수급(키움) + 업종지수 |
| `daily_collection_catchup` | 매일 10:05 | 전날 실패분만 값싸게 재수집 |
| `daily_short_credit` | 화~토 10:00 | 공매도+신용잔고(키움, T+1~2 지연 고려) |
| `daily_earnings` | 평일 16:00 | DART 실적 증분(당기+전분기, `--multi-batch`) |
| `daily_consensus` | 평일 18:00 | 네이버 애널리스트 컨센서스 |
| `daily_krx_shares` | 평일 18:30 | KRX 일별 상장주식수(point-in-time) |
| `daily_minervini_scan` | 평일 18:40 | 미너비니 스캐너 픽 + RBA 실현결과 축적 |

**주간 (백필/스냅샷):**

| DAG | 스케줄(KST) | 수집 대상 |
|---|---|---|
| `earnings_backfill` | 일 10:00 | DART 실적 전체 이력 백필(`--multi-batch`, resume) |
| `weekly_history_backfill` | 일 11:00 | 업종지수·공매도·신용잔고 히스토리 깊이 재수집(신규 상장·구멍 자동 보정) |
| `weekly_listed_shares` | 월 10:10 | 키움 상장주식수 스냅샷 |
| `weekly_price_adjust` | 토 10:05 | daily_bars_adjusted(액면분할 백조정) 재생성 |
| `weekly_delisted_stocks` | 토 10:15 | KRX 상장폐지종목(생존편향 보정) |

데이터는 **매일 증분 + 주간 깊이 재수집**의 2단 구조다. 시세·수급은 `daily_collection`이 증분을, `combined --update`가 전체 이력을 유지한다. 업종지수·공매도·신용잔고는 평일 DAG가 증분을, `weekly_history_backfill`이 깊이를 유지해 신규 상장 종목·새 지수·DB 리셋 후에도 히스토리가 비지 않는다(콜렉터가 `(code, date)` upsert라 idempotent). 단 신용잔고는 키움 API가 100 거래일까지만 제공한다.

모든 외부 API 호출 DAG는 `retries=1, retry_delay=10분`(백필은 `retries=2, 30분`) — 일시적 네트워크 오류로 그날 데이터가 조용히 빠지는 것을 방지.

## 구조

- `dags/*.py` — 위 12개 DAG, `run_collector()`로 `python -m collectors.X`를 실행
- `dags/_common.py` — `timescale_dsn()`/`kiwoom_env()`/`dart_env()`/`run_collector()` 공유 헬퍼(DAG마다 복붙하지 않도록). `run_collector()`는 콜렉터 stdout을 태스크 로그로 스트리밍하고 DSN 비밀번호를 마스킹한다
- `collectors/` — 데이터 수집 로직 자체 보유(DART/키움/KRX/네이버), `dags/`·`scripts/`처럼 바인드마운트되는 디렉토리. `collectors/storage.py`가 스키마+upsert 전체를 가짐
- `scripts/sync_to_timescale.py` — sqlite → TimescaleDB 증분 upsert(레거시 경로, 대부분의 DAG는 이제 TimescaleDB에 직접 씀)
- `scripts/wait_and_stop.sh` — Airflow 메타DB에 "오늘 남은 unpaused DAG / 실행 중 런"을 질의해, 전부 끝나면 컨테이너 조기 종료(22:00 안전장치 포함). paused DAG는 자동 제외돼 스케줄 변경에 따라온다
- `sql/init_timescale.sql` — hypertable 스키마(daily_bars/daily_bars_adjusted는 1년 청크, 그 외는 7일 청크 + 7일 후 압축) 및 minervini_scan/rba, earnings, consensus 등 일반 테이블
- `docker/Dockerfile` — collectors/ 자체 의존성만 설치하는 Airflow 이미지 (kr-quant editable install 없음)
- `docker-compose.yml` — Airflow(webserver/scheduler) + Airflow 메타 Postgres + TimescaleDB(앱 데이터, LAN 오픈)

## 메인 PC에서 데이터 읽기

```python
import psycopg2
conn = psycopg2.connect(host="<spare-pc-ip>", port=5432, dbname="kr_quant", user="kr_quant", password="...")
```

## 시크릿 처리

`KIWOOM_APP_KEY`/`KIWOOM_APP_SECRET`(실계좌 키), `DART_API_KEY`(_2/_3)는 `airflow-webserver`/`airflow-scheduler` 컨테이너에는 평문 env로 전달되지 않는다. `airflow-init` 컨테이너에서만 한 번 읽어 `airflow variables set`으로 Airflow 메타DB에 Fernet 암호화 저장하고, DAG 태스크가 실행 시점에 `Variable.get()`으로 꺼내 collector 서브프로세스 환경에만 주입한다(`dags/_common.py`의 `kiwoom_env()`/`dart_env()`).

TimescaleDB 접속 정보(`TIMESCALE_*`)는 LAN 내부용이라 평문 컨테이너 env로 충분하다고 판단해 그대로 뒀다 — 필요하면 Airflow Connection으로 옮길 수 있지만, 지금 스코프에선 과함.

`.env`는 절대 커밋하지 않는다(`.gitignore`에 포함).
