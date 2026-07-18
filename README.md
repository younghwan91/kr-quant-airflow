# kr-quant-airflow

한국 주식(코스피·코스닥)의 **시세·수급·실적·컨센서스**를 매일 자동으로 수집해
TimescaleDB에 적재하는 Airflow 파이프라인이다. 수집 로직(`collectors/`)과
스케줄링(`dags/`)을 모두 이 저장소가 자체 보유한다.

- **오케스트레이션**: Airflow(LocalExecutor) — 12개 DAG, 매일 증분 + 주간 깊이 재수집
- **데이터 소스**: DART(실적) · 키움 REST(시세·수급·공매도·신용·상장주식수) · KRX(상장주식수·상장폐지) · 네이버(컨센서스)
- **저장소**: TimescaleDB(hypertable + 압축) — LAN에 열어 메인 PC가 읽기 전용으로 질의

---

## 목차

- [역할 분리](#역할-분리)
- [아키텍처](#아키텍처)
- [빠른 시작](#빠른-시작)
- [DAG 목록](#dag-목록)
- [데이터 스키마](#데이터-스키마)
- [저장소 구조](#저장소-구조)
- [TimescaleDB 설계 노트](#timescaledb-설계-노트)
- [메인 PC에서 데이터 읽기](#메인-pc에서-데이터-읽기)
- [시크릿 처리](#시크릿-처리)

---

## 역할 분리

*수집 로직이 왜 분석 레포가 아니라 이곳에 있는가*

| | kr-quant-airflow (이 레포, public) | [kr-quant](https://github.com/younghwan91/kr-quant) (private) |
|---|---|---|
| **역할** | 데이터 **수집·적재·스케줄링** | 전략·피처 **분석** (백테스트, SEPA, PEAD, minervini) |
| **DB 접근** | 쓰기 (수집기가 upsert) | 읽기 전용 |
| **핵심 디렉토리** | `collectors/`, `dags/` | `kr_quant/` 라이브러리 |

수집 로직을 분석 레포에서 떼어 이곳에 둔 이유는 두 가지다.

1. **사고 방지** — 분석 세션에서 실수로 수집기를 직접 실행해 DB 정합성이 깨지는
   일을 막는다. 분석(kr-quant)과 수집(이 레포)은 완전히 분리된 프로세스이자 저장소다.
2. **오픈소스 공유** — 수집 로직을 공개한다. `collectors/`는 `kr_quant` 패키지에
   대한 런타임 의존이 전혀 없다(자체 `collectors/storage.py`·`collectors/config.py`를 갖는다).

> **예외** — kr-quant는 여전히 `/opt/kr-quant`에 읽기 전용으로 마운트된다.
> `daily_minervini_scan`(scanner_final.py 전략 로직)과
> `weekly_price_adjust`(kr_quant.price_adjust 백조정 로직) 두 DAG가 kr-quant의
> 분석 코드를 in-place로 실행하기 때문이다(패키지 설치가 아니라 PYTHONPATH/sys.path 기반).

## 아키텍처

```
spare PC (Ubuntu, 이 레포)                        main PC
┌─────────────────────────────────┐
│ Airflow (LocalExecutor)          │
│   dags/*.py                      │
│    └─ subprocess -m collectors.X │          ┌───────────────┐
│        → TimescaleDB 직접 upsert  │◄─────────│  분석/백테스트  │
│                                   │   psql   │   (kr-quant)   │
│ TimescaleDB (5432, LAN 오픈)      │          └───────────────┘
└─────────────────────────────────┘
```

머신 가동은 cron이 관리한다. 매일 10:00에 스택을 올리고, 그날 예정된 DAG가 모두
끝나면 `scripts/wait_and_stop.sh`가 스택을 조기 종료한다(22:00 안전장치 포함). 모든
DAG 스케줄은 이 가동 창 안에 들도록 맞춰져 있다.

## 빠른 시작

```bash
# 스페어 PC (Ubuntu)
git clone <this-repo> kr-quant-airflow
git clone https://github.com/younghwan91/kr-quant.git ../kr-quant   # sibling — 두 DAG만 사용
cd kr-quant-airflow

cp .env.example .env   # KIWOOM_APP_KEY/SECRET, DART_API_KEY(_2/_3), TIMESCALE_*, AIRFLOW_* 채우기
docker compose up -d
```

- **Airflow 웹서버**: `http://<spare-pc-ip>:8080`
- **TimescaleDB**: `<spare-pc-ip>:5432` (LAN 오픈, 메인 PC가 질의)

## DAG 목록

데이터는 **매일 증분 + 주간 깊이 재수집**의 2단 구조로 채운다. 평일 DAG가 최신
데이터를 증분으로 쌓고, 주간 DAG가 히스토리 깊이를 유지해 신규 상장 종목·새 지수·DB
리셋 이후에도 과거가 비지 않도록 한다(모든 수집기가 `(code, date)` upsert라 idempotent).

**매일/평일 — 증분**

| DAG | 스케줄(KST) | 수집 대상 |
|---|---|---|
| `daily_collection` | 평일 16:00 | 일봉 + 수급(키움) + 업종지수 |
| `daily_collection_catchup` | 매일 10:05 | 전날 실패분만 값싸게 재수집 |
| `daily_short_credit` | 화~토 10:00 | 공매도 + 신용잔고(키움, T+1~2 지연 고려) |
| `daily_earnings` | 평일 16:00 | DART 실적 증분(당기 + 전분기, `--multi-batch`) |
| `daily_consensus` | 평일 18:00 | 네이버 애널리스트 컨센서스 |
| `daily_krx_shares` | 평일 18:30 | KRX 일별 상장주식수(point-in-time) |
| `daily_minervini_scan` | 평일 18:40 | 미너비니 스캐너 픽 + RBA 실현결과 축적 |

**주간 — 백필/스냅샷**

| DAG | 스케줄(KST) | 수집 대상 |
|---|---|---|
| `earnings_backfill` | 일 10:00 | DART 실적 전체 이력 백필(`--multi-batch`, resume) |
| `weekly_history_backfill` | 일 11:00 | 업종지수·공매도·신용잔고 히스토리 깊이 재수집 |
| `weekly_listed_shares` | 월 10:10 | 키움 상장주식수 스냅샷 |
| `weekly_price_adjust` | 토 10:05 | `daily_bars_adjusted`(액면분할 백조정) 재생성 |
| `weekly_delisted_stocks` | 토 10:15 | KRX 상장폐지종목(생존편향 보정) |

> **신뢰성** — 모든 DAG 태스크에 재시도를 걸어 두었다. 외부 API·수집 DAG는
> `retries=1, retry_delay=10분`, 전체 이력 백필은 `retries=2, 30분`이다. 일시적
> 네트워크 오류로 그날 데이터가 조용히 빠지는 것을 막기 위함이다.
>
> **신용잔고 한계** — 키움 API가 최근 100 거래일까지만 제공하므로, 그보다 깊은
> 신용잔고 히스토리는 채울 수 없다.

## 데이터 스키마

전체 정의는 [`sql/init_timescale.sql`](sql/init_timescale.sql)에 있다. 시계열 테이블은
TimescaleDB hypertable(PK `(code, date)`)이고, 그 외는 일반 테이블이다.

**시세·수급 (hypertable)**

| 테이블 | 내용 |
|---|---|
| `daily_bars` | 일봉 OHLCV + 거래대금 |
| `daily_bars_adjusted` | 액면분할 백조정 일봉(`weekly_price_adjust`가 매주 재생성) |
| `supply_demand` | 투자자별 순매수(개인·외국인·기관 + 기관 세부 8종) |
| `short_selling` | 공매도 추이(수량·잔고·비율·평균가) |
| `credit_balance` | 신용잔고(신규·상환·잔고·비율) |
| `sector_index` | 업종지수 OHLCV |
| `shares_outstanding_history` | 상장주식수 이력(point-in-time 시총 계산용) |
| `consensus` | 네이버 애널리스트 컨센서스(목표가·투자의견·EPS) |

**펀더멘털·마스터·스캐너 (일반 테이블)**

| 테이블 | 내용 |
|---|---|
| `stocks` | 종목 마스터(코드·이름·시장·섹터) |
| `earnings` | DART 분기 실적(순이익·매출·영업이익, 당기/전년동기), lookahead-safe `avail_date` |
| `minervini_scan` | 미너비니 스캐너의 일별 레짐 + 진입 후보 |
| `minervini_rba` | 스캐너 픽의 실현결과(RBA) 축적 |
| `delisted_stocks` | 상장폐지 종목(생존편향 보정) |

## 저장소 구조

```
dags/                  # 12개 DAG — run_collector()로 `python -m collectors.X` 실행
  _common.py           #   공유 헬퍼: timescale_dsn()/kiwoom_env()/dart_env()/run_collector()
collectors/            # 수집 로직 자체 보유 (kr_quant 런타임 의존 없음)
  storage.py           #   스키마 + upsert 전체 (sqlite/Postgres 듀얼 백엔드)
  config.py            #   자격증명 로딩 + 키움 클라이언트 생성 + DSN 마스킹
  {daily_bars,supply_demand,short_credit,...}.py   # 소스별 수집기
scripts/
  wait_and_stop.sh     # 그날 DAG 전부 끝나면 스택 조기 종료 (22:00 안전장치)
  sync_to_timescale.py # sqlite → TimescaleDB 증분 upsert (레거시 경로)
sql/init_timescale.sql # hypertable 스키마 + 청크/압축 정책
docker/Dockerfile      # collectors/ 의존성만 설치 (kr-quant editable install 없음)
docker-compose.yml     # Airflow(web/scheduler) + Airflow 메타 Postgres + TimescaleDB
```

**`dags/_common.py`** — DAG마다 중복되던 DSN·자격증명 헬퍼를 한곳에 모았다.
`run_collector()`는 수집기 stdout을 태스크 로그로 실시간 스트리밍하고, 로그에 남을 수
있는 DSN 비밀번호를 마스킹한다(`collectors/config.py`의 `mask_dsn`을 재사용하는 단일 소스).

## TimescaleDB 설계 노트

- **청크 크기** — 모든 hypertable이 `chunk_time_interval = 1년`을 쓴다. 기본 7일 청크는
  이 데이터 볼륨(~2,600종목 × 250거래일/년 ≈ 65만 행/년)에 비해 지나치게 잘게 쪼개져
  청크 메타데이터 오버헤드가 커지고, 여러 해에 걸친 스캔이 느려진다.
- **압축** — 시세·수급·컨센서스 hypertable은 7일이 지난 청크를 컬럼형으로 자동 압축한다
  (`compress_segmentby = 'code'`). 최근 데이터는 행 기반으로 남겨 잦은 upsert를 빠르게 처리한다.
- **`daily_bars_adjusted`는 압축 제외** — `weekly_price_adjust`가 매주 테이블 전체를
  upsert로 재작성하므로, 압축을 걸면 오래된 청크를 매주 압축 해제했다가 다시 압축하는
  순환만 반복된다.
- **DB 쓰기** — 수집기는 `psycopg2.extras.execute_values`로 배치 upsert하고, 긴 전수
  수집은 청크 단위(100종목)로 중간 커밋해 크래시 시 손실을 제한한다.

## 메인 PC에서 데이터 읽기

```python
import psycopg2
conn = psycopg2.connect(
    host="<spare-pc-ip>", port=5432,
    dbname="kr_quant", user="kr_quant", password="...",
)
# 예: 최근 조정 일봉
df = pd.read_sql("SELECT * FROM daily_bars_adjusted WHERE code = %s ORDER BY date", conn, params=("005930",))
```

분석·백테스트 코드는 [kr-quant](https://github.com/younghwan91/kr-quant)에 있으며, 이
DB를 읽기 전용으로 사용한다.

## 시크릿 처리

`KIWOOM_APP_KEY`/`KIWOOM_APP_SECRET`(실계좌 키)와 `DART_API_KEY`(_2/_3)는
`airflow-webserver`·`airflow-scheduler` 컨테이너에 평문 env로 전달되지 **않는다**.
`airflow-init` 컨테이너가 이 값을 한 번만 읽어 `airflow variables set`으로 Airflow
메타DB에 **Fernet 암호화**해 저장하고, DAG 태스크는 실행 시점에 `Variable.get()`으로
꺼내 수집기 서브프로세스 환경에만 주입한다(`dags/_common.py`의 `kiwoom_env()`·`dart_env()`).

TimescaleDB 접속 정보(`TIMESCALE_*`)는 LAN 내부용이라 평문 컨테이너 env로 두었다.
필요하면 Airflow Connection으로 옮길 수 있지만, 지금 범위에서는 과하다고 판단했다.

`.env`는 절대 커밋하지 않는다(`.gitignore`에 포함).
