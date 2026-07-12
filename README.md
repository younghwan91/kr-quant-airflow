# kr-quant-airflow

[kr-quant](https://github.com/younghwan91/kr-quant) 데이터 수집을 매일 자동 실행하는 Airflow 오케스트레이션 레포.

## 역할 분리

- **kr-quant** (private): 전략/피처 분석 라이브러리(백테스트, SEPA, PEAD, minervini 등). 데이터 수집 로직은 더 이상 여기 없음.
- **kr-quant-airflow** (이 레포, public): 데이터 수집(`collectors/` — DART/키움/KRX/네이버) + 스케줄링 + TimescaleDB 적재를 자체 보유.

수집 로직(`collectors/`)이 이 레포로 이전된 이유: (1) 분석 세션에서 실수로 수집기를 직접 실행해 DB 정합성이 깨지는 사고를 방지 — 분석(private)과 수집(이 레포)이 완전히 분리된 프로세스/레포가 됨, (2) 수집 로직을 오픈소스로 공유하기 위해 private 레포에 갇혀 있을 이유를 없앰. `collectors/`는 `kr_quant` 패키지에 대한 런타임 의존이 전혀 없다(자체 `collectors/storage.py`, `collectors/config.py` 보유).

예외적으로 kr-quant는 여전히 `/opt/kr-quant`로 마운트된다 — `daily_minervini_scan.py`(scanner_final.py 전략 로직)와 `weekly_price_adjust.py`(kr_quant.price_adjust 백조정 로직) 2개 DAG가 in-place로 kr-quant의 analysis 코드를 실행하기 때문(PYTHONPATH/sys.path 기반, 패키지 설치 아님).

```
spare PC (Ubuntu, 이 레포)                 main PC (kr-quant)
┌─────────────────────────────┐
│ Airflow                     │
│  ├─ kq-collect-both 실행     │
│  │   → 로컬 sqlite 적재       │
│  └─ sync_to_timescale.py    │           ┌──────────────┐
│      → TimescaleDB 업서트    │◄──────────│  분석/백테스트  │
│                              │  psql     │  (알파 탐색)   │
│ TimescaleDB (5432, LAN 오픈) │           └──────────────┘
└─────────────────────────────┘
```


## 사전 준비 (스페어 PC, Ubuntu)

```bash
git clone <this-repo> kr-quant-airflow
git clone https://github.com/younghwan91/kr-quant.git ../kr-quant   # sibling 디렉토리
cd kr-quant-airflow
cp .env.example .env   # KIWOOM_APP_KEY/SECRET, POSTGRES_* 채우기
docker compose up -d
```

Airflow 웹서버: http://<spare-pc-ip>:8080

## 구조

- `dags/*.py` — 12개 DAG(일별/주별 시세·수급·실적·컨센서스·미너비니 스캔 등), 대부분 `collectors.X`를 subprocess로 호출
- `collectors/` — 데이터 수집 로직 자체 보유(DART/키움/KRX/네이버), `dags/`·`scripts/`처럼 바인드마운트되는 디렉토리. `collectors/storage.py`가 스키마+upsert 전체를 가짐
- `scripts/sync_to_timescale.py` — sqlite → TimescaleDB 증분 upsert(레거시 경로, 대부분의 DAG는 이제 TimescaleDB에 직접 씀)
- `sql/init_timescale.sql` — hypertable 스키마 (daily_bars, supply_demand, short_selling, credit_balance, earnings, minervini_scan/rba 등)
- `docker/Dockerfile` — collectors/ 자체 의존성만 설치하는 Airflow 이미지 (kr-quant editable install 없음)
- `docker-compose.yml` — Airflow(webserver/scheduler) + Airflow 메타 Postgres + TimescaleDB(앱 데이터, LAN 오픈)

## 메인 PC에서 데이터 읽기

```python
import psycopg2
conn = psycopg2.connect(host="<spare-pc-ip>", port=5432, dbname="kr_quant", user="kr_quant", password="...")
```

## 시크릿 처리

`KIWOOM_APP_KEY`/`KIWOOM_APP_SECRET`(실계좌 키)은 `airflow-webserver`/`airflow-scheduler` 컨테이너에는 평문 env로 전달되지 않는다. `airflow-init` 컨테이너에서만 한 번 읽어 `airflow variables set`으로 Airflow 메타DB에 Fernet 암호화 저장하고, DAG 태스크가 실행 시점에 `Variable.get()`으로 꺼내 collector 서브프로세스 환경에만 주입한다(`dags/*.py`의 `_kiwoom_env()`).

TimescaleDB 접속 정보(`TIMESCALE_*`)는 LAN 내부용이라 평문 컨테이너 env로 충분하다고 판단해 그대로 뒀다 — 필요하면 Airflow Connection으로 옮길 수 있지만, 지금 스코프에선 과함.

`.env`는 절대 커밋하지 않는다(`.gitignore`에 포함).
