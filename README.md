# kr-quant-airflow

[kr-quant](https://github.com/younghwan91/kr-quant) 데이터 수집을 매일 자동 실행하는 Airflow 오케스트레이션 레포.

## 역할 분리

- **kr-quant**: 키움 REST API 수집 로직 + 백테스트/스코어링 라이브러리. 로컬 SQLite에 씀.
- **kr-quant-airflow** (이 레포): 수집 스케줄링, 재시도/모니터링, SQLite → TimescaleDB 동기화.

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

collector 코드(`kr_quant/storage.py`)는 건드리지 않는다 — 이미 9곳에서 쓰이는 테스트된 핫패스라, 백엔드 분기 로직을 넣기보다 "로컬 sqlite에 쓰고, 별도 태스크가 TimescaleDB로 동기화"하는 단방향 sync로 분리했다.

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

- `dags/daily_collection.py` — 일봉+수급 수집 → 신용잔고 수집(별도 스케줄, T+1~2 지연 고려) → TimescaleDB 동기화
- `scripts/sync_to_timescale.py` — sqlite → TimescaleDB 증분 upsert
- `sql/init_timescale.sql` — hypertable 스키마 (daily_bars, supply_demand, short_selling, credit_balance)
- `docker/Dockerfile` — kr-quant를 editable로 설치한 커스텀 Airflow 이미지
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
