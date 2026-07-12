-- Shared TimescaleDB schema for kr-quant data.
-- Mirrors kr_quant/storage.py's sqlite schema, but `date` is a real DATE
-- column (sqlite stores 'YYYYMMDD' TEXT) so hypertable chunking works.

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS stocks (
    code   TEXT PRIMARY KEY,
    name   TEXT,
    market TEXT,
    sector TEXT,
    kind   TEXT
);

CREATE TABLE IF NOT EXISTS daily_bars (
    code        TEXT NOT NULL,
    date        DATE NOT NULL,
    open        INTEGER,
    high        INTEGER,
    low         INTEGER,
    close       INTEGER,
    volume      BIGINT,
    trade_value BIGINT,
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('daily_bars', 'date', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS supply_demand (
    code         TEXT NOT NULL,
    date         DATE NOT NULL,
    close        INTEGER,
    flu_rt       REAL,
    acc_trde_qty BIGINT,
    individual   INTEGER,
    foreign_     INTEGER,
    institution  INTEGER,
    fnnc_invt    INTEGER,
    insrnc       INTEGER,
    invtrt       INTEGER,
    bank         INTEGER,
    penfnd_etc   INTEGER,
    samo_fund    INTEGER,
    natn         INTEGER,
    etc_corp     INTEGER,
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('supply_demand', 'date', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS short_selling (
    code            TEXT NOT NULL,
    date            DATE NOT NULL,
    close           INTEGER,
    volume          BIGINT,
    short_qty       BIGINT,
    short_balance   BIGINT,
    short_ratio     REAL,
    short_avg_price INTEGER,
    short_value     BIGINT,
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('short_selling', 'date', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS credit_balance (
    code        TEXT NOT NULL,
    date        DATE NOT NULL,
    close       INTEGER,
    new_qty     BIGINT,
    repay_qty   BIGINT,
    balance_qty BIGINT,
    balance_amt BIGINT,
    balance_rt  REAL,
    credit_rt   REAL,
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('credit_balance', 'date', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS sector_index (
    code        TEXT NOT NULL,
    name        TEXT,
    date        DATE NOT NULL,
    open        INTEGER,
    high        INTEGER,
    low         INTEGER,
    close       INTEGER,
    volume      BIGINT,
    trade_value BIGINT,
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('sector_index', 'date', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS shares_outstanding_history (
    code               TEXT NOT NULL,
    date               DATE NOT NULL,
    shares_outstanding BIGINT,  -- INTEGER(32bit, max~21억)로는 삼성전자 등 대형주 발행주식수(수십억주)가 오버플로우함
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('shares_outstanding_history', 'date', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_sh_date ON shares_outstanding_history(date);

-- 일반 테이블(하이퍼테이블 아님): 자연키가 (code, period)라 avail_date를 PK에
-- 넣을 수 없고, TimescaleDB는 파티션 컬럼(avail_date)이 빠진 유니크 인덱스를
-- 허용하지 않는다 — create_hypertable+PRIMARY KEY(code,period) 조합은 생성 시
-- 에러남(실제 DB로 검증됨). storage.upsert_earnings()도 ON CONFLICT(code,period)
-- 라 유니크 제약이 정확히 이 두 컬럼이어야 한다. 실적 데이터는 전종목 ~10년치도
-- 수만~십만 행 규모라 압축/청크 이점이 거의 없어 일반 테이블로 충분하다.
CREATE TABLE IF NOT EXISTS earnings (
    code            TEXT NOT NULL,
    period          TEXT NOT NULL,   -- e.g. '2020Q1'
    avail_date      DATE NOT NULL,   -- lookahead-safe availability date (period-end + filing lag)
    netinc          DOUBLE PRECISION,
    netinc_prior    DOUBLE PRECISION,
    revenue         DOUBLE PRECISION,
    revenue_prior   DOUBLE PRECISION,
    op_income       DOUBLE PRECISION,
    op_income_prior DOUBLE PRECISION,
    PRIMARY KEY (code, period)
);
CREATE INDEX IF NOT EXISTS idx_earnings_avail_date ON earnings(avail_date);

CREATE TABLE IF NOT EXISTS consensus (
    code         TEXT NOT NULL,
    date         DATE NOT NULL,
    target_mean  DOUBLE PRECISION,
    recomm_mean  DOUBLE PRECISION,
    base_date    TEXT,
    fwd_eps      DOUBLE PRECISION,
    prev_eps     DOUBLE PRECISION,
    est_year     TEXT,
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('consensus', 'date', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_consensus_date ON consensus(date);

-- 일반 테이블: 거래일당 1행뿐이라(연 ~250행) 하이퍼테이블/압축 이점이 없음.
CREATE TABLE IF NOT EXISTS minervini_scan (
    date         TEXT NOT NULL,
    breadth      DOUBLE PRECISION,
    regime       TEXT,
    n_candidates INTEGER,
    codes        TEXT,
    PRIMARY KEY (date)
);

CREATE TABLE IF NOT EXISTS daily_bars_adjusted (
    code        TEXT NOT NULL,
    date        DATE NOT NULL,
    open        DOUBLE PRECISION,  -- back-adjust 배수 적용 후라 daily_bars(INTEGER)와 달리 REAL/DOUBLE
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      BIGINT,            -- 미조정 원본 그대로(adjust_volume=False 기본값)
    trade_value BIGINT,
    PRIMARY KEY (code, date)
);
SELECT create_hypertable('daily_bars_adjusted', 'date', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_dba_date ON daily_bars_adjusted(date);

-- 일반 테이블: 종목당 1행뿐이고 시계열이 아니라 하이퍼테이블 대상 아님.
CREATE TABLE IF NOT EXISTS delisted_stocks (
    code            TEXT NOT NULL,
    name            TEXT,
    market          TEXT,
    last_trade_date TEXT,   -- daily_bars 기준 마지막 거래일(상장폐지일 근사), 이력 없으면 NULL
    PRIMARY KEY (code)
);

-- 일반 테이블: RBA 축적은 스캐너 픽 건수 기준이라 소규모, 하이퍼테이블 이점 없음.
CREATE TABLE IF NOT EXISTS minervini_rba (
    pick_date TEXT NOT NULL,  -- 스캐너가 진입후보로 뽑은 날짜
    code      TEXT NOT NULL,
    entry     REAL,
    exit_px   REAL,
    outcome   TEXT,   -- 'stop' / 'target_2R' / 'open'(20일 경과, 미확정 종료)
    ret_pct   REAL,
    days      INTEGER,
    PRIMARY KEY (pick_date, code)
);

-- Recent rows stay row-oriented (frequent upserts); anything older than 7
-- days is compressed columnar in the background — cuts disk use and speeds
-- up the long-range scans backtest/screener code does.
ALTER TABLE daily_bars SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
ALTER TABLE supply_demand SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
ALTER TABLE short_selling SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
ALTER TABLE credit_balance SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
ALTER TABLE sector_index SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
ALTER TABLE shares_outstanding_history SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
ALTER TABLE consensus SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
-- daily_bars_adjusted는 압축 대상에서 제외 — weekly_price_adjust가 매주 전체를
-- upsert로 재작성하므로, 압축을 걸면 매주 오래된 청크를 압축해제→재압축하는
-- 순환이 반복돼 이득 없이 CPU/IO만 낭비된다(주간 전체재생성 테이블 특성).

SELECT add_compression_policy('daily_bars', INTERVAL '7 days');
SELECT add_compression_policy('supply_demand', INTERVAL '7 days');
SELECT add_compression_policy('short_selling', INTERVAL '7 days');
SELECT add_compression_policy('credit_balance', INTERVAL '7 days');
SELECT add_compression_policy('sector_index', INTERVAL '7 days');
SELECT add_compression_policy('shares_outstanding_history', INTERVAL '7 days');
SELECT add_compression_policy('consensus', INTERVAL '7 days');
-- earnings는 일반 테이블이라 압축/보존 정책 대상 아님 (위 CREATE TABLE 주석 참고).
