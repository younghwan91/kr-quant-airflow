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

-- Recent rows stay row-oriented (frequent upserts); anything older than 7
-- days is compressed columnar in the background — cuts disk use and speeds
-- up the long-range scans backtest/screener code does.
ALTER TABLE daily_bars SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
ALTER TABLE supply_demand SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
ALTER TABLE short_selling SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');
ALTER TABLE credit_balance SET (timescaledb.compress, timescaledb.compress_segmentby = 'code');

SELECT add_compression_policy('daily_bars', INTERVAL '7 days');
SELECT add_compression_policy('supply_demand', INTERVAL '7 days');
SELECT add_compression_policy('short_selling', INTERVAL '7 days');
SELECT add_compression_policy('credit_balance', INTERVAL '7 days');
