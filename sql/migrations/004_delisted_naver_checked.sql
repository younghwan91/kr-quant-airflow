-- delisted_stocks: 네이버 조회 결과가 "구간 내 데이터 없음"이었던 코드를 기록
--
-- naver_delisted_bars 는 주간 DAG 로 돈다. 이미 시세를 받은 코드는 건너뛰게 해뒀지만,
-- **빈 응답이 오는 코드는 매주 다시 조회한다** — 실측 1,758개 중 1,751개가 그렇다
-- (우리 시세 구간 2016-09 이전에 폐지돼 네이버에도 겹치는 데이터가 없는 종목들).
-- last_trade_date 로 거르려 해도 그 컬럼은 daily_bars 에서 파생하므로, 애초에 바가
-- 없는 이 코드들은 전부 NULL 이라 필터에 안 걸린다.
--
-- 상장폐지는 과거 사실이라 한 번 "구간 내 데이터 없음"이면 영원히 그렇다. 그래서
-- 확인한 날짜를 남기고 다음 주부터 건너뛴다. 주간 외부 요청이 ~1,750회에서 신규
-- 폐지분 수준으로 떨어진다.
--
-- 적용:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -f sql/migrations/004_delisted_naver_checked.sql
--
-- 다시 훑고 싶으면(구간 시작을 앞당겼다거나): UPDATE delisted_stocks SET naver_checked = NULL;
-- 또는 수집기에 --refetch.

BEGIN;

ALTER TABLE delisted_stocks ADD COLUMN IF NOT EXISTS naver_checked DATE;

COMMENT ON COLUMN delisted_stocks.naver_checked IS
    '네이버 siseJson 을 조회했으나 우리 시세 구간 내 데이터가 없던 날. '
    'NULL = 아직 확인 안 함. 데이터를 받은 코드는 daily_bars 에 source=naver 로 남으므로 '
    '이 컬럼을 쓰지 않는다.';

COMMIT;

-- 검증:
--   SELECT count(*) FILTER (WHERE naver_checked IS NULL) AS 미확인,
--          count(*) FILTER (WHERE naver_checked IS NOT NULL) AS 확인됨
--     FROM delisted_stocks;
--
-- 롤백:
--   ALTER TABLE delisted_stocks DROP COLUMN naver_checked;
