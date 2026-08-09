-- earnings: 정정공시 이력 보존 (knowledge_date 도입)
--
-- 이전 스키마는 PRIMARY KEY (code, period)라 (종목, 분기)당 행이 하나뿐이었다.
-- 지금까지는 --db-table 모드의 resume 가드(이미 있는 (code, period)는 재수집
-- 안 함)가 덮어쓰기를 막아줘서 사실상 "최초 보고치"가 보존돼 왔지만, 그 성질이
-- 스키마가 아니라 수집기 최적화에 의존하고 있었다. 가드를 걷어내는 순간(정정
-- 반영·백필 재실행 등) 과거가 조용히 재작성된다.
--
-- 이 마이그레이션은 그 성질을 스키마로 못박는다. 기존 행은 최초 보고치이므로
-- knowledge_date를 avail_date로 백필한다 — 그 시점에 알 수 있었던 값이라는
-- 뜻이고, 실제 수집일이 남아있지 않으므로 이게 가장 보수적인 근사다.
--
-- 적용:
--   psql "$DB_URL" -f sql/migrations/001_earnings_knowledge_date.sql
-- 되돌리기는 파일 하단 주석 참조.

BEGIN;

ALTER TABLE earnings ADD COLUMN IF NOT EXISTS knowledge_date DATE;

UPDATE earnings SET knowledge_date = avail_date WHERE knowledge_date IS NULL;

ALTER TABLE earnings ALTER COLUMN knowledge_date SET NOT NULL;

ALTER TABLE earnings DROP CONSTRAINT IF EXISTS earnings_pkey;
ALTER TABLE earnings ADD PRIMARY KEY (code, period, knowledge_date);

CREATE INDEX IF NOT EXISTS idx_earnings_asof
    ON earnings(code, period, knowledge_date DESC);

COMMIT;

-- 검증: 백필 후에는 (code, period)당 정확히 한 버전이어야 한다.
--   SELECT COUNT(*) FROM (
--     SELECT code, period FROM earnings GROUP BY code, period HAVING COUNT(*) > 1
--   ) t;   -- 기대값 0
--
-- 롤백(버전이 하나뿐일 때만 안전):
--   BEGIN;
--   ALTER TABLE earnings DROP CONSTRAINT earnings_pkey;
--   ALTER TABLE earnings ADD PRIMARY KEY (code, period);
--   DROP INDEX IF EXISTS idx_earnings_asof;
--   ALTER TABLE earnings DROP COLUMN knowledge_date;
--   COMMIT;
