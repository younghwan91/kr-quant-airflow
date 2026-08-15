-- shares_outstanding_history: 출처와 인지일 기록 (폐지 종목 DART 백필 대비)
--
-- 이 테이블은 시가총액의 분모다. 폐지 종목이 없으면 cap 기반 유니버스가 생존자만
-- 담는다(GUARDRAILS §4 공백 2) — 시세·실적을 메워도 그 경로는 편향이 남는다.
--
-- KRX MDCSTAT01501(전종목 시세)은 날짜 파라미터·전종목·무인증이라 원래 이 용도에
-- 이상적이었으나, KRX 가 MDCSTAT 계열에 로그인을 걸면서 응답이 0행이 됐다
-- (2026-08-15 실측). 그래서 폐지분은 DART stockTotqySttus 로 받는다.
--
-- 두 컬럼을 더한다:
--   source         'kiwoom'(ka10001 현재 스냅샷) / 'krx'(MDCSTAT, 현재는 사실상 중단)
--                  / 'dart'(폐지 종목 백필, 보고서 기준일의 발행주식총수)
--   knowledge_date 이 값을 알 수 있게 된 날. DART 는 기준일(stlm_dt)과 공시 접수일이
--                  다르다 — 2025-12-31 기준 수치가 2026-03-10 에 공시된다. date 는
--                  기준일이라 backward as-of 조회가 기존 행과 섞이지만, 엄밀한 PIT 를
--                  물으면 접수일을 봐야 하므로 함께 남긴다(earnings 와 같은 구조).
--
-- 적용:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -f sql/migrations/005_shares_source_knowledge.sql

BEGIN;

ALTER TABLE shares_outstanding_history
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'kiwoom';
ALTER TABLE shares_outstanding_history
    ADD COLUMN IF NOT EXISTS knowledge_date DATE;

COMMENT ON COLUMN shares_outstanding_history.source IS
    'kiwoom(ka10001 스냅샷) / krx(MDCSTAT, 로그인 장벽으로 중단) / dart(폐지 종목 백필)';
COMMENT ON COLUMN shares_outstanding_history.knowledge_date IS
    '이 수치를 알 수 있게 된 날. DART 는 보고서 기준일(date)과 공시 접수일이 다르다.';

COMMIT;

-- 검증:
--   SELECT source, count(*), count(DISTINCT code) FROM shares_outstanding_history GROUP BY 1;
--
-- 롤백:
--   ALTER TABLE shares_outstanding_history DROP COLUMN source, DROP COLUMN knowledge_date;
