-- supply_demand: 행의 출처 기록 (폐지 종목 부분 백필 대비)
--
-- 폐지 종목 수급은 키움에서 못 받는다 — ka10059 가 폐지 코드에 return_code=0(성공) +
-- 0행을 준다(2026-08-15 실측). 네이버 frgn.naver 는 폐지분도 주지만 **항목이 일부뿐**
-- 이라, 같은 테이블에 성격이 다른 행이 섞이게 된다.
--
-- 네이버가 채우는 것: close, acc_trde_qty, institution, foreign_
-- 못 채우는 것:      individual, etc_corp, 기관 세부 8종(fnnc_invt·insrnc·invtrt·
--                    bank·penfnd_etc·samo_fund·natn), flu_rt
--
-- **정의도 미묘하게 다르다.** 네이버 '외국인'은 개인+외국인+기관+기타법인 합이 0이
-- 되도록 맞춘 값이고, 키움 foreign_ 은 순수 외국인이라 잔차가 남는다(삼성전자
-- 2026-08-12 실측: 키움 5,818,519 vs 네이버 5,802,466, 차이 16,053 = 그날 잔차와 일치).
-- 두 값을 구분 없이 섞으면 이 차이가 조용히 신호에 들어간다.
--
-- NULL 과 0 의 구분이 특히 중요하다: individual 이 NULL 이면 "모름"이고 0 이면
-- "순매매 없음"이다. 네이버 행은 NULL 로 남겨야 하며, 읽는 쪽은 source 를 보고
-- 판단해야 한다.
--
-- 적용:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -f sql/migrations/006_supply_demand_source.sql

BEGIN;

ALTER TABLE supply_demand ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'kiwoom';

COMMENT ON COLUMN supply_demand.source IS
    'kiwoom(ka10059, 11개 분류 전체) / naver(폐지 종목 부분 백필 — 기관·외국인 순매매만, '
    '외국인 정의가 키움과 다름). naver 행은 individual 등이 NULL 이며 0 과 구분해야 한다.';

COMMIT;

-- 검증:
--   SELECT source, count(*), count(individual) AS individual_notnull
--     FROM supply_demand GROUP BY 1;   -- naver 행은 individual_notnull=0 이어야 한다
--
-- 롤백:
--   ALTER TABLE supply_demand DROP COLUMN source;
