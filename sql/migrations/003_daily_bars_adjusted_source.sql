-- daily_bars_adjusted: 행의 출처를 전파 (002 의 짝)
--
-- 002 가 daily_bars 에 source 를 넣었지만, **백테스트가 실제로 읽는 테이블은
-- daily_bars_adjusted 다**(PRICE_TABLE = "daily_bars_adjusted"). rebuild_adjusted_table
-- 이 source 를 빼고 SELECT 했기 때문에, "이 행의 trade_value 는 close*volume 근사치"
-- 라는 사실이 정작 소비되는 쪽에는 없었다.
--
-- 왜 중요한가: 유니버스 편입을 가르는 ADV 문턱(features/universe.py ADV_FLOOR,
-- sim_crosssectional adv_floor)이 전적으로 trade_value 로 돈다. 폐지 종목의 그 값이
-- 실측이 아니라는 정보가 데이터에 없으면, 근사 오차가 유니버스 경계 종목의 편입 여부를
-- 바꿔도 아무도 그 사실을 되짚을 수 없다.
--
-- 적용:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -f sql/migrations/003_daily_bars_adjusted_source.sql
--   그 뒤 조정가 테이블을 재생성해야 기존 행의 source 가 실제 값으로 채워진다:
--   python -m kr_quant.price_adjust --rebuild-db --db "$DB_URL"   (약 9분)
--
-- daily_bars_adjusted 는 압축 대상이 아니라(weekly_price_adjust 가 매주 전량 재작성)
-- ADD COLUMN 이 가볍다.

BEGIN;

ALTER TABLE daily_bars_adjusted
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'kiwoom';

COMMENT ON COLUMN daily_bars_adjusted.source IS
    'daily_bars.source 를 그대로 전파한 값. kiwoom = 보고된 거래대금, '
    'naver = close*volume/1e6 근사(폐지 종목 백필).';

COMMIT;

-- 검증(재생성 후): 두 테이블의 source 분포가 같아야 한다.
--   SELECT source, count(*) FROM daily_bars GROUP BY 1;
--   SELECT source, count(*) FROM daily_bars_adjusted GROUP BY 1;
--
-- 롤백:
--   ALTER TABLE daily_bars_adjusted DROP COLUMN source;
