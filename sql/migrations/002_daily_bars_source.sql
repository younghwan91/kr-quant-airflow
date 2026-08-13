-- daily_bars: 행의 출처를 기록 (생존편향 보정용 폐지종목 시세 유입 대비)
--
-- 지금까지 daily_bars 는 키움 ka10081 한 소스만 담았고, 그 소스는 **현재 상장된**
-- 종목만 돌려준다(fetch_stock_list). 상장폐지된 종목은 애초에 수집 루프에 못 들어와
-- 과거 시세가 통째로 비어 있었다 — 백테스트가 살아남은 회사만 보고 성적을 재는
-- 생존편향의 발생 지점이다.
--
-- 키움은 폐지 종목 일봉을 주지 않는다(2026-08-13 실측: return_code=0 "정상"에
-- 빈 행 1개 — 실패조차 조용하다). 네이버 siseJson 은 준다. 그래서 폐지 종목만
-- 두 번째 소스로 채우게 되고, 그 사실을 행 단위로 남긴다.
--
-- 왜 컬럼인가: 출처를 안 남기면 (1) 네이버 행의 trade_value 가 종가×거래량 근사치
-- 라는 사실이 실측치와 한 컬럼에 섞이고, (2) 나중에 "이 수치 어디서 왔나"를 못
-- 되짚고, (3) 재수집·정정 때 무엇을 덮어도 되는지 판단이 안 선다.
--
-- 적용:
--   psql "$DB_URL" -v ON_ERROR_STOP=1 -f sql/migrations/002_daily_bars_source.sql
--
-- daily_bars 는 압축 하이퍼테이블이지만 TimescaleDB 2.17 에서 ADD COLUMN ... DEFAULT
-- 와 압축 청크 INSERT 모두 지원된다(적용 전 롤백 트랜잭션으로 실측 확인).

BEGIN;

ALTER TABLE daily_bars ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'kiwoom';

COMMENT ON COLUMN daily_bars.source IS
    'kiwoom = ka10081 (상장 종목, trade_value 는 보고된 거래대금). '
    'naver = siseJson 백필 (폐지 종목, trade_value 는 close*volume/1e6 근사 — '
    '실측 오차 중앙값 0.7%%, p95 3.6%%).';

COMMIT;

-- 검증:
--   SELECT source, count(*) FROM daily_bars GROUP BY 1;   -- 적용 직후엔 kiwoom 만
--
-- 롤백:
--   ALTER TABLE daily_bars DROP COLUMN source;
