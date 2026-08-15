

def test_build_headers_called_with_token_in_the_right_slot():
    """``_build_headers(api_id, token, cont_yn, next_key)`` — 인자 순서 회귀 방지.

    2026-08-15 까지 이 호출이 옛 순서라 token 자리에 "N" 이 들어가고 next_key=None 이
    헤더 값이 되어 AttributeError 로 죽었다. 이 경로를 부르는 DAG 가 없어서 아무도
    못 봤다 — 도달 불가능한 코드는 조용히 썩는다.
    """
    import collectors.supply_demand as sd

    seen = {}

    class FakeResp:
        status_code = 200
        headers = {"cont-yn": "N", "next-key": ""}

        def raise_for_status(self):
            pass

        def json(self):
            return {"return_code": 0, "stk_invsr_orgn": [{"dt": "20260812"}]}

    class FakeHttp:
        def post(self, url, headers=None, json=None):
            return FakeResp()

    class FakeBase:
        _max_retries = 0
        _retry_backoff = 0
        _rate_limiter = None
        _client = FakeHttp()

        def _current_token(self):
            return "TOKEN"

        def _build_headers(self, api_id, token, cont_yn="N", next_key="", extra=None):
            seen.update(api_id=api_id, token=token, cont_yn=cont_yn, next_key=next_key)
            return {}

    class FakeStockInfo:
        _client = FakeBase()
        RESOURCE_URL = "/x"

    class FakeApi:
        stock_info = FakeStockInfo()

        def login(self):
            pass

    rows = sd._fetch_investor_flow_pages(FakeApi(), "005930", "20260812", max_pages=1)
    assert rows and rows[0]["dt"] == "20260812"
    assert seen["token"] == "TOKEN", f"token 자리에 {seen['token']!r} 이 들어갔다"
    assert seen["cont_yn"] == "N"
    assert seen["next_key"] == ""
