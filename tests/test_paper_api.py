"""Quant Paper API 测试（设计文档 §20 / §21 / §33）。"""
from __future__ import annotations

from quant_demo.api.v1_app import dispatch

from v1_fixtures import SAMPLE_SYMBOLS, build_state, history_dates


def _create_account(initial_cash: float = 1_000_000.0) -> str:
    status, payload = dispatch("POST", "/api/v1/paper/accounts", {}, {"name": "paper-test", "initial_cash": initial_cash}, {})
    assert status == 201
    return payload["data"]["account_id"]


def test_paper_full_flow(tmp_path):
    state = build_state(tmp_path, days=60)
    dates = history_dates(state, SAMPLE_SYMBOLS[:2])
    as_of, execute_on = dates[5], dates[6]
    account_id = _create_account()

    status, payload = dispatch(
        "POST", "/api/v1/paper/orders/generate", {},
        {
            "account_id": account_id,
            "as_of": as_of,
            "scores": [
                {"symbol": SAMPLE_SYMBOLS[0], "score": 0.9, "reference_price": 10.0},
                {"symbol": SAMPLE_SYMBOLS[1], "score": 0.5, "reference_price": 12.0},
            ],
            "top_k": 2,
        },
        {"Idempotency-Key": "paper-gen-1"},
    )
    assert status == 200
    orders = payload["data"]["orders"]
    assert orders and all(order["side"] == "buy" for order in orders)
    assert payload["data"]["execute_on"] == execute_on

    # 幂等：同一 Idempotency-Key 不产生新订单（§33）
    status, replayed = dispatch(
        "POST", "/api/v1/paper/orders/generate", {},
        {"account_id": account_id, "as_of": as_of, "scores": [{"symbol": SAMPLE_SYMBOLS[0], "score": 0.9, "reference_price": 10.0}], "top_k": 2},
        {"Idempotency-Key": "paper-gen-1"},
    )
    assert status == 200
    assert replayed["data"]["reused"] is True
    assert len(replayed["data"]["orders"]) == len(orders)

    # 用执行日行情撮合
    data = state.market.bars_batch(SAMPLE_SYMBOLS[:2], execute_on, execute_on)
    market_prices = {
        data["symbols"][i]: {field: data["bars"][field][i][0] for field in ("open", "close", "high", "low")}
        for i in range(len(data["symbols"]))
        if data["dates"]
    }
    status, run_payload = dispatch(
        "POST", "/api/v1/paper/run", {},
        {"account_id": account_id, "as_of": execute_on, "market_prices": market_prices}, {},
    )
    assert status == 200
    filled = run_payload["data"]["filled"]
    assert filled, run_payload["data"]
    equity = run_payload["data"]["equity"]
    assert equity["total_asset"] > 0

    # T+1：买入当日不可卖（sellable=0）
    status, positions_payload = dispatch("GET", f"/api/v1/paper/accounts/{account_id}/positions", {}, {}, {})
    assert status == 200
    for position in positions_payload["data"]:
        if position["qty"] > 0:
            assert position["sellable_qty"] == 0

    for section in ("orders", "trades", "equity"):
        status, section_payload = dispatch("GET", f"/api/v1/paper/accounts/{account_id}/{section}", {}, {}, {})
        assert status == 200
        assert section_payload["data"] is not None

    status, account_payload = dispatch("GET", f"/api/v1/paper/accounts/{account_id}", {}, {}, {})
    assert status == 200
    assert account_payload["data"]["account_id"] == account_id


def test_paper_plan_endpoint(tmp_path):
    build_state(tmp_path)
    account_id = _create_account()
    status, payload = dispatch(
        "POST", "/api/v1/paper/plans", {},
        {
            "account_id": account_id,
            "targets": [{"symbol": SAMPLE_SYMBOLS[0], "target_weight": 0.5}],
            "time_contract": {
                "signal_time": "2026-08-14T15:00:00",
                "available_at": "2026-08-14T15:05:00",
                "executable_from": "2026-08-15T09:30:00",
            },
        }, {},
    )
    assert status == 201
    assert payload["data"]["plan_id"]


def test_paper_t1_sell_rejected_same_day(tmp_path):
    state = build_state(tmp_path, days=60)
    dates = history_dates(state, [SAMPLE_SYMBOLS[0]])
    as_of, execute_on = dates[5], dates[6]
    account_id = _create_account()
    dispatch(
        "POST", "/api/v1/paper/orders/generate", {},
        {"account_id": account_id, "as_of": as_of, "scores": [{"symbol": SAMPLE_SYMBOLS[0], "score": 1.0, "reference_price": 10.0}], "top_k": 1}, {},
    )
    prices = {SAMPLE_SYMBOLS[0]: {"open": 10.0, "close": 10.2, "high": 10.3, "low": 9.9}}
    dispatch("POST", "/api/v1/paper/run", {}, {"account_id": account_id, "as_of": execute_on, "market_prices": prices}, {})
    # 手工卖出订单：当日买入不可卖 -> REJECTED
    from quant_demo.api.v1_app import get_state
    from quant_demo.db.models_market import PaperOrderRow
    from datetime import date

    state2 = get_state()
    with state2.paper._session_factory() as session:  # noqa: SLF001
        session.add(PaperOrderRow(
            account_id=account_id, trading_date=date.fromisoformat(execute_on),
            symbol=SAMPLE_SYMBOLS[0], side="sell", qty=100,
        ))
        session.commit()
    _, run_payload = dispatch("POST", "/api/v1/paper/run", {}, {"account_id": account_id, "as_of": execute_on, "market_prices": prices}, {})
    rejected = run_payload["data"]["rejected"]
    assert any(item["reason"] == "T1_NO_SELLABLE" for item in rejected)
