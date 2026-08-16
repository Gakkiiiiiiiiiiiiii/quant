"""Data Quality Flags（详细修改方案 §18）。

- Market API 返回 quality_flags（标准词表）；
- 生产 Backtest 对 critical quality flag 默认拒绝，显式 allow_critical_quality 可放行。
"""
from __future__ import annotations

import time
from datetime import date

import pandas as pd

from quant_demo.api.v1_app import dispatch, get_state
from quant_demo.marketdata.quality import (
    CRITICAL_QUALITY_FLAGS,
    DUPLICATE_BAR,
    MISSING_BAR,
    OUT_OF_ORDER,
    STALE_DATA,
    STANDARD_QUALITY_FLAGS,
    critical_flags,
    evaluate_frame_quality,
)

from v1_fixtures import SAMPLE_SYMBOLS, build_state, history_dates


def test_standard_flag_vocabulary():
    expected = {
        "MISSING_BAR", "DUPLICATE_BAR", "OUT_OF_ORDER", "STALE_DATA",
        "UNRESOLVED_CORPORATE_ACTION", "MEMBERSHIP_GAP", "CALENDAR_MISMATCH", "UNKNOWN_PRICE_LIMIT",
    }
    assert set(STANDARD_QUALITY_FLAGS) == expected
    assert CRITICAL_QUALITY_FLAGS <= set(STANDARD_QUALITY_FLAGS)


def test_evaluate_frame_quality_detects_problems():
    clean = pd.DataFrame(
        [
            {"symbol": "A", "trading_date": date(2026, 8, 13), "close": 1.0},
            {"symbol": "A", "trading_date": date(2026, 8, 14), "close": 1.1},
        ]
    )
    assert evaluate_frame_quality(clean, ["A"], date(2026, 8, 14)) == []

    duplicated = pd.concat([clean, clean.iloc[[0]]], ignore_index=True)
    assert DUPLICATE_BAR in evaluate_frame_quality(duplicated, ["A"], date(2026, 8, 14))

    out_of_order = clean.iloc[::-1].reset_index(drop=True)
    assert OUT_OF_ORDER in evaluate_frame_quality(out_of_order, ["A"], date(2026, 8, 14))

    assert MISSING_BAR in evaluate_frame_quality(clean, ["A", "B"], date(2026, 8, 14))
    assert STALE_DATA in evaluate_frame_quality(clean, ["A"], date(2026, 8, 31))
    assert critical_flags([STALE_DATA, DUPLICATE_BAR]) == [DUPLICATE_BAR]


def test_bars_batch_returns_quality_flags(tmp_path):
    state = build_state(tmp_path, days=60)
    data = state.market.bars_batch(SAMPLE_SYMBOLS[:2], "1990-01-01", "2099-12-31")
    assert "quality_flags" in data
    # 合成历史干净：无 critical；end 超出现有数据时允许 STALE_DATA（非 critical）
    assert critical_flags(data["quality_flags"]) == []
    manifest = state.market._snapshot_store.read_manifest(data["data_snapshot_id"])  # noqa: SLF001
    assert manifest["quality_flags"] == data["quality_flags"]


def _wait_completion(backtest_id: str, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, payload = dispatch("GET", f"/api/v1/backtests/{backtest_id}", {}, {}, {})
        job = payload["data"]
        if job["status"] in {"COMPLETED", "FAILED"}:
            return job
        time.sleep(0.05)
    raise TimeoutError("backtest did not finish")


def test_backtest_rejects_critical_quality_by_default(tmp_path, monkeypatch):
    state = build_state(tmp_path, days=60)
    created = state.market.create_snapshot(SAMPLE_SYMBOLS[:2], "1990-01-01", "2099-12-31")
    snapshot_id = created["snapshot_id"]
    original_load = state.market.load_snapshot

    def poisoned_load(sid: str):
        data = original_load(sid)
        data["quality_flags"] = [DUPLICATE_BAR]  # 注入 critical 质量标志
        return data

    monkeypatch.setattr(get_state().market, "load_snapshot", poisoned_load)
    dates = history_dates(state)
    payload = {
        "market_snapshot_id": snapshot_id,
        "start": dates[0],
        "end": dates[-1],
        "strategy": {"type": "equal_weight", "version": "q-test", "rebalance_every_days": 10},
    }
    _, response = dispatch("POST", "/api/v1/backtests", {}, payload, {})
    job = _wait_completion(response["data"]["backtest_id"])
    assert job["status"] == "FAILED"
    assert job["error_code"] == "DATA_QUALITY_REJECTED"

    # 显式 allow_critical_quality -> 放行
    payload["allow_critical_quality"] = True
    payload["strategy"] = {"type": "equal_weight", "version": "q-test-allow", "rebalance_every_days": 10}
    _, response = dispatch("POST", "/api/v1/backtests", {}, payload, {})
    job = _wait_completion(response["data"]["backtest_id"])
    assert job["status"] == "COMPLETED"
