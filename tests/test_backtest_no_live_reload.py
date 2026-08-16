"""Backtest No Live Reload 测试（详细修改方案 §3.5）。

指定 market_snapshot_id 后，即使 mock 的 live provider 被投毒，
Backtest 也绝不允许访问 live provider —— 访问即测试失败。
"""
from __future__ import annotations

import time

import pytest

from quant_demo.api.v1_app import dispatch

from v1_fixtures import SAMPLE_SYMBOLS, build_state, history_dates


def _wait_completion(backtest_id: str, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, payload = dispatch("GET", f"/api/v1/backtests/{backtest_id}", {}, {}, {})
        job = payload["data"]
        if job["status"] in {"COMPLETED", "FAILED"}:
            return job
        time.sleep(0.05)
    raise TimeoutError("backtest did not finish")


class _PoisonedLiveProvider:
    """任何对 live/current 行情源的访问都会立即使测试失败。"""

    def __init__(self) -> None:
        self.accessed: list[str] = []

    def bars_batch(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003, ARG002
        self.accessed.append("bars_batch")
        pytest.fail("Backtest 在 snapshot 模式下访问了 live provider（bars_batch）")

    def _load_frame(self):
        self.accessed.append("_load_frame")
        pytest.fail("Backtest 在 snapshot 模式下访问了 live provider（_load_frame）")


def test_snapshot_backtest_never_touches_live_provider(tmp_path, monkeypatch):
    state = build_state(tmp_path, days=200)
    dates = history_dates(state)
    start, end = dates[0], dates[-1]
    data = state.market.bars_batch(SAMPLE_SYMBOLS[:2], start, end)
    snapshot_id = data["data_snapshot_id"]

    # 快照创建完成后投毒：替换 current source 的读取入口。
    poisoned = _PoisonedLiveProvider()
    monkeypatch.setattr(state.market, "bars_batch", poisoned.bars_batch)
    monkeypatch.setattr(state.market, "_load_frame", poisoned._load_frame)

    status, payload = dispatch(
        "POST",
        "/api/v1/backtests",
        {},
        {
            "market_snapshot_id": snapshot_id,
            "strategy": {"type": "equal_weight", "version": "no-live"},
            "start": start,
            "end": end,
            "initial_cash": 500_000,
        },
        {},
    )
    assert status == 202
    job = _wait_completion(payload["data"]["backtest_id"])
    assert job["status"] == "COMPLETED", job
    assert poisoned.accessed == [], "snapshot 模式不得重新读取当前行情"


def test_snapshot_backtest_ignores_poisoned_history_file(tmp_path):
    """直接改写 history parquet 并重建 market，回测仍使用快照数据。"""
    from v1_fixtures import fresh_market, rewrite_history

    state = build_state(tmp_path, days=200)
    dates = history_dates(state)
    start, end = dates[0], dates[-1]
    data = state.market.bars_batch(SAMPLE_SYMBOLS[:2], start, end)
    snapshot_id = data["data_snapshot_id"]

    status, payload = dispatch(
        "POST",
        "/api/v1/backtests",
        {},
        {
            "market_snapshot_id": snapshot_id,
            "strategy": {"type": "equal_weight", "version": "no-live-2"},
            "start": start,
            "end": end,
            "initial_cash": 500_000,
        },
        {},
    )
    assert status == 202
    backtest_id = payload["data"]["backtest_id"]
    assert _wait_completion(backtest_id)["status"] == "COMPLETED"
    _, before = dispatch("GET", f"/api/v1/backtests/{backtest_id}/metrics", {}, {}, {})

    # 当前行情被彻底改写，且 market 服务以新源重建
    rewrite_history(tmp_path / "history.parquet", close_multiplier=2.0)
    fresh_market(tmp_path)

    status, payload = dispatch(
        "POST",
        "/api/v1/backtests",
        {},
        {
            "market_snapshot_id": snapshot_id,
            "strategy": {"type": "equal_weight", "version": "no-live-2-rerun"},
            "start": start,
            "end": end,
            "initial_cash": 500_000,
        },
        {},
    )
    assert status == 202
    rerun_id = payload["data"]["backtest_id"]
    assert _wait_completion(rerun_id)["status"] == "COMPLETED"
    _, after = dispatch("GET", f"/api/v1/backtests/{rerun_id}/metrics", {}, {}, {})
    assert before["data"]["metrics"] == after["data"]["metrics"]
