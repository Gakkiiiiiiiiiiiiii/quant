"""Backtest Snapshot Replay 确定性测试（详细修改方案 §3.4）。

相同 Snapshot + 相同 StrategySpec + 相同 Execution Config：
orders/trades/equity/metrics 必须完全一致（内容哈希一致）。
"""
from __future__ import annotations

import hashlib
import json
import time

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


def _result_hashes(backtest_id: str) -> dict[str, str]:
    hashes: dict[str, str] = {}
    sections = {
        "metrics": "metrics",
        "equity": "equity",
        "trades": "trades",
        "positions": "positions",
        "daily_actions": "daily-actions",
    }
    for section, path in sections.items():
        status, payload = dispatch("GET", f"/api/v1/backtests/{backtest_id}/{path}", {}, {}, {})
        assert status == 200, section
        hashes[section] = hashlib.sha256(
            json.dumps(payload["data"], ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
    return hashes


def _spec_hash(backtest_id: str) -> str:
    status, payload = dispatch("GET", f"/api/v1/backtests/{backtest_id}/lineage", {}, {}, {})
    assert status == 200
    spec_hash = payload["data"].get("backtest_spec_hash")
    assert spec_hash, "lineage 必须包含 backtest_spec_hash"
    return spec_hash


def _create_backtest(snapshot_id: str, start: str, end: str, version: str) -> str:
    status, payload = dispatch(
        "POST",
        "/api/v1/backtests",
        {},
        {
            "market_snapshot_id": snapshot_id,
            "strategy": {"type": "equal_weight", "version": version, "rebalance_every_days": 20},
            "start": start,
            "end": end,
            "initial_cash": 500_000,
        },
        {},
    )
    assert status == 202
    return payload["data"]["backtest_id"]


def test_backtest_replay_deterministic_across_runs(tmp_path):
    state = build_state(tmp_path, days=200)
    dates = history_dates(state)
    start, end = dates[0], dates[-1]
    data = state.market.bars_batch(SAMPLE_SYMBOLS[:2], start, end)
    snapshot_id = data["data_snapshot_id"]

    # 两个不同 job（不同 strategy.version 标签避免 reproducibility hash 复用）
    first_id = _create_backtest(snapshot_id, start, end, "replay-a")
    second_id = _create_backtest(snapshot_id, start, end, "replay-b")
    assert _wait_completion(first_id)["status"] == "COMPLETED"
    assert _wait_completion(second_id)["status"] == "COMPLETED"

    first_hashes = _result_hashes(first_id)
    second_hashes = _result_hashes(second_id)
    assert first_hashes == second_hashes, "同 Snapshot + 同 StrategySpec 结果必须一致"
    # P0 Q-04：两次运行均携带 backtest_spec_hash 身份。
    assert _spec_hash(first_id)
    assert _spec_hash(second_id)


def test_backtest_replay_stable_after_source_correction(tmp_path):
    """原始行情事后修正，指定 snapshot 的回测结果不变。"""
    from v1_fixtures import fresh_market, rewrite_history

    state = build_state(tmp_path, days=200)
    dates = history_dates(state)
    start, end = dates[0], dates[-1]
    data = state.market.bars_batch(SAMPLE_SYMBOLS[:2], start, end)
    snapshot_id = data["data_snapshot_id"]

    backtest_id = _create_backtest(snapshot_id, start, end, "replay-correction")
    assert _wait_completion(backtest_id)["status"] == "COMPLETED"
    before = _result_hashes(backtest_id)

    rewrite_history(tmp_path / "history.parquet", close_multiplier=1.3)
    fresh_market(tmp_path)  # 模拟服务重启后读取“修正后的当前行情”

    rerun_id = _create_backtest(snapshot_id, start, end, "replay-correction-rerun")
    assert _wait_completion(rerun_id)["status"] == "COMPLETED"
    after = _result_hashes(rerun_id)
    assert before == after, "快照模式下行情修正不得改变回测结果"


def test_reproducibility_hash_reuses_identical_spec(tmp_path):
    """完全相同的 spec 直接复用既有结果（内容寻址）。"""
    state = build_state(tmp_path, days=200)
    dates = history_dates(state)
    start, end = dates[0], dates[-1]
    data = state.market.bars_batch(SAMPLE_SYMBOLS[:2], start, end)
    payload = {
        "market_snapshot_id": data["data_snapshot_id"],
        "strategy": {"type": "equal_weight", "version": "same"},
        "start": start,
        "end": end,
        "initial_cash": 500_000,
    }
    _, first = dispatch("POST", "/api/v1/backtests", {}, payload, {})
    _wait_completion(first["data"]["backtest_id"])
    spec_hash_first = _spec_hash(first["data"]["backtest_id"])
    _, second = dispatch("POST", "/api/v1/backtests", {}, payload, {})
    assert second["data"]["backtest_id"] == first["data"]["backtest_id"]
    assert second["data"].get("reused") is True
    # P0 Q-04：固定全部身份输入（snapshot/spec/cash/execution/cost/benchmark/code）→ 同 spec hash。
    assert _spec_hash(second["data"]["backtest_id"]) == spec_hash_first
