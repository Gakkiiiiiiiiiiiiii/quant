"""Snapshot verify / Backtest lineage / replay API（详细修改方案 §12 / §14 / §20）。"""
from __future__ import annotations

import json
import time
from pathlib import Path

from quant_demo.api.v1_app import dispatch
from quant_demo.snapshot.store import DATASET_FILENAME

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


def _create_backtest(state, snapshot_id: str) -> str:
    dates = history_dates(state)
    payload = {
        "market_snapshot_id": snapshot_id,
        "start": dates[0],
        "end": dates[-1],
        "initial_cash": 1_000_000,
        "strategy": {"type": "equal_weight", "version": "lineage-test", "rebalance_every_days": 10},
    }
    _, response = dispatch("POST", "/api/v1/backtests", {}, payload, {})
    backtest_id = response["data"]["backtest_id"]
    job = _wait_completion(backtest_id)
    assert job["status"] == "COMPLETED"
    return backtest_id


def test_snapshot_verify_endpoint(tmp_path):
    state = build_state(tmp_path, days=60)
    created = state.market.create_snapshot(SAMPLE_SYMBOLS[:2], "1990-01-01", "2099-12-31")
    snapshot_id = created["snapshot_id"]

    status, payload = dispatch("POST", f"/api/v1/market/snapshots/{snapshot_id}/verify", {}, {}, {})
    assert status == 200
    assert payload["data"]["verified"] is True
    assert payload["data"]["files"]

    # 篡改 parquet -> SNAPSHOT_CORRUPTED
    directory = Path(state.market._snapshot_store._directory(snapshot_id))  # noqa: SLF001
    dataset = directory / DATASET_FILENAME
    raw = bytearray(dataset.read_bytes())
    raw[-1] ^= 0xFF
    dataset.write_bytes(bytes(raw))
    status, payload = dispatch("POST", f"/api/v1/market/snapshots/{snapshot_id}/verify", {}, {}, {})
    assert status == 409
    assert payload["error"]["code"] == "SNAPSHOT_CORRUPTED"

    # 不存在的快照 -> 404
    status, _ = dispatch("POST", "/api/v1/market/snapshots/mds-missing/verify", {}, {}, {})
    assert status == 404


def test_backtest_lineage_contains_spec_identity(tmp_path):
    state = build_state(tmp_path, days=60)
    created = state.market.create_snapshot(SAMPLE_SYMBOLS[:2], "1990-01-01", "2099-12-31")
    backtest_id = _create_backtest(state, created["snapshot_id"])

    status, payload = dispatch("GET", f"/api/v1/backtests/{backtest_id}/lineage", {}, {}, {})
    assert status == 200
    lineage = payload["data"]
    assert lineage["backtest_spec_hash"]
    assert lineage["execution_model_version"] == "execution-model.v1"
    assert lineage["transaction_cost_version"] == "transaction-cost.v1"
    assert lineage["market_snapshot"]["snapshot_id"] == created["snapshot_id"]
    assert lineage["code_sha"]
    assert lineage["strategy"]["version"] == "lineage-test"

    # diagnostics 同样携带 spec 身份（§14：版本必须写进 BacktestResult）
    _, diag_payload = dispatch("GET", f"/api/v1/backtests/{backtest_id}/diagnostics", {}, {}, {})
    diagnostics = diag_payload["data"]
    assert diagnostics["backtest_spec_hash"] == lineage["backtest_spec_hash"]
    assert diagnostics["execution_model_version"] == "execution-model.v1"


def test_backtest_replay_endpoint_matches_original(tmp_path):
    state = build_state(tmp_path, days=60)
    created = state.market.create_snapshot(SAMPLE_SYMBOLS[:2], "1990-01-01", "2099-12-31")
    backtest_id = _create_backtest(state, created["snapshot_id"])

    status, payload = dispatch("POST", f"/api/v1/backtests/{backtest_id}/replay", {}, {}, {})
    assert status == 200
    replay = payload["data"]
    assert replay["match"] is True
    assert replay["stored_result_hash"] == replay["replay_result_hash"]
    assert replay["replayed"]["market_snapshot_id"] == created["snapshot_id"]

    status, _ = dispatch("POST", "/api/v1/backtests/bt-missing/replay", {}, {}, {})
    assert status == 404


def test_spec_hash_stable_for_same_request_material(tmp_path):
    """相同请求材料（不含时间戳类字段）必须产生相同 backtest_spec_hash。"""
    from quant_demo.application.backtest_service import spec_hash_of

    material = {
        "strategy_id": "equal_weight",
        "strategy_version": "v1",
        "strategy_config_hash": "abc",
        "market_snapshot_id": "mds-x",
        "execution_model_version": "execution-model.v1",
        "transaction_cost_version": "transaction-cost.v1",
        "initial_cash": 1_000_000.0,
        "benchmark": None,
        "code_sha": "deadbeef",
    }
    first = spec_hash_of(**material)
    second = spec_hash_of(**material)
    assert first == second
    assert len(json.dumps(material)) > 0 and len(first) == 64
