"""Snapshot Corruption 检测测试（详细修改方案 §3.3）。

流程：
- create snapshot → 手工修改 parquet bytes → load → SNAPSHOT_CORRUPTED；
- 再修改 manifest → load → SNAPSHOT_CORRUPTED。
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from quant_demo.snapshot.store import DATASET_FILENAME, MANIFEST_FILENAME, SnapshotCorruptedError, SnapshotNotFoundError

from v1_fixtures import SAMPLE_SYMBOLS, build_state


def _dataset_path(state, snapshot_id: str) -> Path:
    return Path(state.market._snapshot_store._directory(snapshot_id)) / DATASET_FILENAME  # noqa: SLF001


def _manifest_path(state, snapshot_id: str) -> Path:
    return Path(state.market._snapshot_store._directory(snapshot_id)) / MANIFEST_FILENAME  # noqa: SLF001


def test_tampered_parquet_detected_as_corrupted(tmp_path):
    state = build_state(tmp_path, days=60)
    created = state.market.create_snapshot(SAMPLE_SYMBOLS[:2], "1990-01-01", "2099-12-31")
    snapshot_id = created["snapshot_id"]

    dataset = _dataset_path(state, snapshot_id)
    raw = bytearray(dataset.read_bytes())
    raw[-1] = (raw[-1] + 1) % 256  # 篡改尾部字节
    dataset.write_bytes(bytes(raw))

    with pytest.raises(SnapshotCorruptedError):
        state.market.load_snapshot(snapshot_id)


def test_tampered_manifest_detected_as_corrupted(tmp_path):
    state = build_state(tmp_path, days=60)
    created = state.market.create_snapshot(SAMPLE_SYMBOLS[:2], "1990-01-01", "2099-12-31")
    snapshot_id = created["snapshot_id"]

    manifest_path = _manifest_path(state, snapshot_id)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"][0]["sha256"] = "0" * 64  # 伪造哈希
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(SnapshotCorruptedError):
        state.market.load_snapshot(snapshot_id)


def test_missing_snapshot_raises_not_found(tmp_path):
    state = build_state(tmp_path, days=60)
    with pytest.raises(SnapshotNotFoundError):
        state.market.load_snapshot("mds-does-not-exist")


def test_backtest_api_reports_snapshot_corrupted(tmp_path):
    """通过 API 创建回测并指定已损坏快照 → 显式 SNAPSHOT_CORRUPTED 错误码。"""
    from quant_demo.api.v1_app import dispatch

    state = build_state(tmp_path, days=120)
    created = state.market.create_snapshot(SAMPLE_SYMBOLS[:2], "1990-01-01", "2099-12-31")
    snapshot_id = created["snapshot_id"]

    dataset = _dataset_path(state, snapshot_id)
    raw = bytearray(dataset.read_bytes())
    raw[0] = (raw[0] + 1) % 256
    dataset.write_bytes(bytes(raw))

    status, payload = dispatch(
        "POST",
        "/api/v1/backtests",
        {},
        {
            "market_snapshot_id": snapshot_id,
            "strategy": {"type": "equal_weight", "version": "corruption-test"},
            "start": "1990-01-01",
            "end": "2099-12-31",
        },
        {},
    )
    assert status == 202
    backtest_id = payload["data"]["backtest_id"]

    import time

    deadline = time.time() + 30
    while time.time() < deadline:
        _, job_payload = dispatch("GET", f"/api/v1/backtests/{backtest_id}", {}, {}, {})
        job = job_payload["data"]
        if job["status"] in {"COMPLETED", "FAILED"}:
            break
        time.sleep(0.05)
    assert job["status"] == "FAILED"
    assert job.get("error_code") == "SNAPSHOT_CORRUPTED"
