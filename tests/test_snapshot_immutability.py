"""Snapshot 不可变性测试（详细修改方案 §3.1）。

规则：
- 同 snapshot_id 不允许覆盖数据；
- 只有内容完全一致且操作幂等时才允许复用；
- 原始行情事后修正不得改变既有快照内容。
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from quant_demo.snapshot.store import (
    DATASET_FILENAME,
    LocalParquetSnapshotStore,
    SnapshotImmutableConflictError,
)

from v1_fixtures import SAMPLE_SYMBOLS, build_state, fresh_market, rewrite_history


def _dataset_sha256(store: LocalParquetSnapshotStore, snapshot_id: str) -> str:
    path = Path(store._directory(snapshot_id)) / DATASET_FILENAME  # noqa: SLF001
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_snapshot_cannot_be_overwritten(tmp_path):
    state = build_state(tmp_path, days=60)
    data = state.market.bars_batch(SAMPLE_SYMBOLS[:2], "1990-01-01", "2099-12-31")
    snapshot_id = data["data_snapshot_id"]
    store = state.market._snapshot_store  # noqa: SLF001
    original_sha = _dataset_sha256(store, snapshot_id)
    original_manifest = store.read_manifest(snapshot_id)

    # Q-02：同 snapshot_id + 不同内容必须硬失败，不得静默覆盖/复用。
    import pandas as pd

    with pytest.raises(SnapshotImmutableConflictError):
        store.save(snapshot_id, pd.DataFrame({"symbol": ["999999.SH"]}), {"hijack": True})

    assert _dataset_sha256(store, snapshot_id) == original_sha, "同 snapshot_id 覆盖数据必须被拒绝"
    assert store.read_manifest(snapshot_id) == original_manifest
    assert "hijack" not in store.read_manifest(snapshot_id)


def test_snapshot_reuse_is_idempotent(tmp_path):
    state = build_state(tmp_path, days=60)
    first = state.market.bars_batch(SAMPLE_SYMBOLS[:2], "1990-01-01", "2099-12-31")
    second = state.market.bars_batch(SAMPLE_SYMBOLS[:2], "1990-01-01", "2099-12-31")
    assert first["data_snapshot_id"] == second["data_snapshot_id"]
    assert first["data_version"] == second["data_version"]


def test_snapshot_survives_source_correction(tmp_path):
    """原始行情后来被修正时，已创建的快照重读必须完全一致。"""
    state = build_state(tmp_path, days=60)
    created = state.market.create_snapshot(SAMPLE_SYMBOLS[:2], "1990-01-01", "2099-12-31")
    snapshot_id = created["snapshot_id"]
    before = state.market.load_snapshot(snapshot_id)

    rewrite_history(tmp_path / "history.parquet", close_multiplier=1.5)
    market = fresh_market(tmp_path)

    after = market.load_snapshot(snapshot_id)
    assert after["bars"] == before["bars"]
    assert after["data_version"] == before["data_version"]
    assert after["dates"] == before["dates"]


def test_store_save_returns_existing_location_without_write(tmp_path):
    store = LocalParquetSnapshotStore(tmp_path / "snapshots")
    import pandas as pd

    frame = pd.DataFrame({"symbol": ["600519.SH"], "close": [10.0]})
    manifest = {"schema_version": "market-snapshot.v1"}
    first = store.save("mds-test", frame, manifest)
    manifest_path = Path(first.manifest_uri)
    manifest_bytes_before = manifest_path.read_bytes()

    # 同 ID + 同内容：幂等复用，不重新落盘。
    second = store.save("mds-test", frame.copy(), dict(manifest))
    assert second.manifest_hash == first.manifest_hash
    assert manifest_path.read_bytes() == manifest_bytes_before

    # 同 ID + 不同内容：immutable conflict。
    with pytest.raises(SnapshotImmutableConflictError):
        store.save("mds-test", pd.DataFrame({"symbol": ["000001.SZ"]}), manifest)


def test_first_save_succeeds(tmp_path):
    """Q-02：首次写入正常落盘。"""
    import pandas as pd

    store = LocalParquetSnapshotStore(tmp_path / "snapshots")
    frame = pd.DataFrame({"symbol": ["600519.SH"], "close": [10.0]})
    location = store.save("mds-first", frame, {"schema_version": "market-snapshot.v1"})
    assert store.exists("mds-first")
    assert location.snapshot_id == "mds-first"
    loaded = store.load("mds-first")
    assert loaded["close"].tolist() == [10.0]


def test_same_id_same_content_is_idempotent(tmp_path):
    """Q-02：同 ID + 同内容 → idempotent success。"""
    import pandas as pd

    store = LocalParquetSnapshotStore(tmp_path / "snapshots")
    frame = pd.DataFrame({"symbol": ["600519.SH", "000001.SZ"], "close": [10.0, 20.0]})
    manifest = {"schema_version": "market-snapshot.v1"}
    first = store.save("mds-idem", frame, manifest)
    second = store.save("mds-idem", frame.copy(), dict(manifest))
    assert second.manifest_hash == first.manifest_hash


def test_same_id_different_content_raises_conflict(tmp_path):
    """Q-02：同 ID + 不同内容 → immutable conflict。"""
    import pandas as pd

    store = LocalParquetSnapshotStore(tmp_path / "snapshots")
    manifest = {"schema_version": "market-snapshot.v1"}
    store.save("mds-conflict", pd.DataFrame({"close": [10.0]}), manifest)
    with pytest.raises(SnapshotImmutableConflictError):
        store.save("mds-conflict", pd.DataFrame({"close": [99.0]}), manifest)


def test_conflict_does_not_mutate_existing_snapshot(tmp_path):
    """Q-02：冲突不得改变已有快照的任何字节。"""
    import pandas as pd

    store = LocalParquetSnapshotStore(tmp_path / "snapshots")
    manifest = {"schema_version": "market-snapshot.v1"}
    frame = pd.DataFrame({"symbol": ["600519.SH"], "close": [10.0]})
    store.save("mds-guard", frame, manifest)
    sha_before = _dataset_sha256(store, "mds-guard")
    manifest_before = store.read_manifest("mds-guard")

    with pytest.raises(SnapshotImmutableConflictError):
        store.save("mds-guard", pd.DataFrame({"symbol": ["X"], "close": [1.0]}), manifest)

    assert _dataset_sha256(store, "mds-guard") == sha_before
    assert store.read_manifest("mds-guard") == manifest_before
    assert store.load("mds-guard")["close"].tolist() == [10.0]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
