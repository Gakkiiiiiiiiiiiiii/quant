"""Snapshot Manifest 校验测试（详细修改方案 §3.2 / §9 Manifest v2）。"""
from __future__ import annotations

from quant_demo.snapshot.manifest import schema_hash_of, symbols_hash_of
from quant_demo.snapshot.models import SNAPSHOT_SCHEMA_VERSION

from v1_fixtures import SAMPLE_SYMBOLS, build_state


def test_manifest_contains_required_identity_fields(tmp_path):
    state = build_state(tmp_path, days=60)
    created = state.market.create_snapshot(SAMPLE_SYMBOLS[:2], "1990-01-01", "2099-12-31")
    manifest = state.market._snapshot_store.read_manifest(created["snapshot_id"])  # noqa: SLF001

    # §3.2：schema_version / row_count / column schema / min-max date / symbols hash /
    # parquet sha256 / source data version / adjustment 必须齐备。
    assert manifest["schema_version"] == SNAPSHOT_SCHEMA_VERSION
    assert manifest["snapshot_id"] == created["snapshot_id"]
    assert manifest["row_count"] > 0
    assert manifest["fields"]
    assert manifest["schema_hash"] == schema_hash_of(manifest["fields"])
    assert manifest["min_date"] and manifest["max_date"]
    assert manifest["min_date"] <= manifest["max_date"]
    assert manifest["symbols"] == SAMPLE_SYMBOLS[:2]
    assert manifest["symbols_hash"] == symbols_hash_of(SAMPLE_SYMBOLS[:2])
    assert manifest["source"] and manifest["source_provider"] == manifest["source"]
    assert manifest["source_data_version"] == created["data_version"]
    assert manifest["data_version"] == created["data_version"]
    assert manifest["adjustment"] == "qfq"
    assert manifest["as_of"] == manifest["max_date"]
    assert manifest["pit_enforced"] is False
    # P0 Q-03：frequency / quality_flags 也必须进入 manifest。
    assert manifest["frequency"]
    assert isinstance(manifest["quality_flags"], list)

    files = manifest["files"]
    assert files and files[0]["path"] == "bars.parquet"
    assert files[0]["sha256"], "parquet sha256 必须写入 manifest"


def test_symbols_hash_is_order_insensitive():
    """P0 Q-03：symbols_hash 确定性（顺序无关）。"""
    assert symbols_hash_of(["B", "A"]) == symbols_hash_of(["A", "B"])
    assert symbols_hash_of(["A"]) != symbols_hash_of(["B"])


def test_manifest_row_count_matches_dataset(tmp_path):
    state = build_state(tmp_path, days=60)
    created = state.market.create_snapshot(SAMPLE_SYMBOLS[:2], "1990-01-01", "2099-12-31")
    store = state.market._snapshot_store  # noqa: SLF001
    manifest = store.read_manifest(created["snapshot_id"])
    frame = store.load(created["snapshot_id"])
    assert manifest["row_count"] == len(frame)
    # min/max date 与数据一致
    dates = sorted({day.isoformat() for day in frame["trading_date"]})
    assert manifest["min_date"] == dates[0]
    assert manifest["max_date"] == dates[-1]


def test_manifest_registered_metadata_matches(tmp_path):
    """登记的 MarketSnapshotRow 与 manifest 内容寻址一致。"""
    state = build_state(tmp_path, days=60)
    created = state.market.create_snapshot(SAMPLE_SYMBOLS[:1], "1990-01-01", "2099-12-31")
    registered = state.market.get_snapshot(created["snapshot_id"])
    manifest = state.market._snapshot_store.read_manifest(created["snapshot_id"])  # noqa: SLF001
    assert registered["manifest_hash"]
    assert registered["data_version"] == manifest["data_version"]
    assert registered["row_count"] == manifest["row_count"]
