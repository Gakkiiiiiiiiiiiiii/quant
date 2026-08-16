"""不可变市场快照（收尾设计文档 §6-§10）。

Bars → Canonical Serialization → SHA256 → Immutable Dataset → Manifest → Snapshot Metadata。
"""
from quant_demo.snapshot.models import SNAPSHOT_SCHEMA_VERSION, SnapshotLocation
from quant_demo.snapshot.store import (
    LocalParquetSnapshotStore,
    MarketSnapshotStore,
    SnapshotCorruptedError,
    SnapshotNotFoundError,
)

__all__ = [
    "SNAPSHOT_SCHEMA_VERSION",
    "SnapshotLocation",
    "MarketSnapshotStore",
    "LocalParquetSnapshotStore",
    "SnapshotNotFoundError",
    "SnapshotCorruptedError",
]
