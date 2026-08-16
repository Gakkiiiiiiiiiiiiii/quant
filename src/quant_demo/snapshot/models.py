"""快照域模型（收尾设计文档 §7）。"""
from __future__ import annotations

from dataclasses import dataclass

SNAPSHOT_SCHEMA_VERSION = "market-snapshot.v1"


@dataclass(frozen=True)
class SnapshotLocation:
    """一次快照物化的落位引用。"""

    snapshot_id: str
    dataset_uri: str
    manifest_uri: str
    manifest_hash: str
