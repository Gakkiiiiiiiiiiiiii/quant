"""Snapshot Store（收尾设计文档 §7 / §12）。

第一阶段实现 LocalParquetSnapshotStore：

    data/market_snapshots/
    └── mds-xxx/
        ├── bars.parquet
        └── manifest.json

快照一经写入即不可变（§101）：已存在的 snapshot_id 不得覆盖；
读取时校验 parquet 文件 sha256 与 manifest 一致，否则 SNAPSHOT_CORRUPTED。
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from quant_demo.snapshot.manifest import manifest_bytes
from quant_demo.snapshot.models import SnapshotLocation

DATASET_FILENAME = "bars.parquet"
MANIFEST_FILENAME = "manifest.json"


class SnapshotNotFoundError(KeyError):
    """SNAPSHOT_NOT_FOUND：快照不存在。"""


class SnapshotCorruptedError(RuntimeError):
    """SNAPSHOT_CORRUPTED（§12.3）：manifest hash 与 Parquet 不一致，禁止继续使用。"""


class MarketSnapshotStore(Protocol):
    def save(self, snapshot_id: str, frame: pd.DataFrame, manifest: dict) -> SnapshotLocation: ...

    def load(self, snapshot_id: str) -> pd.DataFrame: ...

    def exists(self, snapshot_id: str) -> bool: ...

    def read_manifest(self, snapshot_id: str) -> dict: ...


class LocalParquetSnapshotStore:
    """本地 Parquet 不可变快照存储。"""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    # ------------------------------------------------------------------ API
    def save(self, snapshot_id: str, frame: pd.DataFrame, manifest: dict) -> SnapshotLocation:
        directory = self._directory(snapshot_id)
        if self.exists(snapshot_id):
            # §101：快照不可覆盖；内容寻址保证相同 snapshot_id 数据一致，直接复用。
            return self._location(snapshot_id)
        directory.mkdir(parents=True, exist_ok=True)
        dataset_path = directory / DATASET_FILENAME
        frame.to_parquet(dataset_path, index=False)
        dataset_sha256 = _sha256_file(dataset_path)
        recorded = dict(manifest)
        recorded["files"] = [{"path": DATASET_FILENAME, "sha256": dataset_sha256}]
        payload = manifest_bytes(recorded)
        manifest_path = directory / MANIFEST_FILENAME
        manifest_path.write_bytes(payload)
        return SnapshotLocation(
            snapshot_id=snapshot_id,
            dataset_uri=dataset_path.as_posix(),
            manifest_uri=manifest_path.as_posix(),
            manifest_hash=hashlib.sha256(payload).hexdigest(),
        )

    def load(self, snapshot_id: str) -> pd.DataFrame:
        manifest = self.read_manifest(snapshot_id)
        files = manifest.get("files") or []
        if not files:
            raise SnapshotCorruptedError(f"{snapshot_id}: manifest 缺少 files")
        dataset_path = self._directory(snapshot_id) / files[0]["path"]
        if not dataset_path.exists():
            raise SnapshotNotFoundError(snapshot_id)
        expected = files[0].get("sha256")
        if expected and _sha256_file(dataset_path) != expected:
            raise SnapshotCorruptedError(f"{snapshot_id}: parquet 与 manifest sha256 不一致")
        return pd.read_parquet(dataset_path)

    def exists(self, snapshot_id: str) -> bool:
        return (self._directory(snapshot_id) / MANIFEST_FILENAME).exists()

    def read_manifest(self, snapshot_id: str) -> dict[str, Any]:
        manifest_path = self._directory(snapshot_id) / MANIFEST_FILENAME
        if not manifest_path.exists():
            raise SnapshotNotFoundError(snapshot_id)
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    # ------------------------------------------------------------- internal
    def _directory(self, snapshot_id: str) -> Path:
        safe = "".join(ch for ch in snapshot_id if ch.isalnum() or ch in "-_")
        if not safe:
            raise ValueError(f"invalid snapshot_id: {snapshot_id}")
        return self._root / safe

    def _location(self, snapshot_id: str) -> SnapshotLocation:
        directory = self._directory(snapshot_id)
        payload = (directory / MANIFEST_FILENAME).read_bytes()
        return SnapshotLocation(
            snapshot_id=snapshot_id,
            dataset_uri=(directory / DATASET_FILENAME).as_posix(),
            manifest_uri=(directory / MANIFEST_FILENAME).as_posix(),
            manifest_hash=hashlib.sha256(payload).hexdigest(),
        )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "MarketSnapshotStore",
    "LocalParquetSnapshotStore",
    "SnapshotNotFoundError",
    "SnapshotCorruptedError",
    "DATASET_FILENAME",
    "MANIFEST_FILENAME",
]
