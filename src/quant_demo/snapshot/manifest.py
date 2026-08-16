"""Snapshot Manifest（收尾设计文档 §8；详细修改方案 §9 Manifest v2）。

Manifest v2 在 v1 基础上补齐内容寻址身份：

- symbols_hash / schema_hash：标的集与列模式的内容哈希；
- row_count / min_date / max_date：数据规模与时间边界；
- source_provider / source_data_version：源数据身份；
- pit_enforced：PIT 元数据是否强制。
"""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from quant_demo.snapshot.models import SNAPSHOT_SCHEMA_VERSION


def symbols_hash_of(symbols: list[str]) -> str:
    return hashlib.sha256("|".join(sorted(symbols)).encode("utf-8")).hexdigest()


def schema_hash_of(fields: list[str]) -> str:
    return hashlib.sha256("|".join(fields).encode("utf-8")).hexdigest()


def build_manifest(
    *,
    snapshot_id: str,
    data_version: str,
    source: str,
    frequency: str,
    adjustment: str,
    start: str,
    end: str,
    as_of: str | None,
    symbols: list[str],
    fields: list[str],
    dataset_path: str,
    dataset_sha256: str,
    dates: list[str] | None = None,
    row_count: int = 0,
    pit_enforced: bool = False,
    quality_flags: list[str] | None = None,
) -> dict[str, Any]:
    ordered_dates = sorted(dates or [])
    return {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "created_at": datetime.now(UTC).isoformat(),
        "as_of": as_of,
        "data_version": data_version,
        "source": source,
        "source_provider": source,
        "source_data_version": data_version,
        "frequency": frequency,
        "adjustment": adjustment,
        "start": start,
        "end": end,
        "symbols": list(symbols),
        "symbols_hash": symbols_hash_of(symbols),
        "fields": list(fields),
        "schema_hash": schema_hash_of(fields),
        "row_count": int(row_count),
        "min_date": ordered_dates[0] if ordered_dates else None,
        "max_date": ordered_dates[-1] if ordered_dates else None,
        "pit_enforced": pit_enforced,
        "quality_flags": list(quality_flags or []),
        "files": [{"path": dataset_path, "sha256": dataset_sha256}],
    }


def manifest_bytes(manifest: dict[str, Any]) -> bytes:
    return json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
