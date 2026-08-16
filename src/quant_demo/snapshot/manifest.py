"""Snapshot Manifest（收尾设计文档 §8）。"""
from __future__ import annotations

import json
from typing import Any

from quant_demo.snapshot.models import SNAPSHOT_SCHEMA_VERSION


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
) -> dict[str, Any]:
    return {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "data_version": data_version,
        "source": source,
        "frequency": frequency,
        "adjustment": adjustment,
        "start": start,
        "end": end,
        "as_of": as_of,
        "symbols": list(symbols),
        "fields": list(fields),
        "files": [{"path": dataset_path, "sha256": dataset_sha256}],
    }


def manifest_bytes(manifest: dict[str, Any]) -> bytes:
    return json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
