"""market-data.v1 契约测试（设计文档 §7 / §12 / §31 / §106）。"""
from __future__ import annotations

import json

from quant_demo.api.v1_app import dispatch

from v1_fixtures import SAMPLE_SYMBOLS, build_state


def test_bars_batch_contract_fields(tmp_path):
    build_state(tmp_path)
    status, payload = dispatch(
        "POST",
        "/api/v1/market/bars/batch",
        {},
        {"symbols": SAMPLE_SYMBOLS[:2], "start": "2020-01-01", "end": "2099-12-31", "frequency": "1d", "adjust": "qfq"},
        {"X-Trace-Id": "trace-market-1"},
    )
    assert status == 200
    assert payload["contract_version"] == "market-data.v1"
    meta = payload["meta"]
    assert meta["trace_id"] == "trace-market-1"
    assert meta["contract_version"] == "market-data.v1"
    assert meta["service_version"]
    assert meta["generated_at"]
    data = payload["data"]
    for field in ("symbols", "dates", "bars", "data_version", "data_snapshot_id", "source"):
        assert field in data
    for bar_field in ("open", "high", "low", "close", "volume", "amount", "turnover"):
        assert bar_field in data["bars"]
        assert len(data["bars"][bar_field]) == 2
        assert len(data["bars"][bar_field][0]) == len(data["dates"])
    assert data["data_snapshot_id"] == f"mds-{data['data_version']}"


def test_bars_batch_is_content_addressed_and_replayable(tmp_path):
    build_state(tmp_path)
    request = {"symbols": SAMPLE_SYMBOLS, "start": "2020-01-01", "end": "2099-12-31", "adjust": "qfq"}
    _, first = dispatch("POST", "/api/v1/market/bars/batch", {}, request, {})
    _, second = dispatch("POST", "/api/v1/market/bars/batch", {}, request, {})
    assert first["data"]["data_version"] == second["data"]["data_version"]
    assert json.dumps(first["data"], sort_keys=True) == json.dumps(second["data"], sort_keys=True)


def test_legacy_path_compatibility(tmp_path):
    """迁移期间必须兼容旧路径 /v1/bars/batch（§12）。"""
    build_state(tmp_path)
    status, payload = dispatch(
        "POST", "/v1/bars/batch", {},
        {"symbols": [SAMPLE_SYMBOLS[0]], "start": "2020-01-01", "end": "2099-12-31", "adjust": "qfq"}, {},
    )
    assert status == 200
    assert payload["contract_version"] == "market-data.v1"


def test_snapshot_registered_and_queryable(tmp_path):
    build_state(tmp_path)
    _, payload = dispatch(
        "POST", "/api/v1/market/bars/batch", {},
        {"symbols": SAMPLE_SYMBOLS, "start": "2020-01-01", "end": "2099-12-31", "adjust": "qfq"}, {},
    )
    snapshot_id = payload["data"]["data_snapshot_id"]
    status, snapshot_payload = dispatch("GET", f"/api/v1/market/snapshots/{snapshot_id}", {}, {}, {})
    assert status == 200
    snapshot = snapshot_payload["data"]
    assert snapshot["snapshot_id"] == snapshot_id
    assert snapshot["source"] == "qmt"
    assert snapshot["payload_summary"]["symbols"] == SAMPLE_SYMBOLS
    # 快照不可变：重复请求不得生成新 snapshot_id（§101）
    _, again = dispatch(
        "POST", "/api/v1/market/bars/batch", {},
        {"symbols": SAMPLE_SYMBOLS, "start": "2020-01-01", "end": "2099-12-31", "adjust": "qfq"}, {},
    )
    assert again["data"]["data_snapshot_id"] == snapshot_id


def test_snapshot_not_found(tmp_path):
    build_state(tmp_path)
    status, payload = dispatch("GET", "/api/v1/market/snapshots/mds-missing", {}, {}, {})
    assert status == 404
    assert payload["error"]["code"] == "SNAPSHOT_NOT_FOUND"


def test_pit_metadata_upsert_and_query(tmp_path):
    build_state(tmp_path)
    status, _ = dispatch(
        "POST", "/api/v1/market/security-master", {},
        {"items": [{"symbol": "600519.SH", "exchange": "SH", "name": "贵州茅台", "lot_size": 100}]}, {},
    )
    assert status == 200
    status, _ = dispatch(
        "POST", "/api/v1/market/security-status", {},
        {"items": [{"trading_date": "2026-08-14", "symbol": "600519.SH", "is_st": False, "is_suspended": True, "available_at": "2026-08-14T18:00:00", "source": "qmt"}]}, {},
    )
    assert status == 200
    status, payload = dispatch(
        "GET", "/api/v1/market/security-status",
        {"symbol": "600519.SH", "start": "2026-08-01", "end": "2026-08-31"}, {}, {},
    )
    assert status == 200
    items = payload["items"]
    assert len(items) == 1
    assert items[0]["is_suspended"] is True
    assert items[0]["available_at"].startswith("2026-08-14")
    status, _ = dispatch("POST", "/api/v1/market/price-limits", {},
        {"items": [{"trading_date": "2026-08-14", "symbol": "600519.SH", "limit_rate": 0.1, "upper_limit_price": 1500.0, "lower_limit_price": 1230.0, "rule_version": "limit-rule-v1"}]}, {})
    assert status == 200
    status, _ = dispatch("POST", "/api/v1/market/calendar", {}, {"trading_days": ["2026-08-14", "2026-08-17"]}, {})
    assert status == 200


def test_health_version_endpoint(tmp_path):
    build_state(tmp_path)
    status, payload = dispatch("GET", "/health/version", {}, {}, {})
    assert status == 200
    assert payload["service"] == "quant"
    assert "market-data.v1" in payload["contract_versions"]
    assert payload["git_commit"]


def test_fastapi_app_exposes_openapi(tmp_path):
    build_state(tmp_path)
    from quant_demo.api.v1_app import create_v1_app

    app = create_v1_app()
    paths = {route.path for route in app.routes}
    assert "/api/v1/market/bars/batch" in paths
    assert "/api/v1/backtests" in paths
    assert "/api/v1/paper/accounts" in paths
    assert "/health/version" in paths
