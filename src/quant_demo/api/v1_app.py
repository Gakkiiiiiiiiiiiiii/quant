"""Quant Contract API v1（设计文档 §35：FastAPI + Pydantic + OpenAPI）。

实现：
- §7   market-data.v1 Bars Batch（兼容旧路径 /v1/bars/batch）
- §9   Market Snapshot 查询
- §8   PIT 市场元数据写入/查询
- §18  异步 Backtest API
- §21  Paper API
- §31  Common Metadata / §32 Trace Headers / §33 Idempotency / §60 错误码
- §106 /health/version

收尾文档 §46：最终部署入口收敛为单一 FastAPI 应用（本模块 ``app``），
旧 Dashboard 端点通过 ``legacy_router`` 挂载到同一应用，不再长期维护
BaseHTTPRequestHandler + FastAPI 两套路由。
"""
from __future__ import annotations

import json
import os
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from quant_demo.application.backtest_service import BacktestService
from quant_demo.application.market_data_service import MARKET_DATA_CONTRACT, MarketDataService
from quant_demo.application.paper_service import PaperService

try:  # FastAPI 为可选依赖（[api] extra）；模块级导入供端点注解解析。
    from fastapi import Request
except ImportError:  # pragma: no cover
    Request = None  # type: ignore[assignment]
from quant_demo.core.error_codes import (
    BACKTEST_FAILED,
    CONTRACT_VERSION_UNSUPPORTED,
    DATA_NOT_READY,
    SNAPSHOT_NOT_FOUND,
)
from quant_demo.core.exceptions import DataNotReadyError
from quant_demo.db.session import create_session_factory
from quant_demo.snapshot.store import LocalParquetSnapshotStore

SERVICE_NAME = "quant"
SERVICE_VERSION = "1.0.0"
SUPPORTED_CONTRACTS = ["market-data.v1", "backtest.v1", "trading.v1"]

ROOT = Path(__file__).resolve().parents[3]


class V1State:
    """单例服务装配。"""

    def __init__(
        self,
        database_url: str | None = None,
        history_path: str | None = None,
        source: str | None = None,
        snapshot_dir: str | None = None,
    ) -> None:
        database_url = (
            database_url
            or os.getenv("QUANT_V1_DATABASE_URL")
            or f"sqlite:///{(ROOT / 'data' / 'quant_v1.db').as_posix()}"
        )
        history_path = history_path or os.getenv("QUANT_HISTORY_PARQUET") or str(ROOT / "data" / "parquet" / "history.parquet")
        source = source or os.getenv("QUANT_MARKET_SOURCE", "qmt")
        snapshot_dir = snapshot_dir or os.getenv("QUANT_SNAPSHOT_DIR") or str(ROOT / "data" / "market_snapshots")
        session_factory = create_session_factory(database_url)
        # 收尾文档 §7：不可变快照物化存储。
        snapshot_store = LocalParquetSnapshotStore(snapshot_dir)
        self.market = MarketDataService(history_path, session_factory, source=source, snapshot_store=snapshot_store)
        self.backtests = BacktestService(session_factory, self.market)
        self.paper = PaperService(session_factory, self.market)


_state: V1State | None = None


def get_state() -> V1State:
    global _state
    if _state is None:
        _state = V1State()
    return _state


def set_state(state: V1State) -> None:
    """测试注入。"""
    global _state
    _state = state


def _git_commit() -> str:
    cached = os.getenv("QUANT_GIT_COMMIT")
    if cached:
        return cached
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=str(ROOT), capture_output=True, text=True, timeout=2, check=False
        )
        return result.stdout.strip() or "unknown"
    except Exception:  # noqa: BLE001
        return "unknown"


def _meta(trace_id: str, contract_version: str, as_of: str | None = None) -> dict[str, Any]:
    return {
        "trace_id": trace_id,
        "contract_version": contract_version,
        "service_version": SERVICE_VERSION,
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "as_of": as_of,
    }


def _error(status: int, code: str, message: str, trace_id: str) -> tuple[int, dict]:
    return status, {"error": {"code": code, "message": message}, "meta": _meta(trace_id, CONTRACT_VERSION_UNSUPPORTED)}


Handler = Callable[..., tuple[int, dict]]


# --------------------------------------------------------------------- handlers
def handle_health(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    return 200, {"status": "ok", "service": SERVICE_NAME}


def handle_health_version(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    return 200, {
        "service": SERVICE_NAME,
        "service_version": SERVICE_VERSION,
        "git_commit": _git_commit(),
        "contract_versions": SUPPORTED_CONTRACTS,
    }


def handle_bars_batch(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    symbols = body.get("symbols") or []
    start, end = body.get("start"), body.get("end")
    if not symbols or not start or not end:
        return _error(422, DATA_NOT_READY, "symbols/start/end are required", trace_id)
    try:
        data = get_state().market.bars_batch(
            symbols=symbols,
            start=start,
            end=end,
            frequency=body.get("frequency", "1d"),
            adjust=body.get("adjust", "qfq"),
        )
    except ValueError as exc:
        return _error(422, DATA_NOT_READY, str(exc), trace_id)
    except DataNotReadyError as exc:
        return _error(503, DATA_NOT_READY, str(exc), trace_id)
    return 200, {
        "contract_version": MARKET_DATA_CONTRACT,
        "meta": _meta(trace_id, MARKET_DATA_CONTRACT, as_of=data["dates"][-1] if data["dates"] else None),
        "data": data,
    }


def handle_snapshot_get(headers: dict, query: dict, body: dict, snapshot_id: str = "") -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    snapshot = get_state().market.get_snapshot(snapshot_id)
    if snapshot is None:
        return _error(404, SNAPSHOT_NOT_FOUND, f"snapshot {snapshot_id} not found", trace_id)
    return 200, {"contract_version": MARKET_DATA_CONTRACT, "meta": _meta(trace_id, MARKET_DATA_CONTRACT), "data": snapshot}


def handle_snapshot_create(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    """收尾文档 §10：显式创建不可变市场快照。"""
    trace_id = _trace_id(headers, body)
    symbols = body.get("symbols") or []
    start, end = body.get("start"), body.get("end")
    if not symbols or not start or not end:
        return _error(422, DATA_NOT_READY, "symbols/start/end are required", trace_id)
    try:
        data = get_state().market.create_snapshot(
            symbols=symbols,
            start=start,
            end=end,
            adjust=body.get("adjust", "qfq"),
            frequency=body.get("frequency", "1d"),
        )
    except ValueError as exc:
        return _error(422, DATA_NOT_READY, str(exc), trace_id)
    except DataNotReadyError as exc:
        return _error(503, DATA_NOT_READY, str(exc), trace_id)
    snapshot = get_state().market.get_snapshot(data["snapshot_id"])
    return 201, {"contract_version": MARKET_DATA_CONTRACT, "meta": _meta(trace_id, MARKET_DATA_CONTRACT), "data": snapshot or data}


def handle_security_master_upsert(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    count = get_state().market.upsert_security_master(body.get("items") or [])
    return 200, {"contract_version": MARKET_DATA_CONTRACT, "meta": _meta(trace_id, MARKET_DATA_CONTRACT), "data": {"upserted": count}}


def handle_securities(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    limit = int((query.get("limit") or ["100"])[0]) if isinstance(query.get("limit"), list) else int(query.get("limit") or 100)
    items = get_state().market.list_securities(limit)
    return 200, {"contract_version": MARKET_DATA_CONTRACT, "meta": _meta(trace_id, MARKET_DATA_CONTRACT), "items": items}


def handle_security_status_upsert(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    count = get_state().market.upsert_security_status(body.get("items") or [])
    return 200, {"contract_version": MARKET_DATA_CONTRACT, "meta": _meta(trace_id, MARKET_DATA_CONTRACT), "data": {"upserted": count}}


def handle_security_status_query(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    params = _flat(query)
    symbol, start, end = params.get("symbol"), params.get("start"), params.get("end")
    if not symbol or not start or not end:
        return _error(422, DATA_NOT_READY, "symbol/start/end are required", trace_id)
    items = get_state().market.get_security_status(symbol, start, end)
    return 200, {"contract_version": MARKET_DATA_CONTRACT, "meta": _meta(trace_id, MARKET_DATA_CONTRACT), "items": items}


def handle_price_limits_upsert(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    count = get_state().market.upsert_price_limits(body.get("items") or [])
    return 200, {"contract_version": MARKET_DATA_CONTRACT, "meta": _meta(trace_id, MARKET_DATA_CONTRACT), "data": {"upserted": count}}


def handle_calendar_upsert(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    count = get_state().market.upsert_calendar(body.get("trading_days") or [])
    return 200, {"contract_version": MARKET_DATA_CONTRACT, "meta": _meta(trace_id, MARKET_DATA_CONTRACT), "data": {"upserted": count}}


def handle_corporate_actions_upsert(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    count = get_state().market.upsert_corporate_actions(body.get("items") or [])
    return 200, {"contract_version": MARKET_DATA_CONTRACT, "meta": _meta(trace_id, MARKET_DATA_CONTRACT), "data": {"upserted": count}}


def handle_corporate_actions_query(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    params = _flat(query)
    symbol, start, end = params.get("symbol"), params.get("start"), params.get("end")
    if not symbol or not start or not end:
        return _error(422, DATA_NOT_READY, "symbol/start/end are required", trace_id)
    items = get_state().market.get_corporate_actions(symbol, start, end)
    return 200, {"contract_version": MARKET_DATA_CONTRACT, "meta": _meta(trace_id, MARKET_DATA_CONTRACT), "items": items}


def handle_industry_memberships_upsert(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    """收尾文档 §13：PIT 行业归属写入。"""
    trace_id = _trace_id(headers, body)
    count = get_state().market.upsert_industry_memberships(body.get("items") or [])
    return 200, {"contract_version": MARKET_DATA_CONTRACT, "meta": _meta(trace_id, MARKET_DATA_CONTRACT), "data": {"upserted": count}}


def handle_industry_memberships_query(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    params = _flat(query)
    symbol, as_of = params.get("symbol"), params.get("as_of")
    if not symbol or not as_of:
        return _error(422, DATA_NOT_READY, "symbol/as_of are required", trace_id)
    items = get_state().market.get_industry_memberships(symbol, as_of)
    return 200, {"contract_version": MARKET_DATA_CONTRACT, "meta": _meta(trace_id, MARKET_DATA_CONTRACT), "items": items}


def handle_index_constituents_upsert(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    """收尾文档 §13/§49：历史指数成分写入（防幸存者偏差）。"""
    trace_id = _trace_id(headers, body)
    count = get_state().market.upsert_index_constituents(body.get("items") or [])
    return 200, {"contract_version": MARKET_DATA_CONTRACT, "meta": _meta(trace_id, MARKET_DATA_CONTRACT), "data": {"upserted": count}}


def handle_index_constituents_query(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    params = _flat(query)
    index_code, as_of = params.get("index_code"), params.get("as_of")
    if not index_code or not as_of:
        return _error(422, DATA_NOT_READY, "index_code/as_of are required", trace_id)
    items = get_state().market.get_index_constituents(index_code, as_of)
    return 200, {"contract_version": MARKET_DATA_CONTRACT, "meta": _meta(trace_id, MARKET_DATA_CONTRACT), "items": items}


def handle_concept_memberships_upsert(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    """收尾文档 §13：PIT 概念板块归属写入。"""
    trace_id = _trace_id(headers, body)
    count = get_state().market.upsert_concept_memberships(body.get("items") or [])
    return 200, {"contract_version": MARKET_DATA_CONTRACT, "meta": _meta(trace_id, MARKET_DATA_CONTRACT), "data": {"upserted": count}}


def handle_concept_memberships_query(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    params = _flat(query)
    symbol, as_of = params.get("symbol"), params.get("as_of")
    if not symbol or not as_of:
        return _error(422, DATA_NOT_READY, "symbol/as_of are required", trace_id)
    items = get_state().market.get_concept_memberships(symbol, as_of)
    return 200, {"contract_version": MARKET_DATA_CONTRACT, "meta": _meta(trace_id, MARKET_DATA_CONTRACT), "items": items}


def handle_backtest_create(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    try:
        result = get_state().backtests.create(body, idempotency_key=headers.get("idempotency-key"))
    except Exception as exc:  # noqa: BLE001
        return _error(422, BACKTEST_FAILED, str(exc), trace_id)
    return 202, {"contract_version": "backtest.v1", "meta": _meta(trace_id, "backtest.v1"), "data": result}


def handle_backtest_get(headers: dict, query: dict, body: dict, backtest_id: str = "") -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    payload = get_state().backtests.get(backtest_id)
    if payload is None:
        return _error(404, SNAPSHOT_NOT_FOUND, f"backtest {backtest_id} not found", trace_id)
    return 200, {"contract_version": "backtest.v1", "meta": _meta(trace_id, "backtest.v1"), "data": payload}


def _backtest_field_handler(field: str) -> Handler:
    def handler(headers: dict, query: dict, body: dict, backtest_id: str = "") -> tuple[int, dict]:
        trace_id = _trace_id(headers, body)
        service = get_state().backtests
        value = getattr(service, field)(backtest_id)
        if value is None:
            return _error(404, SNAPSHOT_NOT_FOUND, f"backtest {backtest_id} result not found", trace_id)
        return 200, {"contract_version": "backtest.v1", "meta": _meta(trace_id, "backtest.v1"), "data": value}

    return handler


def handle_paper_create_account(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    account = get_state().paper.create_account(body.get("name", "paper"), float(body.get("initial_cash", 1_000_000)))
    return 201, {"contract_version": "trading.v1", "meta": _meta(trace_id, "trading.v1"), "data": account}


def handle_paper_create_plan(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    try:
        plan = get_state().paper.create_plan(body["account_id"], body.get("targets") or [], body.get("time_contract"))
    except KeyError as exc:
        return _error(404, SNAPSHOT_NOT_FOUND, f"account not found: {exc}", trace_id)
    return 201, {"contract_version": "trading.v1", "meta": _meta(trace_id, "trading.v1"), "data": plan}


def handle_paper_generate_orders(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    if not body.get("account_id") or not body.get("as_of"):
        return _error(422, DATA_NOT_READY, "account_id/as_of are required", trace_id)
    try:
        result = get_state().paper.generate_orders(
            account_id=body["account_id"],
            as_of=body["as_of"],
            scores=body.get("scores"),
            plan_id=body.get("plan_id"),
            top_k=int(body.get("top_k", 10)),
            idempotency_key=headers.get("idempotency-key"),
        )
    except KeyError as exc:
        return _error(404, SNAPSHOT_NOT_FOUND, f"account not found: {exc}", trace_id)
    return 200, {"contract_version": "trading.v1", "meta": _meta(trace_id, "trading.v1"), "data": result}


def handle_paper_run(headers: dict, query: dict, body: dict) -> tuple[int, dict]:
    trace_id = _trace_id(headers, body)
    if not body.get("account_id") or not body.get("as_of"):
        return _error(422, DATA_NOT_READY, "account_id/as_of are required", trace_id)
    try:
        result = get_state().paper.run(body["account_id"], body["as_of"], body.get("market_prices") or {})
    except KeyError as exc:
        return _error(404, SNAPSHOT_NOT_FOUND, f"account not found: {exc}", trace_id)
    return 200, {"contract_version": "trading.v1", "meta": _meta(trace_id, "trading.v1"), "data": result}


def _paper_account_handler(section: str | None) -> Handler:
    def handler(headers: dict, query: dict, body: dict, account_id: str = "") -> tuple[int, dict]:
        trace_id = _trace_id(headers, body)
        service = get_state().paper
        if section is None:
            data = service.get_account(account_id)
        else:
            data = getattr(service, section)(account_id)
        if data is None:
            return _error(404, SNAPSHOT_NOT_FOUND, f"account {account_id} not found", trace_id)
        return 200, {"contract_version": "trading.v1", "meta": _meta(trace_id, "trading.v1"), "data": data}

    return handler


def _trace_id(headers: dict, body: dict) -> str:
    return str(headers.get("x-trace-id") or (body or {}).get("trace_id") or uuid.uuid4())


def _flat(query: dict) -> dict:
    return {key: (value[0] if isinstance(value, list) else value) for key, value in query.items()}


# ------------------------------------------------------------------- dispatch
_ROUTES: list[tuple[str, str, Handler, bool]] = [
    # (method, path-template, handler, has-path-param)
    ("GET", "/health", handle_health, False),
    ("GET", "/api/health", handle_health, False),
    ("GET", "/health/version", handle_health_version, False),
    ("POST", "/api/v1/market/bars/batch", handle_bars_batch, False),
    ("POST", "/v1/bars/batch", handle_bars_batch, False),  # 迁移期兼容旧路径（§12）
    ("POST", "/api/v1/market/snapshots", handle_snapshot_create, False),
    ("GET", "/api/v1/market/snapshots/{snapshot_id}", handle_snapshot_get, True),
    ("POST", "/api/v1/market/security-master", handle_security_master_upsert, False),
    ("GET", "/api/v1/market/securities", handle_securities, False),
    ("POST", "/api/v1/market/security-status", handle_security_status_upsert, False),
    ("GET", "/api/v1/market/security-status", handle_security_status_query, False),
    ("POST", "/api/v1/market/price-limits", handle_price_limits_upsert, False),
    ("POST", "/api/v1/market/calendar", handle_calendar_upsert, False),
    ("POST", "/api/v1/market/corporate-actions", handle_corporate_actions_upsert, False),
    ("GET", "/api/v1/market/corporate-actions", handle_corporate_actions_query, False),
    ("POST", "/api/v1/market/industry-memberships", handle_industry_memberships_upsert, False),
    ("GET", "/api/v1/market/industry-memberships", handle_industry_memberships_query, False),
    ("POST", "/api/v1/market/index-constituents", handle_index_constituents_upsert, False),
    ("GET", "/api/v1/market/index-constituents", handle_index_constituents_query, False),
    ("POST", "/api/v1/market/concept-memberships", handle_concept_memberships_upsert, False),
    ("GET", "/api/v1/market/concept-memberships", handle_concept_memberships_query, False),
    ("POST", "/api/v1/backtests", handle_backtest_create, False),
    ("GET", "/api/v1/backtests/{backtest_id}", handle_backtest_get, True),
    ("GET", "/api/v1/backtests/{backtest_id}/metrics", _backtest_field_handler("metrics"), True),
    ("GET", "/api/v1/backtests/{backtest_id}/equity", _backtest_field_handler("equity"), True),
    ("GET", "/api/v1/backtests/{backtest_id}/trades", _backtest_field_handler("trades"), True),
    ("GET", "/api/v1/backtests/{backtest_id}/positions", _backtest_field_handler("positions"), True),
    ("GET", "/api/v1/backtests/{backtest_id}/daily-actions", _backtest_field_handler("daily_actions"), True),
    ("GET", "/api/v1/backtests/{backtest_id}/diagnostics", _backtest_field_handler("diagnostics"), True),
    ("POST", "/api/v1/paper/accounts", handle_paper_create_account, False),
    ("POST", "/api/v1/paper/plans", handle_paper_create_plan, False),
    ("POST", "/api/v1/paper/orders/generate", handle_paper_generate_orders, False),
    ("POST", "/api/v1/paper/run", handle_paper_run, False),
    ("GET", "/api/v1/paper/accounts/{account_id}", _paper_account_handler(None), True),
    ("GET", "/api/v1/paper/accounts/{account_id}/positions", _paper_account_handler("positions"), True),
    ("GET", "/api/v1/paper/accounts/{account_id}/orders", _paper_account_handler("orders"), True),
    ("GET", "/api/v1/paper/accounts/{account_id}/trades", _paper_account_handler("trades"), True),
    ("GET", "/api/v1/paper/accounts/{account_id}/equity", _paper_account_handler("equity"), True),
]


def is_v1_path(path: str) -> bool:
    return path.startswith("/api/v1/") or path in {"/v1/bars/batch", "/health/version"}


def dispatch(method: str, path: str, query: dict | None = None, body: dict | None = None, headers: dict | None = None) -> tuple[int, dict]:
    """供 FastAPI 与旧 BaseHTTPRequestHandler 共用的路由分发。"""
    normalized_headers = {str(key).lower(): str(value) for key, value in (headers or {}).items()}
    for route_method, template, handler, has_param in _ROUTES:
        if route_method != method:
            continue
        params = _match(template, path)
        if params is None:
            continue
        try:
            if has_param:
                return handler(normalized_headers, query or {}, body or {}, **params)
            return handler(normalized_headers, query or {}, body or {})
        except Exception as exc:  # noqa: BLE001
            return _error(500, BACKTEST_FAILED, f"internal error: {exc}", _trace_id(normalized_headers, body or {}))
    return _error(404, SNAPSHOT_NOT_FOUND, f"no route for {method} {path}", _trace_id(normalized_headers, body or {}))


def _match(template: str, path: str) -> dict | None:
    template_parts = template.strip("/").split("/")
    path_parts = path.strip("/").split("/")
    if len(template_parts) != len(path_parts):
        return None
    params: dict[str, str] = {}
    for template_part, path_part in zip(template_parts, path_parts):
        if template_part.startswith("{") and template_part.endswith("}"):
            params[template_part[1:-1]] = path_part
        elif template_part != path_part:
            return None
    return params


def create_v1_app():
    """构建 FastAPI 契约应用（§35）。"""
    from fastapi import FastAPI, Response

    app = FastAPI(title="quant-contract-api", version=SERVICE_VERSION)

    @app.middleware("http")
    async def trace_headers(request: Request, call_next):
        trace_id = request.headers.get("x-trace-id") or str(uuid.uuid4())
        response: Response = await call_next(request)
        response.headers["x-trace-id"] = trace_id
        if request.headers.get("x-decision-id"):
            response.headers["x-decision-id"] = request.headers["x-decision-id"]
        response.headers["x-caller-service"] = request.headers.get("x-caller-service", "")
        return response

    async def _handle(request: Request, method: str) -> tuple[int, dict]:
        body: dict = {}
        if method == "POST":
            raw = await request.body()
            if raw:
                try:
                    body = json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError:
                    body = {}
        query = {key: request.query_params.get(key) for key in request.query_params}
        headers = dict(request.headers)
        headers.setdefault("x-trace-id", request.headers.get("x-trace-id") or str(uuid.uuid4()))
        return dispatch(method, request.url.path, query, body, headers)

    def _register(method: str, template: str) -> None:
        async def endpoint(request: Request):
            status, payload = await _handle(request, method)
            return _json_response(payload, status)

        app.add_api_route(template, endpoint, methods=[method], name=f"{method.lower()} {template}")

    for route_method, template, _handler, _param in _ROUTES:
        _register(route_method, template)

    # 收尾文档 §46：旧 Dashboard 继续保留时以 legacy_router 挂载进同一 FastAPI 应用。
    try:
        from quant_demo.api.legacy_dashboard import legacy_router

        app.include_router(legacy_router)
    except ImportError:  # pragma: no cover - dashboard 依赖缺失时仅提供契约 API
        pass

    def _json_response(payload: dict, status: int):
        from fastapi.responses import JSONResponse

        return JSONResponse(content=json.loads(json.dumps(payload, ensure_ascii=False, default=str)), status_code=status)

    return app


# 收尾文档 §46：最终部署入口（uvicorn quant_demo.api.v1_app:app）。
app = create_v1_app()


__all__ = ["V1State", "app", "create_v1_app", "dispatch", "get_state", "is_v1_path", "set_state"]
