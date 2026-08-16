"""旧 Dashboard 路由的 FastAPI 收敛（收尾文档 §46）。

§46：最终部署入口统一为 FastAPI（quant_demo.api.v1_app:app），
旧 BaseHTTPRequestHandler 双栈不再长期维护；原 Dashboard 端点
以 legacy_router 形式挂载进同一个 FastAPI 应用。
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from urllib.parse import unquote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

from quant_demo.api.dashboard_payloads import (
    ROOT,
    build_b1_score_card,
    build_dashboard_payload,
    build_qmt_trade_board,
    connection_state,
    delete_backtest_result,
    load_dashboard_data,
    load_live_probe,
    load_runtime_logs,
    overview,
    resolve_settings,
    run_pattern_action,
    run_qlib_action,
    run_strategy_action,
)
from quant_demo.core.enums import Environment

legacy_router = APIRouter(tags=["legacy-dashboard"])


def _frontend_dist() -> Path | None:
    raw = os.getenv("QUANT_FRONTEND_DIST") or getattr(legacy_router, "_frontend_dist", None)
    if not raw:
        return None
    dist = Path(raw).resolve()
    return dist if dist.exists() else None


def _resolve_workspace_file(raw_path: str) -> Path | None:
    candidate = Path(unquote(raw_path))
    target = candidate.resolve() if candidate.is_absolute() else (ROOT / candidate).resolve()
    root_resolved = ROOT.resolve()
    if target != root_resolved and root_resolved not in target.parents:
        return None
    return target


def _sse_event(event: str, payload: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False, default=str)}\n\n"


def _qmt_trade_board_stream(profile: str, config_path: str | None):
    """SSE 推流：与原 BaseHTTPRequestHandler 行为保持一致（收尾文档 §46 收敛）。"""
    yield "retry: 2000\n\n"
    last_signature = ""
    while True:
        try:
            profile_key, resolved_config, settings = resolve_settings(profile, config_path)
            if settings.environment == Environment.BACKTEST:
                yield _sse_event(
                    "qmt-trade-board",
                    {
                        "profile": profile_key,
                        "generated_at": None,
                        "overview": {},
                        "connection": {
                            "label": "离线数据库视图",
                            "color": "#64748b",
                            "detail": "当前未连接 QMT，只展示历史数据。",
                        },
                        "qmt_trade_board": {
                            "available": False,
                            "mode": settings.environment.value,
                            "message": "当前是回测模式，QMT 交易看板未启用。",
                        },
                    },
                )
                return
            data = load_dashboard_data(settings.database_url, settings.report_dir)
            probe = load_live_probe(resolved_config, settings)
            info = overview(data)
            board = build_qmt_trade_board(settings, data, probe, info)
            realtime_asset = board.get("realtime_asset") or {}
            total_asset = realtime_asset.get("total_asset") if realtime_asset.get("total_asset") is not None else info.get("total_asset")
            cash = realtime_asset.get("cash") if realtime_asset.get("cash") is not None else info.get("cash")
            market_value = realtime_asset.get("market_value") if realtime_asset.get("market_value") is not None else info.get("market_value")
            stream_overview = dict(info)
            stream_overview.update(
                {
                    "total_asset": total_asset,
                    "cash": cash,
                    "market_value": market_value,
                    "exposure": (market_value / total_asset) if total_asset else None,
                }
            )
            payload = {
                "profile": profile_key,
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "overview": stream_overview,
                "connection": connection_state(settings, probe, data),
                "qmt_trade_board": board,
            }
            signature = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
            if signature != last_signature:
                yield _sse_event("qmt-trade-board", payload)
                last_signature = signature
            else:
                yield ": keepalive\n\n"
            time.sleep(2)
        except (BrokenPipeError, ConnectionResetError):
            return
        except Exception as exc:  # noqa: BLE001 - SSE 推流内错误必须以事件形式下发
            try:
                yield _sse_event(
                    "qmt-trade-board-error",
                    {"error": str(exc), "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S")},
                )
                time.sleep(2)
            except (BrokenPipeError, ConnectionResetError):
                return


@legacy_router.get("/api/health")
def api_health() -> dict:
    return {"status": "ok"}


@legacy_router.get("/api/bootstrap")
@legacy_router.get("/api/dashboard")
def api_dashboard(
    profile: str = Query("backtest"),
    config: str = Query(""),
    pattern_report_dir: str = Query(""),
    backtest_result_id: str = Query(""),
) -> dict:
    return build_dashboard_payload(
        profile=profile,
        config_path=config.strip() or None,
        pattern_report_dir=pattern_report_dir or None,
        backtest_result_id=backtest_result_id or None,
    )


@legacy_router.get("/api/qmt/stream")
def api_qmt_stream(profile: str = Query("paper"), config: str = Query("")) -> StreamingResponse:
    return StreamingResponse(
        _qmt_trade_board_stream(profile, config.strip() or None),
        media_type="text/event-stream; charset=utf-8",
        headers={"Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*"},
    )


@legacy_router.get("/api/logs")
def api_logs() -> dict:
    return {"logs": load_runtime_logs()}


@legacy_router.get("/api/file")
def api_file(path: str = Query("")) -> FileResponse:
    target = _resolve_workspace_file(path)
    if target is None:
        raise HTTPException(status_code=403, detail="forbidden")
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="not_found")
    return FileResponse(target)


@legacy_router.get("/api/pattern/b1-score")
def api_pattern_b1_score(symbol: str = Query(""), date: str = Query("")) -> dict:
    symbol = symbol.strip()
    target_date = date.strip()
    if not symbol or not target_date:
        raise HTTPException(status_code=400, detail="symbol_and_date_required")
    return {"result": build_b1_score_card(symbol, target_date)}


@legacy_router.post("/api/actions/strategy")
async def action_strategy(request: Request) -> dict:
    try:
        return {"result": run_strategy_action(await _json_body(request))}
    except Exception as exc:  # noqa: BLE001 - legacy action 错误沿用旧 JSON 结构
        return JSONResponse(content={"error": str(exc)}, status_code=500)


@legacy_router.post("/api/actions/qlib")
async def action_qlib(request: Request) -> dict:
    try:
        return {"result": run_qlib_action(await _json_body(request))}
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(content={"error": str(exc)}, status_code=500)


@legacy_router.post("/api/actions/pattern")
async def action_pattern(request: Request) -> dict:
    try:
        return {"result": run_pattern_action(await _json_body(request))}
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(content={"error": str(exc)}, status_code=500)


@legacy_router.post("/api/actions/pattern/delete")
async def action_pattern_delete(request: Request) -> dict:
    try:
        payload = await _json_body(request)
        _, _, settings = resolve_settings("backtest")
        result = delete_backtest_result(settings.database_url, str(payload.get("backtest_result_id", "")))
        return {"result": {"deleted": result}}
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(content={"error": str(exc)}, status_code=500)


async def _json_body(request: Request) -> dict:
    raw = await request.body()
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        return {}


@legacy_router.get("/{full_path:path}", include_in_schema=False)
def legacy_static(full_path: str) -> FileResponse:
    """前端静态资源兜底（原 _serve_static 行为，§46 收敛进 FastAPI）。"""
    if full_path.startswith("api/"):
        # 未匹配的 API 路径保持 JSON 404，不被静态兜底吞掉
        raise HTTPException(status_code=404, detail="not_found")
    dist = _frontend_dist()
    if dist is None:
        raise HTTPException(status_code=404, detail="frontend_not_built")
    relative = full_path.lstrip("/")
    target = (dist / relative).resolve() if relative else (dist / "index.html").resolve()
    if target != dist and dist not in target.parents:
        raise HTTPException(status_code=403, detail="forbidden")
    if not relative or not target.exists() or target.is_dir() or "." not in Path(relative).name:
        target = dist / "index.html"
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="not_found")
    return FileResponse(target)


__all__ = ["legacy_router"]
