from __future__ import annotations

import json
import mimetypes
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from quant_demo.api.dashboard_payloads import (
    ROOT,
    build_b1_score_card,
    build_dashboard_payload,
    delete_backtest_result,
    load_runtime_logs,
    resolve_settings,
    run_pattern_action,
    run_qlib_action,
    run_strategy_action,
)


class DemoApiHandler(BaseHTTPRequestHandler):
    config_path = str(ROOT / "configs" / "app.yaml")
    frontend_dist: str | None = None

    def _json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, file_path: Path, cache_seconds: int = 0) -> None:
        mime_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
        body = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", mime_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", f"public, max-age={cache_seconds}")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _resolve_workspace_file(self, raw_path: str) -> Path | None:
        candidate = Path(unquote(raw_path))
        target = candidate.resolve() if candidate.is_absolute() else (ROOT / candidate).resolve()
        root_resolved = ROOT.resolve()
        if target != root_resolved and root_resolved not in target.parents:
            return None
        return target

    def _serve_static(self, request_path: str) -> None:
        dist = Path(self.frontend_dist).resolve() if self.frontend_dist else None
        if dist is None or not dist.exists():
            self._json({"error": "frontend_not_built"}, status=404)
            return
        relative = request_path.lstrip("/")
        target = (dist / relative).resolve() if relative else (dist / "index.html").resolve()
        if target != dist and dist not in target.parents:
            self._json({"error": "forbidden"}, status=403)
            return
        if request_path == "/" or not relative or not target.exists() or target.is_dir() or "." not in Path(relative).name:
            target = dist / "index.html"
        if not target.exists() or not target.is_file():
            self._json({"error": "not_found"}, status=404)
            return
        cache_seconds = 86400 if target.suffix in {".js", ".css", ".png", ".svg", ".woff2"} else 0
        self._send_file(target, cache_seconds=cache_seconds)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        if parsed.path in {"/health", "/api/health"}:
            self._json({"status": "ok"})
            return
        if parsed.path in {"/api/bootstrap", "/api/dashboard"}:
            profile = query.get("profile", ["backtest"])[0]
            config_path = query.get("config", [self.config_path])[0]
            self._json(build_dashboard_payload(profile=profile, config_path=config_path))
            return
        if parsed.path == "/api/logs":
            self._json({"logs": load_runtime_logs()})
            return
        if parsed.path == "/api/file":
            raw_path = query.get("path", [""])[0]
            target = self._resolve_workspace_file(raw_path)
            if target is None:
                self._json({"error": "forbidden"}, status=403)
                return
            if not target.exists() or not target.is_file():
                self._json({"error": "not_found"}, status=404)
                return
            self._send_file(target)
            return
        if parsed.path == "/api/pattern/b1-score":
            symbol = query.get("symbol", [""])[0].strip()
            target_date = query.get("date", [""])[0].strip()
            if not symbol or not target_date:
                self._json({"error": "symbol_and_date_required"}, status=400)
                return
            self._json({"result": build_b1_score_card(symbol, target_date)})
            return
        self._serve_static(parsed.path)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        payload = self._read_json_body()
        try:
            if parsed.path == "/api/actions/strategy":
                self._json({"result": run_strategy_action(payload)})
                return
            if parsed.path == "/api/actions/qlib":
                self._json({"result": run_qlib_action(payload)})
                return
            if parsed.path == "/api/actions/pattern":
                self._json({"result": run_pattern_action(payload)})
                return
            if parsed.path == "/api/actions/pattern/delete":
                _, _, settings = resolve_settings("backtest")
                result = delete_backtest_result(settings.database_url, str(payload.get("backtest_result_id", "")))
                self._json({"result": {"deleted": result}})
                return
        except Exception as exc:
            self._json({"error": str(exc)}, status=500)
            return
        self._json({"error": "not_found"}, status=404)


def serve(host: str = "127.0.0.1", port: int = 8011, config_path: str | None = None, frontend_dist: str | None = None) -> None:
    if config_path:
        DemoApiHandler.config_path = config_path
    DemoApiHandler.frontend_dist = frontend_dist
    server = ThreadingHTTPServer((host, port), DemoApiHandler)
    server.serve_forever()
