from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse

from sqlalchemy import create_engine, text


class DemoApiHandler(BaseHTTPRequestHandler):
    database_url = "sqlite:///data/quant_demo.db"

    def _json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._json({"status": "ok"})
            return
        engine = create_engine(self.database_url, future=True)
        with engine.connect() as connection:
            if parsed.path == "/summary":
                order_count = connection.execute(text("select count(*) from orders")).scalar_one()
                trade_count = connection.execute(text("select count(*) from trades")).scalar_one()
                asset = connection.execute(text("select total_asset, cash from asset_snapshots order by snapshot_time desc limit 1")).fetchone()
                self._json(
                    {
                        "order_count": order_count,
                        "trade_count": trade_count,
                        "latest_total_asset": float(asset.total_asset) if asset else None,
                        "latest_cash": float(asset.cash) if asset else None,
                    }
                )
                return
            if parsed.path == "/orders":
                rows = connection.execute(text("select order_id, symbol, side, qty, status, filled_qty from orders order by created_at desc"))
                self._json({"items": [dict(row._mapping) for row in rows]})
                return
            if parsed.path == "/trades":
                rows = connection.execute(text("select trade_id, symbol, side, fill_qty, fill_price, trade_time from trades order by trade_time desc"))
                self._json({"items": [dict(row._mapping) for row in rows]})
                return
        self._json({"error": "not_found"}, status=404)


def serve(host: str = "127.0.0.1", port: int = 8011, database_url: str = "sqlite:///data/quant_demo.db") -> None:
    DemoApiHandler.database_url = database_url
    server = HTTPServer((host, port), DemoApiHandler)
    server.serve_forever()
