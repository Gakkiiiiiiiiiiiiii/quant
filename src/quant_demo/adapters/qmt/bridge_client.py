from __future__ import annotations

import json
import os
import subprocess
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

from quant_demo.core.config import AppSettings
from quant_demo.core.exceptions import QmtUnavailableError

PROJECT_ROOT = Path(__file__).resolve().parents[4]
BRIDGE_TIMEOUT_SECONDS = 30
INDUSTRY_MAP_TIMEOUT_SECONDS = 300


class QmtBridgeClient:
    """通过独立 Python 3.6 进程访问 xtquant。"""

    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.python_path = self._resolve_path(settings.qmt_bridge_python)
        self.script_path = self._resolve_path(settings.qmt_bridge_script)
        self.install_dir = self._resolve_path(settings.qmt_install_dir)
        self.userdata_dir = self._resolve_path(settings.qmt_userdata_dir)

    def healthcheck(self) -> dict[str, Any]:
        return self._run("health")

    def get_quotes(self, symbols: list[str]) -> dict[str, Any]:
        if not symbols:
            return {}
        payload = self._run("quote", "--symbols", ",".join(symbols))
        return payload.get("quotes", {})

    def get_latest_prices(self, symbols: list[str]) -> dict[str, Decimal]:
        prices: dict[str, Decimal] = {}
        for symbol, payload in self.get_quotes(symbols).items():
            last_price = payload.get("last_price")
            if last_price in (None, ""):
                continue
            prices[symbol] = Decimal(str(last_price))
        return prices

    def get_instrument_details(self, symbols: list[str]) -> dict[str, Any]:
        if not symbols:
            return {}
        payload = self._run("instrument-detail", "--symbols", ",".join(symbols))
        return payload.get("details", {})

    def get_sector_symbols(self, sector_name: str, only_a_share: bool = True, limit: int = 0) -> list[str]:
        if not sector_name:
            return []
        payload = self._run(
            "sector-members",
            "--sector-name",
            sector_name,
            "--only-a-share",
            str(only_a_share).lower(),
            "--limit",
            str(limit),
        )
        return payload.get("symbols", [])

    def get_industry_map(self, symbols: list[str], sector_prefix: str = "GICS2", only_a_share: bool = True) -> list[dict[str, Any]]:
        payload = self._run(
            "industry-map",
            "--symbols",
            ",".join(symbols),
            "--sector-prefix",
            str(sector_prefix or "GICS2"),
            "--only-a-share",
            str(only_a_share).lower(),
            timeout_seconds=INDUSTRY_MAP_TIMEOUT_SECONDS,
        )
        return payload.get("rows", []) or []

    def get_history(
        self,
        symbols: list[str],
        period: str,
        start_time: str,
        end_time: str,
        dividend_type: str,
        fill_data: bool,
    ) -> pd.DataFrame:
        if not symbols:
            return pd.DataFrame(columns=["trading_date", "symbol", "open", "high", "low", "close", "volume", "amount"])
        payload = self._run(
            "history",
            "--symbols",
            ",".join(symbols),
            "--period",
            period,
            "--start-time",
            start_time,
            "--end-time",
            end_time,
            "--dividend-type",
            dividend_type,
            "--fill-data",
            str(fill_data).lower(),
        )
        frame = pd.DataFrame(payload.get("rows", []))
        if frame.empty:
            raise QmtUnavailableError(
                f"QMT 未返回历史 K 线数据: symbols={','.join(symbols)} period={period} adjustment={dividend_type}"
            )
        frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.date
        return frame.sort_values(["trading_date", "symbol"]).reset_index(drop=True)

    def get_financial_data(
        self,
        symbols: list[str],
        tables: list[str],
        start_time: str,
        end_time: str,
        report_type: str = "announce_time",
    ) -> dict[str, Any]:
        if not symbols or not tables:
            return {}
        payload = self._run(
            "financial-data",
            "--symbols",
            ",".join(symbols),
            "--tables",
            ",".join(tables),
            "--start-time",
            start_time,
            "--end-time",
            end_time,
            "--report-type",
            report_type,
        )
        return payload.get("data", {})

    def get_account_snapshot(self) -> dict[str, Any]:
        return self._run("account")

    def get_order_status(self, order_id: str | int) -> dict[str, Any]:
        payload = self._run("order-status", "--order-id", str(order_id))
        return payload.get("order", {}) or {}

    def cancel_order(self, order_id: str | int) -> dict[str, Any]:
        return self._run("cancel-order", "--order-id", str(order_id))

    def submit_order(
        self,
        symbol: str,
        side: str,
        qty: int,
        price: Decimal,
        *,
        strategy_name: str = "quant-demo-live",
        order_remark: str = "quant-demo",
    ) -> dict[str, Any]:
        return self._run(
            "order",
            "--symbol",
            symbol,
            "--side",
            side,
            "--qty",
            str(qty),
            "--price",
            str(price),
            "--strategy-name",
            strategy_name,
            "--order-remark",
            order_remark,
        )

    def _run(self, command: str, *extra_args: str, timeout_seconds: int | None = None) -> dict[str, Any]:
        self._ensure_runtime_paths()
        cmd = [
            str(self.python_path),
            str(self.script_path),
            command,
            "--install-dir",
            str(self.install_dir),
            "--userdata-dir",
            str(self.userdata_dir),
        ]
        if self.settings.qmt_account_id:
            cmd.extend(["--account-id", self.settings.qmt_account_id])
        cmd.extend(extra_args)
        env = dict(os.environ)
        env["PYTHONIOENCODING"] = "utf-8"
        try:
            completed = subprocess.run(
                cmd,
                cwd=PROJECT_ROOT,
                capture_output=True,
                check=False,
                env=env,
                timeout=timeout_seconds or BRIDGE_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            timeout_text = timeout_seconds or BRIDGE_TIMEOUT_SECONDS
            raise QmtUnavailableError(f"QMT 桥接调用超时（>{timeout_text}s）: command={command}") from exc
        stdout = self._decode_output(completed.stdout)
        stderr = self._decode_output(completed.stderr)
        if completed.returncode != 0:
            detail = stderr or stdout or "桥接进程没有返回错误详情"
            raise QmtUnavailableError(f"QMT 桥接执行失败: {detail}")
        json_start = stdout.find('{"ok"')
        if json_start > 0:
            stdout = stdout[json_start:]
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise QmtUnavailableError(f"QMT 桥接返回了不可解析内容: {stdout}") from exc
        if not payload.get("ok"):
            raise QmtUnavailableError(payload.get("error", "QMT 桥接调用失败"))
        return payload.get("data", {})

    def _ensure_runtime_paths(self) -> None:
        if not self.python_path.exists():
            raise QmtUnavailableError(f"QMT 桥接 Python 不存在: {self.python_path}")
        if not self.script_path.exists():
            raise QmtUnavailableError(f"QMT 桥接脚本不存在: {self.script_path}")
        if not self.install_dir.exists():
            raise QmtUnavailableError(f"QMT 安装目录不存在: {self.install_dir}")
        if not self.userdata_dir.exists():
            raise QmtUnavailableError(f"QMT 用户数据目录不存在: {self.userdata_dir}")

    @staticmethod
    def _resolve_path(raw_path: str) -> Path:
        path = Path(raw_path)
        if path.is_absolute():
            return path
        return (PROJECT_ROOT / path).resolve()

    @staticmethod
    def _decode_output(payload: bytes | str) -> str:
        if isinstance(payload, str):
            return payload.strip()
        for encoding in ("utf-8", "gb18030"):
            try:
                return payload.decode(encoding).strip()
            except UnicodeDecodeError:
                continue
        return payload.decode("utf-8", errors="ignore").strip()
