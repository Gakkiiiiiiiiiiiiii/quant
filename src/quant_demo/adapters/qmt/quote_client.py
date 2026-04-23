from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from quant_demo.adapters.qmt.bridge_client import QmtBridgeClient
from quant_demo.core.config import AppSettings
from quant_demo.marketdata.ingestion import (
    ensure_history_dataset,
    load_history_dataframe,
    load_history_metadata,
    merge_history_frames,
    write_history_dataframe,
    write_history_metadata,
)


LOGGER = logging.getLogger(__name__)


class QuoteClient:
    def load_history(self, symbols: list[str], output_path: str) -> pd.DataFrame:
        raise NotImplementedError

    def get_latest_prices(self, symbols: list[str]) -> dict[str, Any]:
        raise NotImplementedError

    def get_instrument_details(self, symbols: list[str]) -> dict[str, Any]:
        raise NotImplementedError

    def healthcheck(self) -> dict[str, Any]:
        raise NotImplementedError


class LocalParquetQuoteClient(QuoteClient):
    def load_history(self, symbols: list[str], output_path: str) -> pd.DataFrame:
        ensure_history_dataset(symbols, output_path)
        return load_history_dataframe(output_path)

    def get_latest_prices(self, symbols: list[str]) -> dict[str, Any]:
        history_path = ensure_history_dataset(symbols, "data/parquet/history.parquet")
        history = load_history_dataframe(history_path)
        latest = history.sort_values(["trading_date", "symbol"]).groupby("symbol").tail(1)
        return {row["symbol"]: row["close"] for _, row in latest.iterrows()}

    def get_instrument_details(self, symbols: list[str]) -> dict[str, Any]:
        return {symbol: {"instrument_name": symbol, "data_source": "local_parquet"} for symbol in symbols}

    def healthcheck(self) -> dict[str, Any]:
        return {"mode": "local_parquet", "status": "ok"}


class XtQuantQuoteClient(QuoteClient):
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.bridge = QmtBridgeClient(settings)

    def load_history(self, symbols: list[str], output_path: str) -> pd.DataFrame:
        frame, _ = self.update_history(symbols, output_path, mode="auto")
        return frame

    def update_history(self, symbols: list[str], output_path: str | Path, mode: str = "auto") -> tuple[pd.DataFrame, dict[str, Any]]:
        output = Path(output_path)
        resolved_symbols = self.resolve_symbols(symbols)
        signature = self._history_signature(resolved_symbols)
        selected_mode = self._resolve_mode(output, signature, mode)
        LOGGER.info(
            "准备刷新 QMT 历史: requested_mode=%s selected_mode=%s output=%s symbols=%s start=%s end=%s",
            mode,
            selected_mode,
            output,
            len(resolved_symbols),
            self.settings.history_start,
            self.settings.history_end or "latest",
        )
        if selected_mode == "cached":
            LOGGER.info("命中历史缓存，直接复用: output=%s", output)
            return load_history_dataframe(output), {"mode": "cached", "fetched_batches": 0, "fetched_rows": 0}
        if selected_mode == "full":
            frame = self._fetch_history(resolved_symbols, self.settings.history_start, self.settings.history_end)
            write_history_dataframe(frame, output)
            write_history_metadata(output, {**signature, "row_count": len(frame), "cache_mode": "full"})
            LOGGER.info(
                "全量刷新写盘完成: output=%s rows=%s batches=%s",
                output,
                len(frame),
                self._batch_count(resolved_symbols),
            )
            return load_history_dataframe(output), {"mode": "full", "fetched_batches": self._batch_count(resolved_symbols), "fetched_rows": len(frame)}

        existing = load_history_dataframe(output)
        existing_metadata = load_history_metadata(output)
        next_start = self._next_start_time(existing, existing_metadata)
        LOGGER.info(
            "增量刷新准备完成: existing_rows=%s latest=%s next_start=%s",
            len(existing),
            str(existing["trading_date"].max()) if not existing.empty else "",
            next_start,
        )
        if self._range_finished(next_start, self.settings.history_end):
            write_history_metadata(output, {**signature, "row_count": len(existing), "cache_mode": "incremental"})
            LOGGER.info("增量区间已结束，无需抓取: output=%s", output)
            return existing, {"mode": "incremental", "fetched_batches": 0, "fetched_rows": 0}
        incoming = self._fetch_history(resolved_symbols, next_start, self.settings.history_end)
        merged = merge_history_frames(existing, incoming)
        write_history_dataframe(merged, output)
        write_history_metadata(output, {**signature, "row_count": len(merged), "cache_mode": "incremental", "incremental_start": next_start})
        LOGGER.info(
            "增量刷新写盘完成: output=%s incoming_rows=%s merged_rows=%s",
            output,
            len(incoming),
            len(merged),
        )
        return load_history_dataframe(output), {"mode": "incremental", "fetched_batches": self._batch_count(resolved_symbols), "fetched_rows": len(incoming)}

    def resolve_symbols(self, symbols: list[str] | None = None) -> list[str]:
        requested = [item for item in (symbols or self.settings.symbols) if item]
        if requested:
            return sorted(dict.fromkeys(requested))
        sector_name = self.settings.history_universe_sector
        if not sector_name:
            return []
        return sorted(
            dict.fromkeys(
                self.bridge.get_sector_symbols(
                    sector_name=sector_name,
                    only_a_share=True,
                    limit=max(0, self.settings.history_universe_limit),
                )
            )
        )

    def get_latest_prices(self, symbols: list[str]) -> dict[str, Any]:
        return self.bridge.get_latest_prices(symbols)

    def get_instrument_details(self, symbols: list[str]) -> dict[str, Any]:
        return self.bridge.get_instrument_details(symbols)

    def healthcheck(self) -> dict[str, Any]:
        return self.bridge.healthcheck()

    def _fetch_history(self, symbols: list[str], start_time: str, end_time: str) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        batches = self._iter_batches(symbols)
        total_batches = len(batches)
        cumulative_rows = 0
        LOGGER.info(
            "开始抓取 QMT 历史批次: batches=%s batch_size=%s start=%s end=%s",
            total_batches,
            max(1, self.settings.history_batch_size),
            start_time or self.settings.history_start,
            end_time or "latest",
        )
        for index, batch in enumerate(batches, start=1):
            if not batch:
                continue
            started_at = time.perf_counter()
            batch_frame = self.bridge.get_history(
                symbols=batch,
                period=self.settings.history_period,
                start_time=start_time,
                end_time=end_time,
                dividend_type=self.settings.history_adjustment,
                fill_data=self.settings.history_fill_data,
            )
            elapsed = time.perf_counter() - started_at
            batch_rows = len(batch_frame)
            cumulative_rows += batch_rows
            batch_latest = str(batch_frame["trading_date"].max()) if not batch_frame.empty else ""
            batch_earliest = str(batch_frame["trading_date"].min()) if not batch_frame.empty else ""
            LOGGER.info(
                "QMT 历史批次进度: %s/%s symbols=%s rows=%s cumulative_rows=%s earliest=%s latest=%s elapsed=%.2fs",
                index,
                total_batches,
                len(batch),
                batch_rows,
                cumulative_rows,
                batch_earliest,
                batch_latest,
                elapsed,
            )
            frames.append(batch_frame)
        if not frames:
            return pd.DataFrame(columns=["trading_date", "symbol", "open", "high", "low", "close", "volume", "amount"])
        frame = pd.concat(frames, ignore_index=True)
        frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.date
        frame = frame[pd.to_datetime(frame["trading_date"]).dt.dayofweek < 5]
        LOGGER.info(
            "QMT 历史抓取汇总完成: raw_rows=%s filtered_rows=%s earliest=%s latest=%s",
            cumulative_rows,
            len(frame),
            str(frame["trading_date"].min()) if not frame.empty else "",
            str(frame["trading_date"].max()) if not frame.empty else "",
        )
        return frame.sort_values(["trading_date", "symbol"]).reset_index(drop=True)

    def _resolve_mode(self, output_path: Path, signature: dict[str, Any], requested_mode: str) -> str:
        if requested_mode == "full":
            return "full"
        if not output_path.exists():
            return "full"
        if requested_mode == "cached":
            return "cached"
        if requested_mode == "incremental":
            if not self._is_signature_compatible(output_path, signature):
                LOGGER.warning("历史签名与缓存不完全一致，显式 incremental 模式下继续执行增量刷新: output=%s", output_path)
            return "incremental"
        if not self.settings.history_force_refresh and self._is_cache_valid(output_path, signature):
            return "cached"
        return "incremental" if self._is_signature_compatible(output_path, signature) else "full"

    def _history_signature(self, symbols: list[str]) -> dict[str, Any]:
        digest = hashlib.sha256("\n".join(sorted(symbols)).encode("utf-8")).hexdigest() if symbols else ""
        return {
            "source": "qmt",
            "symbols_count": len(symbols),
            "symbols_digest": digest,
            "period": self.settings.history_period,
            "adjustment": self.settings.history_adjustment,
            "start_time": self.settings.history_start,
            "end_time": self.settings.history_end,
            "fill_data": self.settings.history_fill_data,
            "universe_sector": self.settings.history_universe_sector,
            "universe_limit": self.settings.history_universe_limit,
        }

    def _iter_batches(self, symbols: list[str]) -> list[list[str]]:
        batch_size = max(1, self.settings.history_batch_size)
        return [symbols[index : index + batch_size] for index in range(0, len(symbols), batch_size)]

    def _batch_count(self, symbols: list[str]) -> int:
        return len(self._iter_batches(symbols))

    @staticmethod
    def _is_cache_valid(output_path: Path, signature: dict[str, Any]) -> bool:
        metadata = load_history_metadata(output_path)
        return bool(metadata) and all(metadata.get(key) == value for key, value in signature.items())

    @staticmethod
    def _is_signature_compatible(output_path: Path, signature: dict[str, Any]) -> bool:
        metadata = load_history_metadata(output_path)
        compatible_keys = ["source", "symbols_count", "symbols_digest", "period", "adjustment", "fill_data", "universe_sector", "universe_limit"]
        return bool(metadata) and all(metadata.get(key) == signature.get(key) for key in compatible_keys)

    @staticmethod
    def _next_start_time(frame: pd.DataFrame, metadata: dict[str, Any] | None = None) -> str:
        latest = pd.Timestamp(frame["trading_date"].max()).normalize()
        today = pd.Timestamp(datetime.now().date()).normalize()
        updated_at = None
        if metadata:
            raw_updated_at = str(metadata.get("updated_at") or "").strip()
            if raw_updated_at:
                updated_at = pd.Timestamp(raw_updated_at).normalize()
        if updated_at is not None and latest >= updated_at:
            return latest.strftime("%Y%m%d")
        if latest >= today:
            return latest.strftime("%Y%m%d")
        return (latest + pd.Timedelta(days=1)).strftime("%Y%m%d")

    @staticmethod
    def _range_finished(start_time: str, end_time: str) -> bool:
        if not end_time:
            return False
        return pd.Timestamp(start_time) > pd.Timestamp(end_time)
