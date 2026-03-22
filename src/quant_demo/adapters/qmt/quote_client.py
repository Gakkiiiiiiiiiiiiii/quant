from __future__ import annotations

import hashlib
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
        if selected_mode == "cached":
            return load_history_dataframe(output), {"mode": "cached", "fetched_batches": 0, "fetched_rows": 0}
        if selected_mode == "full":
            frame = self._fetch_history(resolved_symbols, self.settings.history_start, self.settings.history_end)
            write_history_dataframe(frame, output)
            write_history_metadata(output, {**signature, "row_count": len(frame), "cache_mode": "full"})
            return load_history_dataframe(output), {"mode": "full", "fetched_batches": self._batch_count(resolved_symbols), "fetched_rows": len(frame)}

        existing = load_history_dataframe(output)
        next_start = self._next_start_time(existing)
        if self._range_finished(next_start, self.settings.history_end):
            write_history_metadata(output, {**signature, "row_count": len(existing), "cache_mode": "incremental"})
            return existing, {"mode": "incremental", "fetched_batches": 0, "fetched_rows": 0}
        incoming = self._fetch_history(resolved_symbols, next_start, self.settings.history_end)
        merged = merge_history_frames(existing, incoming)
        write_history_dataframe(merged, output)
        write_history_metadata(output, {**signature, "row_count": len(merged), "cache_mode": "incremental", "incremental_start": next_start})
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
        for batch in self._iter_batches(symbols):
            if not batch:
                continue
            frames.append(
                self.bridge.get_history(
                    symbols=batch,
                    period=self.settings.history_period,
                    start_time=start_time,
                    end_time=end_time,
                    dividend_type=self.settings.history_adjustment,
                    fill_data=self.settings.history_fill_data,
                )
            )
        if not frames:
            return pd.DataFrame(columns=["trading_date", "symbol", "open", "high", "low", "close", "volume", "amount"])
        frame = pd.concat(frames, ignore_index=True)
        frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.date
        frame = frame[pd.to_datetime(frame["trading_date"]).dt.dayofweek < 5]
        return frame.sort_values(["trading_date", "symbol"]).reset_index(drop=True)

    def _resolve_mode(self, output_path: Path, signature: dict[str, Any], requested_mode: str) -> str:
        if requested_mode == "full":
            return "full"
        if not output_path.exists():
            return "full"
        if requested_mode == "cached":
            return "cached"
        if requested_mode == "incremental":
            return "incremental" if self._is_signature_compatible(output_path, signature) else "full"
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
    def _next_start_time(frame: pd.DataFrame) -> str:
        latest = pd.Timestamp(frame["trading_date"].max()) + pd.Timedelta(days=1)
        return latest.strftime("%Y%m%d")

    @staticmethod
    def _range_finished(start_time: str, end_time: str) -> bool:
        if not end_time:
            return False
        return pd.Timestamp(start_time) > pd.Timestamp(end_time)
