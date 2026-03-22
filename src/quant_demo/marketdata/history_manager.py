from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pandas as pd

from quant_demo.adapters.qmt.gateway import create_gateway
from quant_demo.adapters.qmt.quote_client import XtQuantQuoteClient
from quant_demo.core.config import AppSettings
from quant_demo.marketdata.ingestion import history_metadata_path, load_history_dataframe, load_history_metadata, merge_history_frames


def resolve_history_symbols(settings: AppSettings) -> list[str]:
    gateway = create_gateway(settings)
    quote_client = gateway.quote_client
    resolver = getattr(quote_client, "resolve_symbols", None)
    if callable(resolver):
        symbols = resolver(settings.symbols)
    else:
        symbols = [item for item in settings.symbols if item]
    return sorted(dict.fromkeys(symbols))


def history_status(settings: AppSettings) -> dict[str, Any]:
    history_path = Path(settings.history_parquet)
    metadata = load_history_metadata(history_path)
    status: dict[str, Any] = {
        "history_path": str(history_path),
        "history_exists": history_path.exists(),
        "metadata_path": str(history_metadata_path(history_path)),
        "metadata": metadata,
        "provider_dir": settings.qlib_provider_dir,
        "provider_exists": Path(settings.qlib_provider_dir).exists(),
        "dataset_dir": settings.qlib_dataset_dir,
        "dataset_exists": Path(settings.qlib_dataset_dir).exists(),
        "history_universe_sector": settings.history_universe_sector,
        "history_universe_limit": settings.history_universe_limit,
        "backtest_engine": settings.backtest_engine,
    }
    if history_path.exists():
        frame = load_history_dataframe(history_path)
        status.update(
            {
                "row_count": len(frame),
                "symbol_count": int(frame["symbol"].nunique()) if not frame.empty else 0,
                "latest_trading_date": str(frame["trading_date"].max()) if not frame.empty else "",
                "earliest_trading_date": str(frame["trading_date"].min()) if not frame.empty else "",
                "history_size_mb": round(history_path.stat().st_size / 1024 / 1024, 2),
            }
        )
    else:
        status.update({"row_count": 0, "symbol_count": 0, "latest_trading_date": "", "earliest_trading_date": "", "history_size_mb": 0.0})
    return status


def cleanup_history_cache(
    settings: AppSettings,
    *,
    remove_history: bool = False,
    remove_qlib: bool = False,
) -> dict[str, Any]:
    removed: dict[str, Any] = {"history_removed": [], "qlib_removed": []}
    history_path = Path(settings.history_parquet)
    metadata_path = history_metadata_path(history_path)
    if remove_history:
        for path in [history_path, metadata_path]:
            if path.exists():
                path.unlink()
                removed["history_removed"].append(str(path))
    if remove_qlib:
        for raw in [settings.qlib_provider_dir, settings.qlib_dataset_dir]:
            path = Path(raw)
            if path.exists():
                if path.is_dir():
                    import shutil

                    shutil.rmtree(path, ignore_errors=True)
                else:
                    path.unlink()
                removed["qlib_removed"].append(str(path))
    removed["status"] = history_status(settings)
    return removed


def refresh_history(settings: AppSettings, mode: str = "auto") -> dict[str, Any]:
    gateway = create_gateway(settings)
    quote_client = gateway.quote_client
    symbols = resolve_history_symbols(settings)
    history_path = Path(settings.history_parquet)

    if not isinstance(quote_client, XtQuantQuoteClient):
        frame = quote_client.load_history(symbols, history_path)
        metadata = load_history_metadata(history_path)
        return {
            "mode": "local",
            "row_count": len(frame),
            "symbol_count": int(frame["symbol"].nunique()) if not frame.empty else 0,
            "metadata": metadata,
            "latest_trading_date": str(frame["trading_date"].max()) if not frame.empty else "",
        }

    frame, details = quote_client.update_history(symbols, history_path, mode=mode)
    metadata = load_history_metadata(history_path)
    return {
        "mode": details.get("mode", mode),
        "row_count": len(frame),
        "symbol_count": int(frame["symbol"].nunique()) if not frame.empty else 0,
        "latest_trading_date": str(frame["trading_date"].max()) if not frame.empty else "",
        "metadata": metadata,
        "fetched_batches": details.get("fetched_batches", 0),
        "fetched_rows": details.get("fetched_rows", 0),
        "history_digest": hashlib.sha256("\n".join(symbols).encode("utf-8")).hexdigest() if symbols else "",
    }