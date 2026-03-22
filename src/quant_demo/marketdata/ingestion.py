from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from quant_demo.core.exceptions import DataNotReadyError


def generate_sample_history(symbols: list[str], output_path: str | Path, days: int = 120) -> Path:
    dates = pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=days)
    rows: list[dict] = []
    for offset, symbol in enumerate(symbols):
        base = 1.0 + offset * 0.3
        for index, trading_day in enumerate(dates):
            drift = 0.0008 * (index + 1)
            wave = math.sin(index / (5 + offset)) * 0.02
            close_price = round(3.0 + base + drift + wave, 4)
            rows.append(
                {
                    "trading_date": trading_day.date(),
                    "symbol": symbol,
                    "open": round(close_price * 0.995, 4),
                    "high": round(close_price * 1.01, 4),
                    "low": round(close_price * 0.99, 4),
                    "close": close_price,
                    "volume": 1_000_000 + index * 500 + offset * 10_000,
                }
            )
    frame = pd.DataFrame(rows)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def history_metadata_path(output_path: str | Path) -> Path:
    path = Path(output_path)
    return path.with_name(f"{path.name}.meta.json")


def write_history_dataframe(frame: pd.DataFrame, output_path: str | Path) -> Path:
    required_columns = {"trading_date", "symbol", "open", "high", "low", "close", "volume"}
    missing = sorted(required_columns - set(frame.columns))
    if missing:
        raise DataNotReadyError(f"鍘嗗彶鏁版嵁缂哄皯蹇呰瀛楁: {', '.join(missing)}")
    normalized = frame.copy()
    normalized["trading_date"] = pd.to_datetime(normalized["trading_date"]).dt.date
    normalized = normalized.sort_values(["trading_date", "symbol"]).reset_index(drop=True)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_parquet(path, index=False)
    return path


def write_history_metadata(output_path: str | Path, metadata: dict[str, Any]) -> Path:
    path = history_metadata_path(output_path)
    payload = dict(metadata)
    payload["updated_at"] = datetime.now().isoformat(timespec="seconds")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_history_metadata(output_path: str | Path) -> dict[str, Any]:
    path = history_metadata_path(output_path)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_history_dataset(symbols: list[str], output_path: str | Path, force_refresh: bool = False) -> Path:
    path = Path(output_path)
    if path.exists() and not force_refresh:
        return path
    return generate_sample_history(symbols, path)


def merge_history_frames(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return incoming.copy()
    if incoming.empty:
        return existing.copy()
    merged = pd.concat([existing, incoming], ignore_index=True)
    merged["trading_date"] = pd.to_datetime(merged["trading_date"]).dt.date
    merged = merged.sort_values(["trading_date", "symbol"]).drop_duplicates(["trading_date", "symbol"], keep="last")
    return merged.reset_index(drop=True)


def load_history_dataframe(path: str | Path) -> pd.DataFrame:
    file_path = Path(path)
    if not file_path.exists():
        raise DataNotReadyError(f"鍘嗗彶鏁版嵁涓嶅瓨鍦? {file_path}")
    frame = pd.read_parquet(file_path)
    frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.date
    return frame.sort_values(["trading_date", "symbol"]).reset_index(drop=True)
