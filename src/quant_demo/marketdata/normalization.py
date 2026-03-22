from __future__ import annotations

import pandas as pd


def normalize_ohlcv(frame: pd.DataFrame) -> pd.DataFrame:
    renamed = frame.rename(
        columns={
            "date": "trading_date",
            "datetime": "trading_date",
            "vol": "volume",
        }
    )
    required = ["trading_date", "symbol", "open", "high", "low", "close", "volume"]
    missing = [column for column in required if column not in renamed.columns]
    if missing:
        raise ValueError(f"缺失行情字段: {missing}")
    return renamed[required].copy()
