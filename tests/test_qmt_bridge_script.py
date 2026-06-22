from __future__ import annotations

import json
from types import SimpleNamespace

import pandas as pd

from scripts import qmt_bridge


def _history_frame(trading_date: str, close_price: float) -> pd.DataFrame:
    timestamp = int(pd.Timestamp(f"{trading_date} 00:00:00", tz="Asia/Shanghai").tz_convert("UTC").timestamp() * 1000)
    return pd.DataFrame(
        [
            {
                "time": timestamp,
                "open": close_price - 0.1,
                "high": close_price + 0.1,
                "low": close_price - 0.2,
                "close": close_price,
                "volume": 1000,
                "amount": close_price * 1000,
            }
        ]
    )


def test_command_history_prefers_cache_first_and_only_downloads_missing_symbols(
    monkeypatch,
    capsys,
) -> None:
    downloaded_symbols: list[str] = []
    market_data_calls: list[list[str]] = []

    class FakeXtdata:
        def download_history_data(self, symbol, period, start_time, end_time):  # type: ignore[no-untyped-def]
            downloaded_symbols.append(symbol)

        def get_market_data_ex(self, **kwargs):  # type: ignore[no-untyped-def]
            symbols = list(kwargs["stock_list"])
            market_data_calls.append(symbols)
            if symbols == ["AAA.SZ", "BBB.SZ"]:
                return {"AAA.SZ": _history_frame("2026-05-20", 10.0)}
            if symbols == ["BBB.SZ"]:
                return {"BBB.SZ": _history_frame("2026-05-20", 20.0)}
            raise AssertionError(f"unexpected symbols: {symbols}")

    monkeypatch.setattr(qmt_bridge, "bootstrap_runtime", lambda install_dir: (None, FakeXtdata(), None, None))

    args = SimpleNamespace(
        install_dir="runtime/qmt_client/live_installed",
        userdata_dir="runtime/qmt_client/live_installed/userdata_mini",
        account_id="",
        symbols="AAA.SZ,BBB.SZ",
        period="1d",
        start_time="20260520",
        end_time="",
        dividend_type="front",
        fill_data="true",
        prefer_cache_first="true",
    )

    result = qmt_bridge.command_history(args)

    assert result == 0
    assert downloaded_symbols == ["BBB.SZ"]
    assert market_data_calls == [["AAA.SZ", "BBB.SZ"], ["BBB.SZ"]]

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["data"]["downloaded_symbol_count"] == 1
    assert payload["data"]["row_count"] == 2


def test_command_history_without_cache_first_downloads_all_symbols(monkeypatch, capsys) -> None:
    downloaded_symbols: list[str] = []

    class FakeXtdata:
        def download_history_data(self, symbol, period, start_time, end_time):  # type: ignore[no-untyped-def]
            downloaded_symbols.append(symbol)

        def get_market_data_ex(self, **kwargs):  # type: ignore[no-untyped-def]
            return {
                "AAA.SZ": _history_frame("2026-05-20", 10.0),
                "BBB.SZ": _history_frame("2026-05-20", 20.0),
            }

    monkeypatch.setattr(qmt_bridge, "bootstrap_runtime", lambda install_dir: (None, FakeXtdata(), None, None))

    args = SimpleNamespace(
        install_dir="runtime/qmt_client/live_installed",
        userdata_dir="runtime/qmt_client/live_installed/userdata_mini",
        account_id="",
        symbols="AAA.SZ,BBB.SZ",
        period="1d",
        start_time="20260520",
        end_time="",
        dividend_type="front",
        fill_data="true",
        prefer_cache_first="false",
    )

    result = qmt_bridge.command_history(args)

    assert result == 0
    assert downloaded_symbols == ["AAA.SZ", "BBB.SZ"]

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["data"]["downloaded_symbol_count"] == 2
