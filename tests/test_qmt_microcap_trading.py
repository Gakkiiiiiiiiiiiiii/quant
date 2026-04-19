from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import select

from quant_demo.core.config import AppSettings, StrategySettings
from quant_demo.core.exceptions import QmtUnavailableError
from quant_demo.db.models import AssetSnapshotModel, AuditLogModel, OrderModel
from quant_demo.db.session import create_session_factory, session_scope
from quant_demo.experiment.evaluator import EvaluationResult
from quant_demo.experiment.manager import ExperimentManager
from quant_demo.experiment.qmt_microcap_trading import QmtMicrocapTradingEngine


def _build_app_settings(tmp_path: Path, *, environment: str = "paper", trade_enabled: bool = True) -> AppSettings:
    return AppSettings.model_validate(
        {
            "app_name": "qmt-microcap-test",
            "environment": environment,
            "database_url": f"sqlite:///{(tmp_path / 'demo.db').as_posix()}",
            "history_parquet": str(tmp_path / "history.parquet"),
            "history_source": "qmt",
            "history_period": "1d",
            "history_adjustment": "front",
            "history_start": "20200101",
            "history_end": "20260411",
            "history_fill_data": True,
            "report_dir": str(tmp_path / "reports"),
            "qmt_install_dir": str(tmp_path / "runtime" / "installed"),
            "qmt_download_url": "https://example.com/qmt.rar",
            "qmt_userdata_dir": str(tmp_path / "runtime" / "installed" / "userdata_mini"),
            "qmt_bridge_python": str(tmp_path / ".venv-qmt36" / "Scripts" / "python.exe"),
            "qmt_bridge_script": str(tmp_path / "scripts" / "qmt_bridge.py"),
            "qmt_trade_enabled": trade_enabled,
            "default_strategy": "joinquant_microcap_alpha",
            "history_universe_sector": "沪深京A股",
            "symbols": [],
            "risk": {
                "max_position_ratio": 0.6,
                "daily_loss_limit": -0.2,
                "trading_start": "00:00",
                "trading_end": "23:59",
            },
        }
    )


def _build_strategy_settings() -> StrategySettings:
    return StrategySettings.model_validate(
        {
            "name": "joinquant_microcap_alpha",
            "implementation": "joinquant_microcap_alpha",
            "rebalance_frequency": "daily",
            "lookback_days": 20,
            "top_n": 1,
            "lot_size": 100,
            "extra": {
                "target_hold_num": 1,
                "buy_rank": 1,
                "keep_rank": 1,
                "query_limit": 10,
                "min_list_days": 0,
                "min_avg_money_20": 1.0,
                "cash_buffer": 0.0,
                "min_price_floor": 1.0,
                "max_price_normal_hard": 50.0,
                "max_price_star_hard": 25.0,
                "max_overweight_ratio": 1.15,
                "hedge_symbol": "",
                "hedge_name": "现金",
                "seasonal_hedge_schedule": {4: 0.5},
                "buy_slippage_bps": 35.0,
                "sell_slippage_bps": 35.0,
                "max_trade_volume_ratio": 0.5,
            },
        }
    )


def _write_history(path: Path) -> None:
    dates = pd.bdate_range("2026-03-02", "2026-04-10")
    rows: list[dict[str, object]] = []
    for trading_date in dates:
        rows.append(
            {
                "trading_date": trading_date.date(),
                "symbol": "AAA.SZ",
                "open": 10.0,
                "high": 10.2,
                "low": 9.8,
                "close": 10.0,
                "volume": 1_500_000,
                "amount": 20_000_000.0,
            }
        )
        rows.append(
            {
                "trading_date": trading_date.date(),
                "symbol": "BBB.SZ",
                "open": 20.0,
                "high": 20.2,
                "low": 19.8,
                "close": 20.0,
                "volume": 1_500_000,
                "amount": 40_000_000.0,
            }
        )
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_experiment_manager_routes_microcap_non_backtest_to_qmt_engine(tmp_path: Path, monkeypatch) -> None:
    calls: list[str] = []

    class DummyTradingEngine:
        def __init__(self, session_factory, app_settings, strategy_settings) -> None:  # type: ignore[no-untyped-def]
            calls.append("paper")

        def run(self, initial_cash: Decimal):  # type: ignore[no-untyped-def]
            return tmp_path / "paper.md", EvaluationResult(0.0, 0.0, 0.0, 0.0), pd.DataFrame([{"equity": 1.0}])

    class DummyBacktestEngine:
        def __init__(self, session_factory, app_settings, strategy_settings) -> None:  # type: ignore[no-untyped-def]
            calls.append("backtest")

        def run(self, initial_cash: Decimal):  # type: ignore[no-untyped-def]
            return tmp_path / "backtest.md", EvaluationResult(0.0, 0.0, 0.0, 0.0), pd.DataFrame([{"equity": 1.0}])

    monkeypatch.setattr("quant_demo.experiment.manager.QmtMicrocapTradingEngine", DummyTradingEngine)
    monkeypatch.setattr("quant_demo.experiment.manager.JoinQuantMicrocapBacktestEngine", DummyBacktestEngine)

    paper_settings = _build_app_settings(tmp_path, environment="paper")
    strategy_settings = _build_strategy_settings()
    manager = ExperimentManager(object(), paper_settings, strategy_settings)
    manager.run()

    backtest_settings = _build_app_settings(tmp_path, environment="backtest", trade_enabled=False)
    manager = ExperimentManager(object(), backtest_settings, strategy_settings)
    manager.run()

    assert calls == ["paper", "backtest"]


def test_qmt_microcap_trading_engine_submits_orders_and_persists_plan(tmp_path: Path, monkeypatch) -> None:
    app_settings = _build_app_settings(tmp_path, environment="paper", trade_enabled=True)
    strategy_settings = _build_strategy_settings()
    history_path = Path(app_settings.history_parquet)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    _write_history(history_path)
    session_factory = create_session_factory(app_settings.database_url)

    instrument_frame = pd.DataFrame(
        [
            {"symbol": "AAA.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Alpha", "total_capital_current": 1_000_000.0},
            {"symbol": "BBB.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Beta", "total_capital_current": 2_000_000.0},
        ]
    )
    capital_frame = pd.DataFrame(columns=["symbol", "effective_date", "total_capital", "circulating_capital", "free_float_capital"])
    submitted: list[dict[str, object]] = []

    monkeypatch.setattr(QmtMicrocapTradingEngine, "_refresh_history", lambda self: None)
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_instrument_frame",
        lambda self, symbols: instrument_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_capital_frame",
        lambda self, symbols: capital_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_account_snapshot",
        lambda self: {
            "account_id": "paper-account",
            "asset": {"cash": 100000.0, "frozen_cash": 0.0, "total_asset": 100000.0},
            "positions": [],
            "orders": [],
            "trades": [],
        },
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_quotes",
        lambda self, symbols: {
            "AAA.SZ": {"last_price": 10.0, "volume": 2_000_000},
            "BBB.SZ": {"last_price": 20.0, "volume": 2_000_000},
        },
    )

    def _fake_submit(self, symbol, side, qty, price, **kwargs):  # type: ignore[no-untyped-def]
        submitted.append({"symbol": symbol, "side": side, "qty": qty, "price": float(price), "extra": kwargs})
        return {"order_id": f"mock-{len(submitted)}"}

    monkeypatch.setattr("quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.submit_order", _fake_submit)

    engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
    report_path, metrics, equity_curve = engine.run(Decimal("100000"))

    assert report_path.exists()
    assert not equity_curve.empty
    assert metrics.turnover > 0
    assert len(submitted) == 1
    assert submitted[0]["symbol"] == "AAA.SZ"
    assert submitted[0]["side"] == "buy"
    assert submitted[0]["qty"] > 0

    with session_scope(session_factory) as session:
        orders = list(session.scalars(select(OrderModel).order_by(OrderModel.created_at)))
        audits = list(session.scalars(select(AuditLogModel).order_by(AuditLogModel.created_at)))

    assert len(orders) == 1
    assert orders[0].broker_order_id == "mock-1"
    assert any(item.object_type == "microcap_trade_plan" for item in audits)


def test_qmt_microcap_trading_engine_caps_paper_strategy_asset_to_initial_cash(tmp_path: Path, monkeypatch) -> None:
    app_settings = _build_app_settings(tmp_path, environment="paper", trade_enabled=False)
    strategy_settings = _build_strategy_settings()
    history_path = Path(app_settings.history_parquet)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    _write_history(history_path)
    session_factory = create_session_factory(app_settings.database_url)

    instrument_frame = pd.DataFrame(
        [
            {"symbol": "AAA.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Alpha", "total_capital_current": 1_000_000.0},
            {"symbol": "BBB.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Beta", "total_capital_current": 2_000_000.0},
        ]
    )
    capital_frame = pd.DataFrame(columns=["symbol", "effective_date", "total_capital", "circulating_capital", "free_float_capital"])

    monkeypatch.setattr(QmtMicrocapTradingEngine, "_refresh_history", lambda self: None)
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_instrument_frame",
        lambda self, symbols: instrument_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_capital_frame",
        lambda self, symbols: capital_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_account_snapshot",
        lambda self: {
            "account_id": "paper-account",
            "asset": {"cash": 21_000_000.0, "frozen_cash": 0.0, "total_asset": 21_000_000.0},
            "positions": [],
            "orders": [],
            "trades": [],
        },
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_quotes",
        lambda self, symbols: {
            "AAA.SZ": {"last_price": 10.0, "volume": 2_000_000},
            "BBB.SZ": {"last_price": 20.0, "volume": 2_000_000},
        },
    )

    engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
    report_path, metrics, equity_curve = engine.run(Decimal("100000"))

    assert report_path.exists()
    assert metrics.turnover > 0
    assert float(equity_curve.iloc[-1]["equity"]) == 100000.0

    with session_scope(session_factory) as session:
        asset_snapshots = list(session.scalars(select(AssetSnapshotModel).order_by(AssetSnapshotModel.snapshot_time)))

    assert asset_snapshots
    assert float(asset_snapshots[-1].total_asset) == 100000.0
    assert float(asset_snapshots[-1].cash) <= 100000.0


def test_qmt_microcap_trading_engine_caps_live_strategy_asset_when_capital_is_explicit(tmp_path: Path, monkeypatch) -> None:
    app_settings = _build_app_settings(tmp_path, environment="live", trade_enabled=False)
    strategy_settings = _build_strategy_settings()
    history_path = Path(app_settings.history_parquet)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    _write_history(history_path)
    session_factory = create_session_factory(app_settings.database_url)

    instrument_frame = pd.DataFrame(
        [
            {"symbol": "AAA.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Alpha", "total_capital_current": 1_000_000.0},
            {"symbol": "BBB.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Beta", "total_capital_current": 2_000_000.0},
        ]
    )
    capital_frame = pd.DataFrame(columns=["symbol", "effective_date", "total_capital", "circulating_capital", "free_float_capital"])

    monkeypatch.setattr(QmtMicrocapTradingEngine, "_refresh_history", lambda self: None)
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_instrument_frame",
        lambda self, symbols: instrument_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_capital_frame",
        lambda self, symbols: capital_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_account_snapshot",
        lambda self: {
            "account_id": "live-account",
            "asset": {"cash": 21_000_000.0, "frozen_cash": 0.0, "total_asset": 21_000_000.0},
            "positions": [],
            "orders": [],
            "trades": [],
        },
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_quotes",
        lambda self, symbols: {
            "AAA.SZ": {"last_price": 10.0, "volume": 2_000_000},
            "BBB.SZ": {"last_price": 20.0, "volume": 2_000_000},
        },
    )

    engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
    plan_path, payload = engine.preview(Decimal("100000"))

    assert plan_path.exists()
    assert payload["strategy_total_asset"] == 100000.0


def test_qmt_microcap_trading_engine_load_recent_history_window_keeps_high_low_columns(tmp_path: Path) -> None:
    app_settings = _build_app_settings(tmp_path, environment="live", trade_enabled=False)
    strategy_settings = _build_strategy_settings()
    history_path = Path(app_settings.history_parquet)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    _write_history(history_path)
    session_factory = create_session_factory(app_settings.database_url)

    engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
    frame = engine._load_recent_history_window()

    assert {"open", "high", "low", "close", "volume", "amount"}.issubset(frame.columns)
    assert not frame.empty


def test_qmt_microcap_trading_engine_uses_previous_completed_day_during_trading_hours(tmp_path: Path, monkeypatch) -> None:
    app_settings = _build_app_settings(tmp_path, environment="paper", trade_enabled=False)
    app_settings = app_settings.model_copy(update={"history_end": ""})
    strategy_settings = _build_strategy_settings()
    history_path = Path(app_settings.history_parquet)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    _write_history(history_path)
    extra_rows = [
        {
            "trading_date": pd.Timestamp("2026-04-14").date(),
            "symbol": "AAA.SZ",
            "open": 10.0,
            "high": 10.2,
            "low": 9.8,
            "close": 10.05,
            "volume": 1_500_000,
            "amount": 20_000_000.0,
        },
        {
            "trading_date": pd.Timestamp("2026-04-14").date(),
            "symbol": "BBB.SZ",
            "open": 20.0,
            "high": 20.2,
            "low": 19.8,
            "close": 20.05,
            "volume": 1_500_000,
            "amount": 40_000_000.0,
        },
        {
            "trading_date": pd.Timestamp("2026-04-15").date(),
            "symbol": "AAA.SZ",
            "open": 10.0,
            "high": 10.2,
            "low": 9.8,
            "close": 10.1,
            "volume": 1_500_000,
            "amount": 20_000_000.0,
        },
        {
            "trading_date": pd.Timestamp("2026-04-15").date(),
            "symbol": "BBB.SZ",
            "open": 20.0,
            "high": 20.2,
            "low": 19.8,
            "close": 20.1,
            "volume": 1_500_000,
            "amount": 40_000_000.0,
        },
    ]
    frame = pd.concat([pd.read_parquet(history_path), pd.DataFrame(extra_rows)], ignore_index=True)
    frame.to_parquet(history_path, index=False)
    session_factory = create_session_factory(app_settings.database_url)

    engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
    monkeypatch.setattr(engine, "_now_local", lambda: datetime(2026, 4, 15, 13, 0, 0))

    assert engine._load_latest_history_date() == pd.Timestamp("2026-04-14")


def test_qmt_microcap_trading_engine_uses_today_after_close_cutoff(tmp_path: Path, monkeypatch) -> None:
    app_settings = _build_app_settings(tmp_path, environment="paper", trade_enabled=False)
    app_settings = app_settings.model_copy(update={"history_end": ""})
    strategy_settings = _build_strategy_settings()
    history_path = Path(app_settings.history_parquet)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    _write_history(history_path)
    extra_rows = [
        {
            "trading_date": pd.Timestamp("2026-04-14").date(),
            "symbol": "AAA.SZ",
            "open": 10.0,
            "high": 10.2,
            "low": 9.8,
            "close": 10.05,
            "volume": 1_500_000,
            "amount": 20_000_000.0,
        },
        {
            "trading_date": pd.Timestamp("2026-04-14").date(),
            "symbol": "BBB.SZ",
            "open": 20.0,
            "high": 20.2,
            "low": 19.8,
            "close": 20.05,
            "volume": 1_500_000,
            "amount": 40_000_000.0,
        },
        {
            "trading_date": pd.Timestamp("2026-04-15").date(),
            "symbol": "AAA.SZ",
            "open": 10.0,
            "high": 10.2,
            "low": 9.8,
            "close": 10.1,
            "volume": 1_500_000,
            "amount": 20_000_000.0,
        },
        {
            "trading_date": pd.Timestamp("2026-04-15").date(),
            "symbol": "BBB.SZ",
            "open": 20.0,
            "high": 20.2,
            "low": 19.8,
            "close": 20.1,
            "volume": 1_500_000,
            "amount": 40_000_000.0,
        },
    ]
    frame = pd.concat([pd.read_parquet(history_path), pd.DataFrame(extra_rows)], ignore_index=True)
    frame.to_parquet(history_path, index=False)
    session_factory = create_session_factory(app_settings.database_url)

    engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
    monkeypatch.setattr(engine, "_now_local", lambda: datetime(2026, 4, 15, 15, 6, 0))

    assert engine._load_latest_history_date() == pd.Timestamp("2026-04-15")


def test_qmt_microcap_trading_engine_preview_writes_t1_plan_without_submitting(tmp_path: Path, monkeypatch) -> None:
    app_settings = _build_app_settings(tmp_path, environment="paper", trade_enabled=True)
    strategy_settings = _build_strategy_settings()
    history_path = Path(app_settings.history_parquet)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    _write_history(history_path)
    session_factory = create_session_factory(app_settings.database_url)

    instrument_frame = pd.DataFrame(
        [
            {"symbol": "AAA.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Alpha", "total_capital_current": 1_000_000.0},
            {"symbol": "BBB.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Beta", "total_capital_current": 2_000_000.0},
        ]
    )
    capital_frame = pd.DataFrame(columns=["symbol", "effective_date", "total_capital", "circulating_capital", "free_float_capital"])

    monkeypatch.setattr(QmtMicrocapTradingEngine, "_refresh_history", lambda self: None)
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_instrument_frame",
        lambda self, symbols: instrument_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_capital_frame",
        lambda self, symbols: capital_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_account_snapshot",
        lambda self: {
            "account_id": "paper-account",
            "asset": {"cash": 100000.0, "frozen_cash": 0.0, "total_asset": 100000.0},
            "positions": [],
            "orders": [],
            "trades": [],
        },
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_quotes",
        lambda self, symbols: {
            "AAA.SZ": {"last_price": 10.0, "volume": 2_000_000},
            "BBB.SZ": {"last_price": 20.0, "volume": 2_000_000},
        },
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.submit_order",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("preview 模式不应调用 submit_order")),
    )

    engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
    plan_path, payload = engine.preview(Decimal("100000"))

    assert plan_path.exists()
    assert payload["signal_trade_date"] == "2026-04-10"
    assert payload["planned_execution_date"] == "2026-04-13"
    assert payload["strategy_total_asset"] == 100000.0
    assert len(payload["preview_orders"]) == 1

    stored = json.loads(plan_path.read_text(encoding="utf-8"))
    assert stored["target_meta"]["targets"] == ["AAA.SZ"]

    with session_scope(session_factory) as session:
        orders = list(session.scalars(select(OrderModel)))
    assert not orders


def test_qmt_microcap_trading_engine_execute_plan_uses_saved_targets(tmp_path: Path, monkeypatch) -> None:
    app_settings = _build_app_settings(tmp_path, environment="paper", trade_enabled=True)
    strategy_settings = _build_strategy_settings()
    history_path = Path(app_settings.history_parquet)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    _write_history(history_path)
    session_factory = create_session_factory(app_settings.database_url)

    instrument_frame = pd.DataFrame(
        [
            {"symbol": "AAA.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Alpha", "total_capital_current": 1_000_000.0},
            {"symbol": "BBB.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Beta", "total_capital_current": 2_000_000.0},
        ]
    )
    capital_frame = pd.DataFrame(columns=["symbol", "effective_date", "total_capital", "circulating_capital", "free_float_capital"])
    submitted: list[dict[str, object]] = []

    monkeypatch.setattr(QmtMicrocapTradingEngine, "_refresh_history", lambda self: None)
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_instrument_frame",
        lambda self, symbols: instrument_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_capital_frame",
        lambda self, symbols: capital_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_account_snapshot",
        lambda self: {
            "account_id": "paper-account",
            "asset": {"cash": 100000.0, "frozen_cash": 0.0, "total_asset": 100000.0},
            "positions": [],
            "orders": [],
            "trades": [],
        },
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_quotes",
        lambda self, symbols: {
            "AAA.SZ": {"last_price": 10.0, "volume": 2_000_000},
            "BBB.SZ": {"last_price": 20.0, "volume": 2_000_000},
        },
    )

    def _fake_submit(self, symbol, side, qty, price, **kwargs):  # type: ignore[no-untyped-def]
        submitted.append({"symbol": symbol, "side": side, "qty": qty, "price": float(price), "extra": kwargs})
        return {"order_id": f"exec-{len(submitted)}"}

    monkeypatch.setattr("quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.submit_order", _fake_submit)

    engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
    plan_path, payload = engine.preview(Decimal("100000"))
    assert payload["target_meta"]["targets"] == ["AAA.SZ"]
    monkeypatch.setattr(
        engine,
        "_prepare_trade_context",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("execute 不应重新计算全市场交易上下文")),
    )

    report_path, metrics, equity_curve = engine.execute_plan(plan_path)

    assert report_path.exists()
    assert metrics.turnover > 0
    assert not equity_curve.empty
    assert len(submitted) == 1
    assert submitted[0]["symbol"] == "AAA.SZ"
    assert submitted[0]["side"] == "buy"

    receipt_path = Path(app_settings.report_dir) / "trade_plans" / "microcap_t1_execution_20260410_for_20260413.json"
    assert receipt_path.exists()


def test_qmt_microcap_trading_engine_execute_plan_survives_post_submit_snapshot_timeout(tmp_path: Path, monkeypatch) -> None:
    app_settings = _build_app_settings(tmp_path, environment="paper", trade_enabled=True)
    strategy_settings = _build_strategy_settings()
    history_path = Path(app_settings.history_parquet)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    _write_history(history_path)
    session_factory = create_session_factory(app_settings.database_url)

    instrument_frame = pd.DataFrame(
        [
            {"symbol": "AAA.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Alpha", "total_capital_current": 1_000_000.0},
            {"symbol": "BBB.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Beta", "total_capital_current": 2_000_000.0},
        ]
    )
    capital_frame = pd.DataFrame(columns=["symbol", "effective_date", "total_capital", "circulating_capital", "free_float_capital"])
    snapshot_calls = {"count": 0}

    monkeypatch.setattr(QmtMicrocapTradingEngine, "_refresh_history", lambda self: None)
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_instrument_frame",
        lambda self, symbols: instrument_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_capital_frame",
        lambda self, symbols: capital_frame.copy(),
    )

    def _fake_snapshot(self):  # type: ignore[no-untyped-def]
        snapshot_calls["count"] += 1
        if snapshot_calls["count"] <= 2:
            return {
                "account_id": "paper-account",
                "asset": {"cash": 100000.0, "frozen_cash": 0.0, "total_asset": 100000.0},
                "positions": [],
                "orders": [],
                "trades": [],
            }
        raise QmtUnavailableError("snapshot timeout after submit")

    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_account_snapshot",
        _fake_snapshot,
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_quotes",
        lambda self, symbols: {
            "AAA.SZ": {"last_price": 10.0, "volume": 2_000_000},
            "BBB.SZ": {"last_price": 20.0, "volume": 2_000_000},
        },
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.submit_order",
        lambda self, symbol, side, qty, price, **kwargs: {"order_id": "post-submit-timeout-1"},
    )

    engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
    plan_path, _payload = engine.preview(Decimal("100000"))
    report_path, metrics, equity_curve = engine.execute_plan(plan_path)

    assert report_path.exists()
    assert metrics.turnover > 0
    assert not equity_curve.empty

    with session_scope(session_factory) as session:
        audits = list(session.scalars(select(AuditLogModel).order_by(AuditLogModel.created_at)))

    assert any("账户快照超时" in item.message for item in audits)


def test_qmt_microcap_trading_engine_preview_requires_qmt_realtime_positions(tmp_path: Path, monkeypatch) -> None:
    app_settings = _build_app_settings(tmp_path, environment="paper", trade_enabled=True)
    strategy_settings = _build_strategy_settings()
    history_path = Path(app_settings.history_parquet)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    _write_history(history_path)
    session_factory = create_session_factory(app_settings.database_url)

    instrument_frame = pd.DataFrame(
        [
            {"symbol": "AAA.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Alpha", "total_capital_current": 1_000_000.0},
            {"symbol": "BBB.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Beta", "total_capital_current": 2_000_000.0},
        ]
    )
    capital_frame = pd.DataFrame(columns=["symbol", "effective_date", "total_capital", "circulating_capital", "free_float_capital"])

    monkeypatch.setattr(QmtMicrocapTradingEngine, "_refresh_history", lambda self: None)
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_instrument_frame",
        lambda self, symbols: instrument_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_capital_frame",
        lambda self, symbols: capital_frame.copy(),
    )

    with session_scope(session_factory) as session:
        session.add(
            AssetSnapshotModel(
                account_id="paper-account",
                cash=Decimal("60000"),
                frozen_cash=Decimal("0"),
                total_asset=Decimal("100000"),
                total_pnl=Decimal("0"),
                turnover=Decimal("0"),
                max_drawdown=Decimal("0"),
                snapshot_time=pd.Timestamp("2026-04-14 15:05:00").to_pydatetime(),
            )
        )

    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_account_snapshot",
        lambda self: (_ for _ in ()).throw(QmtUnavailableError("snapshot timeout")),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_quotes",
        lambda self, symbols: (_ for _ in ()).throw(QmtUnavailableError("quote timeout")),
    )

    engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
    with pytest.raises(QmtUnavailableError, match="QMT"):
        engine.preview(Decimal("100000"))


def test_qmt_microcap_trading_engine_preview_errors_when_positions_field_missing(tmp_path: Path, monkeypatch) -> None:
    app_settings = _build_app_settings(tmp_path, environment="paper", trade_enabled=True)
    strategy_settings = _build_strategy_settings()
    history_path = Path(app_settings.history_parquet)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    _write_history(history_path)
    session_factory = create_session_factory(app_settings.database_url)

    instrument_frame = pd.DataFrame(
        [
            {"symbol": "AAA.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Alpha", "total_capital_current": 1_000_000.0},
            {"symbol": "BBB.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Beta", "total_capital_current": 2_000_000.0},
        ]
    )
    capital_frame = pd.DataFrame(columns=["symbol", "effective_date", "total_capital", "circulating_capital", "free_float_capital"])

    monkeypatch.setattr(QmtMicrocapTradingEngine, "_refresh_history", lambda self: None)
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_instrument_frame",
        lambda self, symbols: instrument_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_capital_frame",
        lambda self, symbols: capital_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_account_snapshot",
        lambda self: {
            "account_id": "paper-account",
            "asset": {"cash": 100000.0, "frozen_cash": 0.0, "total_asset": 100000.0},
            "orders": [],
            "trades": [],
        },
    )

    engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
    with pytest.raises(QmtUnavailableError, match="实时持仓不可用"):
        engine.preview(Decimal("100000"))


def test_qmt_microcap_trading_engine_execute_plan_skips_when_today_activity_matches_plan(tmp_path: Path, monkeypatch) -> None:
    app_settings = _build_app_settings(tmp_path, environment="paper", trade_enabled=True)
    strategy_settings = _build_strategy_settings()
    history_path = Path(app_settings.history_parquet)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    _write_history(history_path)
    session_factory = create_session_factory(app_settings.database_url)

    instrument_frame = pd.DataFrame(
        [
            {"symbol": "AAA.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Alpha", "total_capital_current": 1_000_000.0},
            {"symbol": "BBB.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Beta", "total_capital_current": 2_000_000.0},
        ]
    )
    capital_frame = pd.DataFrame(columns=["symbol", "effective_date", "total_capital", "circulating_capital", "free_float_capital"])

    monkeypatch.setattr(QmtMicrocapTradingEngine, "_refresh_history", lambda self: None)
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_instrument_frame",
        lambda self, symbols: instrument_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_capital_frame",
        lambda self, symbols: capital_frame.copy(),
    )
    snapshot_payload = {
        "account_id": "paper-account",
        "asset": {"cash": 100000.0, "frozen_cash": 0.0, "total_asset": 100000.0},
        "positions": [],
        "orders": [],
        "trades": [],
    }
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_account_snapshot",
        lambda self: snapshot_payload,
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_quotes",
        lambda self, symbols: {"AAA.SZ": {"last_price": 10.0, "volume": 2_000_000}},
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.submit_order",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("已有同日委托时不应重复补单")),
    )

    engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
    plan_path, payload = engine.preview(Decimal("100000"))
    preview_qty = int(payload["preview_orders"][0]["qty"])
    snapshot_payload["orders"] = [
        {"stock_code": "AAA.SZ", "order_id": "qmt-1", "order_time": "2026-04-13 09:35:01", "order_volume": preview_qty, "order_type": "buy"}
    ]
    receipt_path, metrics, equity_curve = engine.execute_plan(plan_path)

    assert receipt_path.exists()
    assert metrics.turnover == 0.0
    assert len(equity_curve) == 1
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["submitted_orders"] == []
    assert receipt["execution_summary"]["remaining_order_count"] == 0


def test_qmt_microcap_trading_engine_execute_plan_submits_only_delta_qty(tmp_path: Path, monkeypatch) -> None:
    app_settings = _build_app_settings(tmp_path, environment="paper", trade_enabled=True)
    strategy_settings = _build_strategy_settings()
    history_path = Path(app_settings.history_parquet)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    _write_history(history_path)
    session_factory = create_session_factory(app_settings.database_url)

    instrument_frame = pd.DataFrame(
        [
            {"symbol": "AAA.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Alpha", "total_capital_current": 1_000_000.0},
            {"symbol": "BBB.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Beta", "total_capital_current": 2_000_000.0},
        ]
    )
    capital_frame = pd.DataFrame(columns=["symbol", "effective_date", "total_capital", "circulating_capital", "free_float_capital"])
    submitted: list[dict[str, object]] = []

    monkeypatch.setattr(QmtMicrocapTradingEngine, "_refresh_history", lambda self: None)
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_instrument_frame",
        lambda self, symbols: instrument_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_capital_frame",
        lambda self, symbols: capital_frame.copy(),
    )
    snapshot_payload = {
        "account_id": "paper-account",
        "asset": {"cash": 100000.0, "frozen_cash": 0.0, "total_asset": 100000.0},
        "positions": [],
        "orders": [],
        "trades": [],
    }
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_account_snapshot",
        lambda self: snapshot_payload,
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_quotes",
        lambda self, symbols: {"AAA.SZ": {"last_price": 10.0, "volume": 2_000_000}},
    )

    def _fake_submit(self, symbol, side, qty, price, **kwargs):  # type: ignore[no-untyped-def]
        submitted.append({"symbol": symbol, "side": side, "qty": qty, "price": float(price)})
        return {"order_id": "delta-1"}

    monkeypatch.setattr("quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.submit_order", _fake_submit)

    engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
    plan_path, payload = engine.preview(Decimal("100000"))
    preview_qty = int(payload["preview_orders"][0]["qty"])
    snapshot_payload["orders"] = [
        {"stock_code": "AAA.SZ", "order_id": "qmt-1", "order_time": "2026-04-13 09:35:01", "order_volume": 40, "order_type": "buy"}
    ]
    report_path, metrics, equity_curve = engine.execute_plan(plan_path)

    assert report_path.exists()
    assert metrics.turnover > 0
    assert not equity_curve.empty
    assert len(submitted) == 1
    assert submitted[0]["symbol"] == "AAA.SZ"
    assert submitted[0]["side"] == "buy"
    assert submitted[0]["qty"] == preview_qty - 40


def test_qmt_microcap_trading_engine_retries_unfilled_order_after_timeout(tmp_path: Path, monkeypatch) -> None:
    app_settings = _build_app_settings(tmp_path, environment="paper", trade_enabled=True)
    strategy_settings = _build_strategy_settings()
    history_path = Path(app_settings.history_parquet)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    _write_history(history_path)
    session_factory = create_session_factory(app_settings.database_url)

    instrument_frame = pd.DataFrame(
        [
            {"symbol": "AAA.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Alpha", "total_capital_current": 1_000_000.0},
            {"symbol": "BBB.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Beta", "total_capital_current": 2_000_000.0},
        ]
    )
    capital_frame = pd.DataFrame(columns=["symbol", "effective_date", "total_capital", "circulating_capital", "free_float_capital"])
    submitted: list[int] = []
    wait_results = [
        {"status_available": True, "status_text": "未成", "status_code": 50, "filled_qty": 0, "is_active": True, "timed_out": True},
        {"status_available": True, "status_text": "已成", "status_code": 56, "filled_qty": 4900, "is_active": False, "timed_out": False},
    ]
    cancel_calls: list[str] = []

    monkeypatch.setattr(QmtMicrocapTradingEngine, "_refresh_history", lambda self: None)
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_instrument_frame",
        lambda self, symbols: instrument_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_capital_frame",
        lambda self, symbols: capital_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_account_snapshot",
        lambda self: {
            "account_id": "paper-account",
            "asset": {"cash": 100000.0, "frozen_cash": 0.0, "total_asset": 100000.0},
            "positions": [],
            "orders": [],
            "trades": [],
        },
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_quotes",
        lambda self, symbols: {"AAA.SZ": {"last_price": 10.0, "volume": 2_000_000}},
    )

    def _fake_submit(self, symbol, side, qty, price, **kwargs):  # type: ignore[no-untyped-def]
        submitted.append(qty)
        return {"order_id": f"retry-{len(submitted)}"}

    monkeypatch.setattr("quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.submit_order", _fake_submit)

    engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
    monkeypatch.setattr(engine, "_wait_for_order_resolution", lambda broker_order_id, submitted_qty: wait_results.pop(0))
    monkeypatch.setattr(
        engine,
        "_cancel_broker_order",
        lambda broker_order_id: cancel_calls.append(str(broker_order_id)) or 0,
    )

    plan_path, payload = engine.preview(Decimal("100000"))
    preview_qty = int(payload["preview_orders"][0]["qty"])
    wait_results[1]["filled_qty"] = preview_qty
    report_path, metrics, equity_curve = engine.execute_plan(plan_path)

    assert report_path.exists()
    assert metrics.turnover > 0
    assert not equity_curve.empty
    assert len(submitted) == 2
    assert submitted[0] == preview_qty
    assert submitted[1] == preview_qty
    assert cancel_calls == ["retry-1"]


def test_qmt_microcap_trading_engine_cancels_stale_snapshot_orders_before_resubmitting(tmp_path: Path, monkeypatch) -> None:
    app_settings = _build_app_settings(tmp_path, environment="paper", trade_enabled=True)
    strategy_settings = _build_strategy_settings()
    history_path = Path(app_settings.history_parquet)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    _write_history(history_path)
    session_factory = create_session_factory(app_settings.database_url)

    instrument_frame = pd.DataFrame(
        [
            {"symbol": "AAA.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Alpha", "total_capital_current": 1_000_000.0},
            {"symbol": "BBB.SZ", "open_date": pd.Timestamp("2010-01-01"), "instrument_name": "Beta", "total_capital_current": 2_000_000.0},
        ]
    )
    capital_frame = pd.DataFrame(columns=["symbol", "effective_date", "total_capital", "circulating_capital", "free_float_capital"])
    snapshot_payload = {
        "account_id": "paper-account",
        "asset": {"cash": 100000.0, "frozen_cash": 0.0, "total_asset": 100000.0},
        "positions": [],
        "orders": [],
        "trades": [],
    }
    submitted: list[int] = []
    cancelled: list[str] = []

    monkeypatch.setattr(QmtMicrocapTradingEngine, "_refresh_history", lambda self: None)
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_instrument_frame",
        lambda self, symbols: instrument_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.joinquant_microcap_engine.JoinQuantMicrocapBacktestEngine._load_capital_frame",
        lambda self, symbols: capital_frame.copy(),
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_account_snapshot",
        lambda self: snapshot_payload,
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.get_quotes",
        lambda self, symbols: {"AAA.SZ": {"last_price": 10.0, "volume": 2_000_000}},
    )
    monkeypatch.setattr(
        "quant_demo.experiment.qmt_microcap_trading.QmtBridgeClient.submit_order",
        lambda self, symbol, side, qty, price, **kwargs: submitted.append(qty) or {"order_id": "stale-resubmit-1"},
    )

    engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
    monkeypatch.setattr(engine, "_wait_for_order_resolution", lambda broker_order_id, submitted_qty: {"status_available": False, "status_text": "", "status_code": -1, "filled_qty": 0, "is_active": False, "timed_out": False})
    monkeypatch.setattr(engine, "_cancel_broker_order", lambda broker_order_id: cancelled.append(str(broker_order_id)) or 0)
    plan_path, payload = engine.preview(Decimal("100000"))
    preview_qty = int(payload["preview_orders"][0]["qty"])
    snapshot_payload["orders"] = [
        {
            "stock_code": "AAA.SZ",
            "order_id": "old-order-1",
            "order_time": pd.Timestamp("2026-04-13 09:00:00").timestamp(),
            "order_volume": preview_qty,
            "traded_volume": 0,
            "order_status": 50,
            "order_type": "buy",
        }
    ]

    report_path, metrics, equity_curve = engine.execute_plan(plan_path)

    assert report_path.exists()
    assert metrics.turnover > 0
    assert not equity_curve.empty
    assert cancelled == ["old-order-1"]
    assert submitted == [preview_qty]
