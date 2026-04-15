from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import delete, select
from sqlalchemy.orm import sessionmaker

from quant_demo.adapters.qmt.bridge_client import QmtBridgeClient
from quant_demo.audit.report_service import AuditReportService
from quant_demo.core.config import AppSettings, StrategySettings
from quant_demo.core.enums import Environment, IntentSource, OrderSide
from quant_demo.core.events import AccountState, OrderIntent, Position
from quant_demo.core.exceptions import QmtUnavailableError
from quant_demo.db.models import (
    AssetSnapshotModel,
    AuditLogModel,
    OrderEventModel,
    OrderIntentModel,
    OrderModel,
    PositionSnapshotModel,
    RiskDecisionModel,
    TradeModel,
)
from quant_demo.db.session import session_scope
from quant_demo.experiment.evaluator import EvaluationResult, Evaluator
from quant_demo.experiment.joinquant_microcap_engine import (
    JoinQuantMicrocapBacktestEngine,
    MicrocapStrategyConfig,
    adjust_target_amount_for_rules,
    available_trade_shares,
    build_target_portfolio,
    buy_fee,
    calc_amount_by_cash,
    calc_target_amount_by_value,
    calendar_hedge_ratio,
    can_trade,
    fit_target_count_by_cash,
    sell_fee,
    volume_units_to_shares,
)
from quant_demo.oms.service import OmsService
from quant_demo.risk.rules.cash_check import CashCheckRule
from quant_demo.risk.rules.daily_loss_limit import DailyLossLimitRule
from quant_demo.risk.rules.position_limit import PositionLimitRule
from quant_demo.risk.rules.trading_window import TradingWindowRule
from quant_demo.risk.service import RiskService


@dataclass(slots=True)
class PlannedOrder:
    symbol: str
    side: OrderSide
    qty: int
    price: Decimal
    reason: str
    metadata: dict[str, Any]


class QmtMicrocapTradingEngine:
    ORDER_RETRY_TIMEOUT_SECONDS = 60
    ORDER_RETRY_POLL_SECONDS = 5
    ORDER_RETRY_MAX_ATTEMPTS = 3

    def __init__(self, session_factory: sessionmaker, app_settings: AppSettings, strategy_settings: StrategySettings) -> None:
        self.session_factory = session_factory
        self.app_settings = app_settings
        self.strategy_settings = strategy_settings
        self.cfg = MicrocapStrategyConfig.from_strategy_settings(strategy_settings)
        self.bridge = QmtBridgeClient(app_settings)

    def _now_local(self) -> datetime:
        return datetime.now()

    def preview(self, initial_cash: Decimal) -> tuple[Path, dict[str, Any]]:
        context = self._prepare_trade_context(
            initial_cash,
            allow_snapshot_fallback=False,
            allow_quote_fallback=True,
            prefer_quote_fallback=True,
            require_realtime_positions=True,
        )
        plan_payload = self._build_plan_payload(
            latest_date=context["latest_date"],
            strategy_total_asset=context["strategy_total_asset"],
            target_meta=context["target_meta"],
            planned_orders=context["planned_orders"],
            live_state=context["live_state"],
        )
        plan_path = self._write_trade_plan(plan_payload)
        return plan_path, plan_payload

    def execute_plan(self, plan_path: str | Path) -> tuple[Path, EvaluationResult, pd.DataFrame]:
        payload = self._load_trade_plan(plan_path)
        if not self.app_settings.qmt_trade_enabled:
            raise QmtUnavailableError("paper 配置未开启 qmt_trade_enabled，当前仅允许生成预览计划")
        strategy_total_asset = Decimal(str(payload.get("strategy_total_asset") or "0"))
        expected_signal_date = str(payload.get("signal_trade_date") or "").strip()
        actual_signal_date = self._load_latest_history_date().date().isoformat()
        if expected_signal_date and expected_signal_date != actual_signal_date:
            raise RuntimeError(f"计划信号日为 {expected_signal_date}，当前可执行信号日为 {actual_signal_date}，请先重新生成计划")
        planned_orders = self._planned_orders_from_payload(payload)
        fallback_prices = {item.symbol: item.price for item in planned_orders}
        account_snapshot = self.bridge.get_account_snapshot()
        live_state, reported_total_asset = self._build_account_state(account_snapshot, fallback_prices)
        stale_cancellations = self._cancel_stale_open_orders(
            account_snapshot=account_snapshot,
            trade_date=str(payload.get("planned_execution_date") or "").strip(),
        )
        diff_summary = self._diff_plan_against_today_activity(
            account_snapshot=account_snapshot,
            planned_orders=planned_orders,
            planned_execution_date=str(payload.get("planned_execution_date") or "").strip(),
            stale_cancellations=stale_cancellations,
        )
        planned_orders = diff_summary["remaining_orders"]
        quote_symbols = sorted(set(fallback_prices) | set(live_state.positions))
        quotes = self._get_quotes_payload(quote_symbols, allow_quote_fallback=True)
        price_map, volume_map = self._build_quote_maps_from_quotes(quotes, fallback_prices)
        strategy_total_asset = self._resolve_strategy_total_asset(strategy_total_asset, reported_total_asset)
        strategy_total_asset = self._apply_strategy_capital(live_state, strategy_total_asset, price_map)
        if not planned_orders:
            equity_curve = self._build_equity_curve_snapshot(pd.Timestamp(actual_signal_date), live_state, price_map, turnover=Decimal("0"))
            metrics = Evaluator().evaluate(equity_curve)
            receipt_path = self._write_execution_receipt(payload, [], Path(plan_path), execution_summary=diff_summary)
            return receipt_path, metrics, equity_curve
        planning_state = self._clone_account_state(live_state)
        risk_service = RiskService(
            [
                CashCheckRule(),
                PositionLimitRule(self.app_settings.risk.max_position_ratio),
                TradingWindowRule(self.app_settings.environment, self.app_settings.risk.trading_start, self.app_settings.risk.trading_end),
                DailyLossLimitRule(self.app_settings.risk.daily_loss_limit),
            ]
        )
        target_meta = dict(payload.get("target_meta") or {})
        target_meta["strategy_total_asset"] = float(strategy_total_asset)
        equity_curve, report_path = self._submit_and_persist(
            latest_date=pd.Timestamp(actual_signal_date),
            account_snapshot=account_snapshot,
            live_state=live_state,
            planning_state=planning_state,
            planned_orders=planned_orders,
            prices=price_map,
            risk_service=risk_service,
            target_meta=target_meta,
            instrument_frame=pd.DataFrame(),
            strategy_total_asset=strategy_total_asset,
        )
        metrics = Evaluator().evaluate(equity_curve)
        self._write_execution_receipt(payload, planned_orders, report_path, execution_summary=diff_summary)
        return report_path, metrics, equity_curve

    def run(self, initial_cash: Decimal) -> tuple[Path, EvaluationResult, pd.DataFrame]:
        context = self._prepare_trade_context(initial_cash)
        equity_curve, report_path = self._submit_and_persist(
            latest_date=context["latest_date"],
            account_snapshot=context["account_snapshot"],
            live_state=context["live_state"],
            planning_state=context["planning_state"],
            planned_orders=context["planned_orders"],
            prices=context["price_map"],
            risk_service=context["risk_service"],
            target_meta=context["target_meta"],
            instrument_frame=context["instrument_frame"],
            strategy_total_asset=context["strategy_total_asset"],
        )
        metrics = Evaluator().evaluate(equity_curve)
        return report_path, metrics, equity_curve

        self._refresh_history()
        prepared, instrument_frame = self._load_prepared_history()
        if prepared.empty:
            raise RuntimeError("微盘交易引擎未找到可用历史数据")

        latest_date = pd.Timestamp(prepared["trading_date"].max()).normalize()
        day_frame = prepared[prepared["trading_date"] == latest_date].copy()
        if day_frame.empty:
            raise RuntimeError(f"微盘交易引擎未找到 {latest_date.date()} 的截面数据")
        day_frame = day_frame.sort_values(["market_cap_prev", "symbol"], ascending=[True, True]).set_index("symbol", drop=False)

        fallback_prices = {
            str(symbol): Decimal(str(float(row.get("close") or row.get("open") or 0.0)))
            for symbol, row in day_frame.iterrows()
        }
        account_snapshot = self.bridge.get_account_snapshot()
        live_state, reported_total_asset = self._build_account_state(account_snapshot, fallback_prices)
        strategy_total_asset = self._resolve_strategy_total_asset(initial_cash, reported_total_asset)
        strategy_total_asset = self._apply_strategy_capital(live_state, strategy_total_asset, fallback_prices)

        target_meta = self._build_target_metadata(latest_date, day_frame, live_state, strategy_total_asset)
        quote_symbols = sorted(set(target_meta["targets"]) | set(live_state.positions))
        quotes = self.bridge.get_quotes(quote_symbols) if quote_symbols else {}
        price_map, volume_map = self._build_quote_maps(day_frame, quotes)
        strategy_total_asset = self._apply_strategy_capital(live_state, strategy_total_asset, price_map)
        target_meta["strategy_total_asset"] = float(strategy_total_asset)
        planned_orders = self._build_order_plan(
            latest_date=latest_date,
            day_frame=day_frame,
            live_state=live_state,
            prices=price_map,
            volumes=volume_map,
            target_meta=target_meta,
        )

        planning_state = self._clone_account_state(live_state)
        risk_service = RiskService(
            [
                CashCheckRule(),
                PositionLimitRule(self.app_settings.risk.max_position_ratio),
                TradingWindowRule(self.app_settings.environment, self.app_settings.risk.trading_start, self.app_settings.risk.trading_end),
                DailyLossLimitRule(self.app_settings.risk.daily_loss_limit),
            ]
        )
        equity_curve, report_path = self._submit_and_persist(
            latest_date=latest_date,
            account_snapshot=account_snapshot,
            live_state=live_state,
            planning_state=planning_state,
            planned_orders=planned_orders,
            prices=price_map,
            risk_service=risk_service,
            target_meta=target_meta,
            instrument_frame=instrument_frame,
            strategy_total_asset=strategy_total_asset,
        )
        metrics = Evaluator().evaluate(equity_curve)
        return report_path, metrics, equity_curve

    def _prepare_trade_context(
        self,
        initial_cash: Decimal,
        planned_target_meta: dict[str, Any] | None = None,
        *,
        allow_snapshot_fallback: bool = False,
        allow_quote_fallback: bool = False,
        prefer_snapshot_fallback: bool = False,
        prefer_quote_fallback: bool = False,
        require_realtime_positions: bool = False,
    ) -> dict[str, Any]:
        self._refresh_history()
        prepared, instrument_frame = self._load_prepared_history()
        if prepared.empty:
            raise RuntimeError("微盘交易引擎未找到可用历史数据")

        latest_date = pd.Timestamp(prepared["trading_date"].max()).normalize()
        day_frame = prepared[prepared["trading_date"] == latest_date].copy()
        if day_frame.empty:
            raise RuntimeError(f"微盘交易引擎未找到 {latest_date.date()} 的截面数据")
        day_frame = day_frame.sort_values(["market_cap_prev", "symbol"], ascending=[True, True]).set_index("symbol", drop=False)

        fallback_prices = {
            str(symbol): Decimal(str(float(row.get("close") or row.get("open") or 0.0)))
            for symbol, row in day_frame.iterrows()
        }
        account_snapshot = self._get_account_snapshot_payload(
            fallback_prices,
            allow_snapshot_fallback=allow_snapshot_fallback,
            prefer_snapshot_fallback=prefer_snapshot_fallback,
            require_realtime_positions=require_realtime_positions,
        )
        live_state, reported_total_asset = self._build_account_state(account_snapshot, fallback_prices)
        strategy_total_asset = self._resolve_strategy_total_asset(initial_cash, reported_total_asset)
        strategy_total_asset = self._apply_strategy_capital(live_state, strategy_total_asset, fallback_prices)
        target_meta = self._resolve_target_metadata(
            latest_date=latest_date,
            day_frame=day_frame,
            live_state=live_state,
            strategy_total_asset=strategy_total_asset,
            planned_target_meta=planned_target_meta,
        )
        quote_symbols = sorted(set(target_meta["targets"]) | set(live_state.positions))
        quotes = self._get_quotes_payload(
            quote_symbols,
            allow_quote_fallback=allow_quote_fallback,
            prefer_quote_fallback=prefer_quote_fallback,
        )
        price_map, volume_map = self._build_quote_maps(day_frame, quotes)
        strategy_total_asset = self._apply_strategy_capital(live_state, strategy_total_asset, price_map)
        target_meta["strategy_total_asset"] = float(strategy_total_asset)
        planned_orders = self._build_order_plan(
            latest_date=latest_date,
            day_frame=day_frame,
            live_state=live_state,
            prices=price_map,
            volumes=volume_map,
            target_meta=target_meta,
        )
        planning_state = self._clone_account_state(live_state)
        risk_service = RiskService(
            [
                CashCheckRule(),
                PositionLimitRule(self.app_settings.risk.max_position_ratio),
                TradingWindowRule(self.app_settings.environment, self.app_settings.risk.trading_start, self.app_settings.risk.trading_end),
                DailyLossLimitRule(self.app_settings.risk.daily_loss_limit),
            ]
        )
        return {
            "latest_date": latest_date,
            "day_frame": day_frame,
            "account_snapshot": account_snapshot,
            "live_state": live_state,
            "strategy_total_asset": strategy_total_asset,
            "target_meta": target_meta,
            "price_map": price_map,
            "volume_map": volume_map,
            "planned_orders": planned_orders,
            "planning_state": planning_state,
            "risk_service": risk_service,
            "instrument_frame": instrument_frame,
        }

    def _resolve_target_metadata(
        self,
        latest_date: pd.Timestamp,
        day_frame: pd.DataFrame,
        live_state: AccountState,
        strategy_total_asset: Decimal,
        planned_target_meta: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if not planned_target_meta:
            return self._build_target_metadata(latest_date, day_frame, live_state, strategy_total_asset)
        targets = [str(symbol) for symbol in planned_target_meta.get("targets") or [] if str(symbol).strip()]
        if not targets:
            raise RuntimeError("计划文件缺少 targets，无法执行")
        hedge_ratio = float(planned_target_meta.get("hedge_ratio", 0.0) or 0.0)
        target_count = len(targets)
        invest_value = float(strategy_total_asset) * (1.0 - self.cfg.cash_buffer)
        stock_invest_value = invest_value * (1.0 - hedge_ratio)
        each_target_value = stock_invest_value / float(target_count) if target_count > 0 else 0.0
        return {
            "trade_date": latest_date.date().isoformat(),
            "targets": targets,
            "ranked_count": int(planned_target_meta.get("ranked_count", target_count) or target_count),
            "target_count": target_count,
            "hedge_ratio": hedge_ratio,
            "cash_reserve_ratio": round(hedge_ratio + self.cfg.cash_buffer, 4),
            "stock_invest_value": stock_invest_value,
            "each_target_value": each_target_value,
            "planned_execution_date": str(planned_target_meta.get("planned_execution_date") or "").strip(),
        }

    def _refresh_history(self) -> None:
        if self.app_settings.history_source.lower() != "qmt":
            return
        from quant_demo.marketdata.history_manager import refresh_history

        try:
            refresh_history(self.app_settings, mode="incremental")
        except QmtUnavailableError:
            if not Path(self.app_settings.history_parquet).exists():
                raise

    def _load_prepared_history(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        loader = JoinQuantMicrocapBacktestEngine(self.session_factory, self.app_settings, self.strategy_settings)
        history = self._load_recent_history_window()
        symbols = history["symbol"].dropna().astype(str).unique().tolist()
        instrument_frame = loader._load_instrument_frame(symbols)
        capital_frame = loader._load_capital_frame(symbols)
        prepared = loader._prepare_history(history, instrument_frame, capital_frame)
        return prepared, instrument_frame

    def _load_latest_history_date(self) -> pd.Timestamp:
        history_path = Path(self.app_settings.history_parquet)
        frame = pd.read_parquet(history_path, columns=["trading_date"])
        frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.normalize()
        start_dt = pd.Timestamp(self.app_settings.history_start or "2020-01-01").normalize()
        if self.app_settings.history_end:
            end_dt = pd.Timestamp(self.app_settings.history_end).normalize()
            frame = frame[(frame["trading_date"] >= start_dt) & (frame["trading_date"] <= end_dt)].copy()
        else:
            frame = frame[frame["trading_date"] >= start_dt].copy()
        if frame.empty:
            raise RuntimeError("微盘交易引擎未找到可用历史数据")
        trading_dates = sorted(pd.Timestamp(item).normalize() for item in frame["trading_date"].dropna().unique())
        return self._resolve_completed_history_date(trading_dates)

    def _load_recent_history_window(self, window_days: int = 40) -> pd.DataFrame:
        history_path = Path(self.app_settings.history_parquet)
        frame = pd.read_parquet(history_path, columns=["trading_date", "symbol", "open", "close", "volume", "amount"])
        frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.normalize()
        start_dt = pd.Timestamp(self.app_settings.history_start or "2020-01-01").normalize()
        if self.app_settings.history_end:
            end_dt = pd.Timestamp(self.app_settings.history_end).normalize()
            frame = frame[(frame["trading_date"] >= start_dt) & (frame["trading_date"] <= end_dt)].copy()
        else:
            frame = frame[frame["trading_date"] >= start_dt].copy()
        if frame.empty:
            return frame
        trading_dates = sorted(pd.Timestamp(item).normalize() for item in frame["trading_date"].dropna().unique())
        latest_completed = self._resolve_completed_history_date(trading_dates)
        completed_dates = [item for item in trading_dates if item <= latest_completed]
        keep_dates = set(completed_dates[-window_days:])
        frame = frame[frame["trading_date"].isin(keep_dates)].copy()
        frame["symbol"] = frame["symbol"].astype(str)
        return frame.sort_values(["symbol", "trading_date"]).reset_index(drop=True)

    def _resolve_completed_history_date(self, trading_dates: list[pd.Timestamp]) -> pd.Timestamp:
        if not trading_dates:
            raise RuntimeError("微盘交易引擎未找到可用历史数据")
        latest = pd.Timestamp(trading_dates[-1]).normalize()
        now = pd.Timestamp(self._now_local()).tz_localize(None)
        close_cutoff = pd.Timestamp(now.date()).replace(hour=15, minute=5, second=0, microsecond=0)
        if latest.date() == now.date() and now < close_cutoff and len(trading_dates) >= 2:
            return pd.Timestamp(trading_dates[-2]).normalize()
        return latest

    def _get_account_snapshot_payload(
        self,
        fallback_prices: dict[str, Decimal],
        *,
        allow_snapshot_fallback: bool,
        prefer_snapshot_fallback: bool = False,
        require_realtime_positions: bool = False,
    ) -> dict[str, Any]:
        if prefer_snapshot_fallback:
            payload = self._load_latest_snapshot_from_db(fallback_prices)
            if payload is not None:
                if require_realtime_positions:
                    raise QmtUnavailableError("生成预览计划必须读取 QMT 实时持仓，禁止使用数据库快照回退")
                return payload
        try:
            payload = self.bridge.get_account_snapshot()
        except QmtUnavailableError as exc:
            if not allow_snapshot_fallback:
                if require_realtime_positions:
                    raise QmtUnavailableError("QMT 实时持仓不可用，无法生成预览计划") from exc
                raise
        else:
            if require_realtime_positions:
                positions = payload.get("positions")
                if not isinstance(positions, list):
                    raise QmtUnavailableError("QMT 实时持仓不可用，无法生成预览计划")
            return payload
        payload = self._load_latest_snapshot_from_db(fallback_prices)
        if payload is None:
            raise QmtUnavailableError("QMT 账户快照不可用，且正式库中没有可回退的资产快照")
        if require_realtime_positions:
            raise QmtUnavailableError("生成预览计划必须读取 QMT 实时持仓，禁止使用数据库快照回退")
        return payload

    def _get_quotes_payload(
        self,
        symbols: list[str],
        *,
        allow_quote_fallback: bool,
        prefer_quote_fallback: bool = False,
    ) -> dict[str, Any]:
        if not symbols:
            return {}
        if prefer_quote_fallback:
            return {}
        try:
            return self.bridge.get_quotes(symbols)
        except QmtUnavailableError:
            if allow_quote_fallback:
                return {}
            raise

    def _load_latest_snapshot_from_db(self, fallback_prices: dict[str, Decimal]) -> dict[str, Any] | None:
        account_id = str(self.app_settings.qmt_account_id or "").strip()
        with session_scope(self.session_factory) as session:
            snapshot_stmt = select(AssetSnapshotModel).order_by(AssetSnapshotModel.snapshot_time.desc())
            if account_id:
                snapshot_stmt = snapshot_stmt.where(AssetSnapshotModel.account_id == account_id)
            asset_snapshot = session.scalars(snapshot_stmt.limit(1)).first()
            if asset_snapshot is None:
                return None

            snapshot_time = asset_snapshot.snapshot_time
            position_stmt = select(PositionSnapshotModel).where(PositionSnapshotModel.snapshot_time == snapshot_time)
            if account_id:
                position_stmt = position_stmt.where(PositionSnapshotModel.account_id == asset_snapshot.account_id)
            position_rows = list(session.scalars(position_stmt.order_by(PositionSnapshotModel.symbol)))

        positions: list[dict[str, Any]] = []
        for row in position_rows:
            market_price = fallback_prices.get(row.symbol, Decimal(str(row.market_price)))
            positions.append(
                {
                    "symbol": row.symbol,
                    "qty": int(row.qty),
                    "available_qty": int(row.available_qty),
                    "cost_price": float(row.cost_price),
                    "market_price": float(market_price),
                }
            )

        return {
            "account_id": asset_snapshot.account_id,
            "asset": {
                "cash": float(asset_snapshot.cash),
                "frozen_cash": float(asset_snapshot.frozen_cash),
                "total_asset": float(asset_snapshot.total_asset),
            },
            "positions": positions,
            "orders": [],
            "trades": [],
            "source": "db_snapshot_fallback",
            "snapshot_time": snapshot_time.isoformat(),
        }

    def _build_target_metadata(
        self,
        latest_date: pd.Timestamp,
        day_frame: pd.DataFrame,
        live_state: AccountState,
        reported_total_asset: Decimal,
    ) -> dict[str, Any]:
        stock_holdings = list(live_state.positions)
        total_value = float(reported_total_asset)
        hedge_ratio = calendar_hedge_ratio(latest_date, self.cfg)
        invest_value = total_value * (1.0 - self.cfg.cash_buffer)
        stock_invest_value = invest_value * (1.0 - hedge_ratio)
        target_stocks, ranked_count = build_target_portfolio(
            day_frame.reset_index(drop=True),
            stock_holdings,
            total_value * (1.0 - hedge_ratio),
            self.cfg,
        )
        price_lookup = {str(symbol): float(row.get("open") or 0.0) for symbol, row in day_frame.iterrows()}
        fitted_targets = fit_target_count_by_cash(
            target_stocks,
            price_lookup=price_lookup,
            invest_value=stock_invest_value,
            cfg=self.cfg,
        )
        target_count = len(fitted_targets)
        each_target_value = stock_invest_value / float(target_count) if target_count > 0 else 0.0
        return {
            "trade_date": latest_date.date().isoformat(),
            "targets": fitted_targets,
            "ranked_count": ranked_count,
            "target_count": target_count,
            "hedge_ratio": hedge_ratio,
            "cash_reserve_ratio": round(hedge_ratio + self.cfg.cash_buffer, 4),
            "stock_invest_value": stock_invest_value,
            "each_target_value": each_target_value,
        }

    def _build_plan_payload(
        self,
        latest_date: pd.Timestamp,
        strategy_total_asset: Decimal,
        target_meta: dict[str, Any],
        planned_orders: list[PlannedOrder],
        live_state: AccountState,
    ) -> dict[str, Any]:
        signal_trade_date = latest_date.date().isoformat()
        planned_execution_date = str((latest_date + pd.offsets.BDay(1)).date())
        return {
            "strategy": self.strategy_settings.name,
            "generated_at": datetime.now().isoformat(),
            "signal_trade_date": signal_trade_date,
            "planned_execution_date": planned_execution_date,
            "account_id": live_state.account_id,
            "strategy_total_asset": float(strategy_total_asset),
            "target_meta": {
                "trade_date": signal_trade_date,
                "planned_execution_date": planned_execution_date,
                "targets": list(target_meta.get("targets") or []),
                "ranked_count": int(target_meta.get("ranked_count", 0) or 0),
                "target_count": int(target_meta.get("target_count", 0) or 0),
                "hedge_ratio": float(target_meta.get("hedge_ratio", 0.0) or 0.0),
                "cash_reserve_ratio": float(target_meta.get("cash_reserve_ratio", 0.0) or 0.0),
                "stock_invest_value": float(target_meta.get("stock_invest_value", 0.0) or 0.0),
                "each_target_value": float(target_meta.get("each_target_value", 0.0) or 0.0),
            },
            "preview_orders": [
                {
                    "symbol": item.symbol,
                    "side": item.side.value,
                    "qty": item.qty,
                    "price": float(item.price),
                    "reason": item.reason,
                    "metadata": item.metadata,
                }
                for item in planned_orders
            ],
            "current_positions": [
                {
                    "symbol": item.symbol,
                    "qty": item.qty,
                    "available_qty": item.available_qty,
                    "cost_price": float(item.cost_price),
                    "last_price": float(item.last_price),
                }
                for item in live_state.positions.values()
            ],
        }

    def _trade_plan_dir(self) -> Path:
        return Path(self.app_settings.report_dir) / "trade_plans"

    def _write_trade_plan(self, payload: dict[str, Any]) -> Path:
        plan_dir = self._trade_plan_dir()
        plan_dir.mkdir(parents=True, exist_ok=True)
        signal_date = str(payload.get("signal_trade_date") or "").replace("-", "")
        execution_date = str(payload.get("planned_execution_date") or "").replace("-", "")
        dated_path = plan_dir / f"microcap_t1_plan_{signal_date}_for_{execution_date}.json"
        latest_path = plan_dir / "microcap_t1_plan_latest.json"
        content = json.dumps(payload, ensure_ascii=False, indent=2)
        dated_path.write_text(content, encoding="utf-8")
        latest_path.write_text(content, encoding="utf-8")
        return dated_path

    def _load_trade_plan(self, plan_path: str | Path) -> dict[str, Any]:
        path = Path(plan_path)
        if not path.is_absolute():
            path = (Path.cwd() / path).resolve()
        if not path.exists():
            latest_path = self._trade_plan_dir() / "microcap_t1_plan_latest.json"
            if latest_path.exists():
                path = latest_path
            else:
                raise FileNotFoundError(f"未找到计划文件: {path}")
        return json.loads(path.read_text(encoding="utf-8"))

    def _write_execution_receipt(
        self,
        plan_payload: dict[str, Any],
        planned_orders: list[PlannedOrder],
        report_path: Path,
        *,
        execution_summary: dict[str, Any] | None = None,
    ) -> Path:
        receipt = {
            "strategy": self.strategy_settings.name,
            "executed_at": datetime.now().isoformat(),
            "signal_trade_date": plan_payload.get("signal_trade_date"),
            "planned_execution_date": plan_payload.get("planned_execution_date"),
            "strategy_total_asset": plan_payload.get("strategy_total_asset"),
            "report_path": str(report_path),
            "execution_summary": self._serialize_execution_summary(execution_summary or {}),
            "submitted_orders": [
                {
                    "symbol": item.symbol,
                    "side": item.side.value,
                    "qty": item.qty,
                    "price": float(item.price),
                    "reason": item.reason,
                }
                for item in planned_orders
            ],
        }
        receipt_dir = self._trade_plan_dir()
        receipt_dir.mkdir(parents=True, exist_ok=True)
        signal_date = str(plan_payload.get("signal_trade_date") or "").replace("-", "")
        execution_date = str(plan_payload.get("planned_execution_date") or "").replace("-", "")
        receipt_path = receipt_dir / f"microcap_t1_execution_{signal_date}_for_{execution_date}.json"
        receipt_path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2), encoding="utf-8")
        return receipt_path

    def _planned_orders_from_payload(self, payload: dict[str, Any]) -> list[PlannedOrder]:
        rows = payload.get("preview_orders") or []
        planned_orders: list[PlannedOrder] = []
        for item in rows:
            symbol = str(item.get("symbol") or "").strip()
            side = str(item.get("side") or "").strip().lower()
            qty = int(item.get("qty") or 0)
            price = Decimal(str(item.get("price") or 0))
            if not symbol or side not in {OrderSide.BUY.value, OrderSide.SELL.value} or qty <= 0 or price <= 0:
                continue
            planned_orders.append(
                PlannedOrder(
                    symbol=symbol,
                    side=OrderSide(side),
                    qty=qty,
                    price=price,
                    reason=str(item.get("reason") or "rebalance_buy"),
                    metadata=dict(item.get("metadata") or {}),
                )
            )
        if not planned_orders:
            raise RuntimeError("计划文件中没有可执行委托")
        return planned_orders

    def _diff_plan_against_today_activity(
        self,
        *,
        account_snapshot: dict[str, Any],
        planned_orders: list[PlannedOrder],
        planned_execution_date: str,
        stale_cancellations: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        activity_entries = self._extract_today_activity(account_snapshot, planned_execution_date)
        activity_qty: dict[tuple[str, str], int] = {}
        for item in activity_entries:
            key = (item["symbol"], item["side"])
            activity_qty[key] = activity_qty.get(key, 0) + int(item["qty"])

        remaining_orders: list[PlannedOrder] = []
        differences: list[dict[str, Any]] = []
        for order in planned_orders:
            key = (order.symbol, order.side.value)
            existing_qty = int(activity_qty.get(key, 0))
            delta_qty = max(order.qty - existing_qty, 0)
            differences.append(
                {
                    "symbol": order.symbol,
                    "side": order.side.value,
                    "planned_qty": order.qty,
                    "existing_qty": existing_qty,
                    "remaining_qty": delta_qty,
                    "reason": order.reason,
                }
            )
            if delta_qty <= 0:
                continue
            remaining_orders.append(
                PlannedOrder(
                    symbol=order.symbol,
                    side=order.side,
                    qty=delta_qty,
                    price=order.price,
                    reason=order.reason,
                    metadata=dict(order.metadata),
                )
            )

        return {
            "planned_order_count": len(planned_orders),
            "existing_activity_count": len(activity_entries),
            "remaining_order_count": len(remaining_orders),
            "has_difference": any(int(item["remaining_qty"]) > 0 for item in differences),
            "differences": differences,
            "existing_activity": activity_entries,
            "stale_cancellations": stale_cancellations or [],
            "remaining_orders": remaining_orders,
        }

    def _extract_today_activity(self, account_snapshot: dict[str, Any], trade_date: str) -> list[dict[str, Any]]:
        target_date = pd.to_datetime(trade_date, errors="coerce")
        normalized: dict[str, dict[str, Any]] = {}

        def _match_trade_date(raw_value: Any) -> bool:
            if pd.isna(target_date):
                return True
            occurred = pd.to_datetime(raw_value, errors="coerce")
            if pd.isna(occurred):
                return True
            return occurred.date() == target_date.date()

        trades = account_snapshot.get("trades", []) or []
        for item in trades:
            side = self._normalize_activity_side(
                self._pick_text(item, "side", "order_side", "direction", "entrust_bs", "bs_flag", "operation")
                or item.get("order_type")
                or item.get("m_nOrderType")
            )
            symbol = self._pick_text(item, "stock_code", "symbol", "ticker", "instrument_id")
            occurred_at = self._pick_text(item, "traded_time", "trade_time", "business_time", "成交时间", default="")
            qty = int(round(self._pick_number(item, "traded_volume", "fill_qty", "volume", "business_amount", "qty")))
            if side not in {"buy", "sell"} or not symbol or qty <= 0 or not _match_trade_date(occurred_at):
                continue
            broker_order_id = self._pick_text(item, "order_id", "order_sysid", "entrust_no", default="")
            entry_key = f"trade::{broker_order_id or symbol + ':' + side}"
            normalized[entry_key] = {
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "source": "trade",
                "broker_order_id": broker_order_id,
                "occurred_at": occurred_at,
            }

        orders = account_snapshot.get("orders", []) or []
        for item in orders:
            side = self._normalize_activity_side(
                self._pick_text(item, "side", "order_side", "direction", "entrust_bs", "bs_flag", "operation")
                or item.get("order_type")
                or item.get("m_nOrderType")
            )
            symbol = self._pick_text(item, "stock_code", "symbol", "ticker", "instrument_id")
            occurred_at = self._pick_text(item, "order_time", "created_at", "entrust_time", default="")
            status_code = self._coerce_int(item.get("order_status", item.get("status")))
            traded_qty = int(round(self._pick_number(item, "traded_volume", "filled_qty", "volume", default=0.0)))
            order_qty = int(round(self._pick_number(item, "order_volume", "qty", "volume", default=0.0)))
            qty = traded_qty
            if qty <= 0 and not self._is_active_order_status(status_code):
                qty = order_qty
            if qty <= 0 and self._is_active_order_status(status_code) and not self._is_order_stale(item):
                qty = order_qty
            if side not in {"buy", "sell"} or not symbol or qty <= 0 or not _match_trade_date(occurred_at):
                continue
            broker_order_id = self._pick_text(item, "order_id", "order_sysid", "entrust_no", default="")
            entry_key = f"trade::{broker_order_id}" if broker_order_id else f"order::{symbol}:{side}:{occurred_at}:{qty}"
            current = normalized.get(entry_key)
            if current is None or qty > int(current.get("qty") or 0):
                normalized[entry_key] = {
                    "symbol": symbol,
                    "side": side,
                    "qty": qty,
                    "source": "order",
                    "broker_order_id": broker_order_id,
                    "occurred_at": occurred_at,
                }

        return list(normalized.values())

    def _cancel_stale_open_orders(self, account_snapshot: dict[str, Any], trade_date: str) -> list[dict[str, Any]]:
        cancellations: list[dict[str, Any]] = []
        target_trade_date = pd.to_datetime(trade_date, errors="coerce")
        for item in account_snapshot.get("orders", []) or []:
            status_code = self._coerce_int(item.get("order_status", item.get("status")))
            if not self._is_active_order_status(status_code):
                continue
            occurred_at = self._pick_text(item, "order_time", "created_at", "entrust_time", default="")
            occurred_day = pd.to_datetime(occurred_at, errors="coerce")
            if pd.notna(target_trade_date) and pd.notna(occurred_day) and occurred_day.date() != target_trade_date.date():
                continue
            if not self._is_order_stale(item):
                continue
            broker_order_id = self._pick_text(item, "order_id", "order_sysid", "entrust_no", default="")
            cancel_result = self._cancel_broker_order(broker_order_id)
            cancellations.append(
                {
                    "symbol": self._pick_text(item, "stock_code", "symbol", "ticker", "instrument_id"),
                    "broker_order_id": broker_order_id,
                    "order_status": status_code,
                    "cancel_result": cancel_result,
                    "traded_volume": int(round(self._pick_number(item, "traded_volume", "filled_qty", default=0.0))),
                }
            )
        return cancellations

    def _serialize_execution_summary(self, payload: dict[str, Any]) -> dict[str, Any]:
        serialized = dict(payload)
        if "remaining_orders" in serialized:
            serialized["remaining_orders"] = [
                {
                    "symbol": item.symbol,
                    "side": item.side.value,
                    "qty": item.qty,
                    "price": float(item.price),
                    "reason": item.reason,
                    "metadata": item.metadata,
                }
                for item in serialized.get("remaining_orders") or []
            ]
        return serialized

    def _build_quote_maps_from_quotes(
        self,
        quotes: dict[str, Any],
        fallback_prices: dict[str, Decimal],
    ) -> tuple[dict[str, Decimal], dict[str, float]]:
        prices: dict[str, Decimal] = {}
        volumes: dict[str, float] = {}
        symbols = sorted(set(fallback_prices) | set(quotes))
        for symbol in symbols:
            quote = quotes.get(symbol, {}) if isinstance(quotes, dict) else {}
            last_price = self._pick_number(quote, "last_price", default=float(fallback_prices.get(symbol, Decimal("0"))))
            if last_price <= 0:
                last_price = self._resolve_quote_price(quote, is_buy=True)
            if last_price <= 0:
                last_price = float(fallback_prices.get(symbol, Decimal("0.01")))
            prices[symbol] = Decimal(str(round(max(last_price, 0.01), 4)))
            volume = self._pick_number(quote, "volume", default=0.0)
            volumes[symbol] = volume
        return prices, volumes

    def _build_quote_maps(
        self,
        day_frame: pd.DataFrame,
        quotes: dict[str, Any],
    ) -> tuple[dict[str, Decimal], dict[str, float]]:
        prices: dict[str, Decimal] = {}
        volumes: dict[str, float] = {}
        for symbol, row in day_frame.iterrows():
            quote = quotes.get(symbol, {}) if isinstance(quotes, dict) else {}
            last_price = self._pick_number(quote, "last_price", default=0.0)
            if last_price <= 0:
                last_price = self._resolve_quote_price(quote, is_buy=True)
            if last_price <= 0:
                last_price = float(row.get("open") or row.get("close") or 0.0)
            prices[str(symbol)] = Decimal(str(round(max(last_price, 0.01), 4)))
            quote_volume = self._pick_number(quote, "volume")
            if quote_volume <= 0:
                quote_volume = float(row.get("volume") or 0.0)
            volumes[str(symbol)] = quote_volume
        return prices, volumes

    def _build_order_plan(
        self,
        latest_date: pd.Timestamp,
        day_frame: pd.DataFrame,
        live_state: AccountState,
        prices: dict[str, Decimal],
        volumes: dict[str, float],
        target_meta: dict[str, Any],
    ) -> list[PlannedOrder]:
        remaining_trade_capacity = {
            str(symbol): int(volume_units_to_shares(float(row.get("avg_volume_20_prev", 0.0) or 0.0)) * self.cfg.max_trade_volume_ratio)
            for symbol, row in day_frame.iterrows()
        }
        target_set = set(target_meta["targets"])
        each_target_value = float(target_meta["each_target_value"])
        planning_state = self._clone_account_state(live_state)
        planned_orders: list[PlannedOrder] = []

        for symbol in list(planning_state.positions):
            if symbol in target_set or symbol not in day_frame.index:
                continue
            position = planning_state.positions[symbol]
            sell_qty = available_trade_shares(
                symbol,
                desired_shares=position.available_qty,
                remaining_shares=remaining_trade_capacity.get(symbol, 0),
                current_amount=position.qty,
                is_buy=False,
            )
            if sell_qty <= 0:
                continue
            row = day_frame.loc[symbol]
            sell_price = prices[symbol]
            if not can_trade(symbol, latest_date, float(sell_price), volumes.get(symbol, 0.0), float(row.get("prev_close") or 0.0), is_buy=False):
                continue
            order = PlannedOrder(
                symbol=symbol,
                side=OrderSide.SELL,
                qty=sell_qty,
                price=sell_price,
                reason="not_in_target",
                metadata={"target_symbols": target_meta["targets"]},
            )
            planned_orders.append(order)
            self._apply_planned_order(planning_state, order)
            remaining_trade_capacity[symbol] = max(0, remaining_trade_capacity.get(symbol, 0) - sell_qty)

        for symbol in target_meta["targets"]:
            if symbol not in planning_state.positions or symbol not in day_frame.index:
                continue
            position = planning_state.positions[symbol]
            row = day_frame.loc[symbol]
            current_value = float(position.qty) * float(prices[symbol])
            if current_value <= each_target_value * self.cfg.max_overweight_ratio:
                continue
            target_amount = calc_target_amount_by_value(symbol, each_target_value, float(prices[symbol]))
            adjusted_target = adjust_target_amount_for_rules(symbol, position.qty, target_amount)
            desired_sell = min(position.available_qty, max(position.qty - adjusted_target, 0))
            sell_qty = available_trade_shares(
                symbol,
                desired_shares=desired_sell,
                remaining_shares=remaining_trade_capacity.get(symbol, 0),
                current_amount=position.qty,
                is_buy=False,
            )
            if sell_qty <= 0:
                continue
            sell_price = prices[symbol]
            if not can_trade(symbol, latest_date, float(sell_price), volumes.get(symbol, 0.0), float(row.get("prev_close") or 0.0), is_buy=False):
                continue
            order = PlannedOrder(
                symbol=symbol,
                side=OrderSide.SELL,
                qty=sell_qty,
                price=sell_price,
                reason="overweight_trim",
                metadata={"target_value": round(each_target_value, 2)},
            )
            planned_orders.append(order)
            self._apply_planned_order(planning_state, order)
            remaining_trade_capacity[symbol] = max(0, remaining_trade_capacity.get(symbol, 0) - sell_qty)

        buy_plan: list[tuple[str, float]] = []
        for symbol in target_meta["targets"]:
            if symbol not in day_frame.index:
                continue
            row = day_frame.loc[symbol]
            buy_price = prices[symbol]
            if not can_trade(symbol, latest_date, float(buy_price), volumes.get(symbol, 0.0), float(row.get("prev_close") or 0.0), is_buy=True):
                continue
            current_value = 0.0
            if symbol in planning_state.positions:
                current_value = float(planning_state.positions[symbol].qty) * float(buy_price)
            gap_value = each_target_value - current_value
            if gap_value > 0:
                buy_plan.append((symbol, gap_value))
        buy_plan.sort(key=lambda item: item[1], reverse=True)

        for index, (symbol, gap_value) in enumerate(buy_plan):
            remaining = len(buy_plan) - index
            if remaining <= 0:
                continue
            buy_price = prices[symbol]
            budget = min(gap_value, float(planning_state.cash) / float(remaining) * 0.98)
            desired_buy = calc_amount_by_cash(symbol, budget, float(buy_price))
            buy_qty = available_trade_shares(
                symbol,
                desired_shares=desired_buy,
                remaining_shares=remaining_trade_capacity.get(symbol, 0),
                current_amount=planning_state.positions.get(symbol).qty if symbol in planning_state.positions else 0,
                is_buy=True,
            )
            if buy_qty <= 0:
                continue
            total_cash = Decimal(str(buy_qty)) * buy_price + Decimal(str(buy_fee(float(Decimal(str(buy_qty)) * buy_price))))
            if total_cash > planning_state.cash:
                continue
            order = PlannedOrder(
                symbol=symbol,
                side=OrderSide.BUY,
                qty=buy_qty,
                price=buy_price,
                reason="rebalance_buy",
                metadata={"budget": round(budget, 2)},
            )
            planned_orders.append(order)
            self._apply_planned_order(planning_state, order)
            remaining_trade_capacity[symbol] = max(0, remaining_trade_capacity.get(symbol, 0) - buy_qty)

        return planned_orders

    def _submit_and_persist(
        self,
        latest_date: pd.Timestamp,
        account_snapshot: dict[str, Any],
        live_state: AccountState,
        planning_state: AccountState,
        planned_orders: list[PlannedOrder],
        prices: dict[str, Decimal],
        risk_service: RiskService,
        target_meta: dict[str, Any],
        instrument_frame: pd.DataFrame,
        strategy_total_asset: Decimal,
    ) -> tuple[pd.DataFrame, Path]:
        instrument_names = (
            instrument_frame.drop_duplicates("symbol", keep="last").set_index("symbol")["instrument_name"].fillna("").to_dict()
            if not instrument_frame.empty
            else {}
        )
        snapshot_time = datetime.now()
        qmt_enabled = bool(self.app_settings.qmt_trade_enabled)
        turnover = Decimal("0")

        with session_scope(self.session_factory) as session:
            self._reset_non_live_state(session)
            oms_service = OmsService(session)
            for planned in planned_orders:
                intent = OrderIntent(
                    account_id=planning_state.account_id,
                    trading_date=latest_date.date(),
                    symbol=planned.symbol,
                    side=planned.side,
                    qty=planned.qty,
                    reference_price=planned.price,
                    limit_price=planned.price,
                    source=IntentSource.STRATEGY,
                    metadata={"reason": planned.reason, **planned.metadata},
                )
                decision = risk_service.evaluate(intent, planning_state, prices)
                oms_service.register_intent(intent, decision)
                session.add(
                    AuditLogModel(
                        object_type="risk_decision",
                        object_id=decision.risk_decision_id,
                        message=f"{planned.symbol} 椋庢帶{'閫氳繃' if decision.is_approved() else '鎷掔粷'}",
                        payload={"reason": planned.reason, "rules": [asdict(result) for result in decision.rule_results]},
                    )
                )
                if not decision.is_approved():
                    continue
                order = oms_service.create_order(intent, decision)
                if qmt_enabled:
                    submission_result = self._submit_order_with_retry(planned, prices, session)
                    broker_order_id = submission_result["broker_order_id"]
                    order.broker_order_id = broker_order_id or None
                    model = session.get(OrderModel, order.order_id)
                    if model is not None:
                        model.broker_order_id = order.broker_order_id
                    oms_service.submit_order(order)
                    session.add(
                        OrderEventModel(
                            order_id=order.order_id,
                            status=order.status.value,
                            source="broker",
                            payload={"broker_order_id": broker_order_id, "reason": planned.reason},
                        )
                    )
                    session.add(
                        AuditLogModel(
                            object_type="order",
                            object_id=order.order_id,
                            message=f"{planned.symbol} 宸叉彁浜?QMT 濮旀墭",
                            payload={
                                "broker_order_id": broker_order_id,
                                "side": planned.side.value,
                                "qty": planned.qty,
                                "price": float(planned.price),
                                "retry_attempts": submission_result["attempts"],
                                "remaining_qty": submission_result["remaining_qty"],
                            },
                        )
                    )
                else:
                    session.add(
                        OrderEventModel(
                            order_id=order.order_id,
                            status=order.status.value,
                            source="preview",
                            payload={"reason": planned.reason},
                        )
                    )
                    session.add(
                        AuditLogModel(
                            object_type="order_preview",
                            object_id=order.order_id,
                            message=f"{planned.symbol} 鐢熸垚棰勬紨濮旀墭锛屾湭鎻愪氦 QMT",
                            payload={"side": planned.side.value, "qty": planned.qty, "price": float(planned.price)},
                        )
                    )
                turnover += Decimal(str(planned.qty)) * planned.price
                self._apply_planned_order(planning_state, planned)

            snapshot_payload = account_snapshot
            snapshot_source = "input_snapshot"
            if qmt_enabled:
                try:
                    snapshot_payload = self.bridge.get_account_snapshot()
                    snapshot_source = "qmt_account"
                except QmtUnavailableError as exc:
                    snapshot_payload = self._build_snapshot_payload_from_state(planning_state, prices)
                    snapshot_source = "planned_state_fallback"
                    session.add(
                        AuditLogModel(
                            object_type="account_snapshot",
                            object_id=latest_date.date().isoformat(),
                            message="QMT 账户快照超时，已回退为本地计划持仓快照",
                            payload={"error": str(exc), "source": snapshot_source},
                        )
                    )
            asset_row, position_rows = self._build_snapshot_rows(snapshot_payload, prices, strategy_total_asset)
            if asset_row is not None:
                session.add(
                    AssetSnapshotModel(
                        account_id=asset_row["account_id"],
                        cash=asset_row["cash"],
                        frozen_cash=asset_row["frozen_cash"],
                        total_asset=asset_row["total_asset"],
                        total_pnl=Decimal("0"),
                        turnover=turnover,
                        max_drawdown=Decimal("0"),
                        snapshot_time=snapshot_time,
                    )
                )
            for item in position_rows:
                session.add(
                    PositionSnapshotModel(
                        account_id=item["account_id"],
                        symbol=item["symbol"],
                        qty=item["qty"],
                        available_qty=item["available_qty"],
                        cost_price=item["cost_price"],
                        market_price=item["market_price"],
                        snapshot_time=snapshot_time,
                    )
                )
            session.add(
                AuditLogModel(
                    object_type="microcap_trade_plan",
                    object_id=latest_date.date().isoformat(),
                    message="QMT 微盘交易计划已生成",
                    payload={
                        "trade_date": latest_date.date().isoformat(),
                        "targets": target_meta["targets"],
                        "ranked_count": target_meta["ranked_count"],
                        "target_count": target_meta["target_count"],
                        "hedge_ratio": target_meta["hedge_ratio"],
                        "cash_reserve_ratio": target_meta["cash_reserve_ratio"],
                        "strategy_total_asset": float(strategy_total_asset),
                        "qmt_trade_enabled": qmt_enabled,
                        "snapshot_source": snapshot_source,
                        "planned_orders": [
                            {
                                "symbol": item.symbol,
                                "side": item.side.value,
                                "qty": item.qty,
                                "price": float(item.price),
                                "reason": item.reason,
                                "instrument_name": instrument_names.get(item.symbol, item.symbol),
                            }
                            for item in planned_orders
                        ],
                    },
                )
            )
            report_path = AuditReportService().write_daily_report(session, self.app_settings.report_dir)

        total_asset = asset_row["total_asset"] if asset_row is not None else live_state.total_asset(prices)
        cash = asset_row["cash"] if asset_row is not None else live_state.cash
        equity_curve = pd.DataFrame(
            [
                {
                    "trading_date": latest_date.date(),
                    "equity": float(total_asset),
                    "cash": float(cash),
                    "turnover": float(turnover),
                }
            ]
        )
        return equity_curve, report_path

    def _submit_order_with_retry(
        self,
        planned: PlannedOrder,
        prices: dict[str, Decimal],
        session,
    ) -> dict[str, Any]:
        remaining_qty = int(planned.qty)
        attempts = 0
        broker_order_id = ""
        while remaining_qty > 0 and attempts < self.ORDER_RETRY_MAX_ATTEMPTS:
            attempts += 1
            current_price = self._resolve_resubmit_price(planned, prices)
            result = self.bridge.submit_order(
                planned.symbol,
                planned.side.value,
                remaining_qty,
                current_price,
                strategy_name="joinquant-microcap-paper" if self.app_settings.environment == Environment.PAPER else "joinquant-microcap-live",
                order_remark=planned.reason,
            )
            broker_order_id = str(result.get("order_id") or "")
            wait_result = self._wait_for_order_resolution(broker_order_id, remaining_qty)
            filled_qty = int(wait_result.get("filled_qty") or 0)
            final_remaining_qty = max(remaining_qty - filled_qty, 0)
            session.add(
                AuditLogModel(
                    object_type="order_retry",
                    object_id=broker_order_id or planned.symbol,
                    message=f"{planned.symbol} 第 {attempts} 次委托检查完成",
                    payload={
                        "attempt": attempts,
                        "submitted_qty": remaining_qty,
                        "filled_qty": filled_qty,
                        "remaining_qty": final_remaining_qty,
                        "status_text": wait_result.get("status_text"),
                        "status_code": wait_result.get("status_code"),
                        "timed_out": bool(wait_result.get("timed_out")),
                    },
                )
            )
            if final_remaining_qty <= 0:
                return {"broker_order_id": broker_order_id, "attempts": attempts, "remaining_qty": 0}
            if not wait_result.get("status_available", True):
                return {"broker_order_id": broker_order_id, "attempts": attempts, "remaining_qty": final_remaining_qty}
            if wait_result.get("is_active", False) or wait_result.get("timed_out", False):
                cancel_result = self._cancel_broker_order(broker_order_id)
                session.add(
                    AuditLogModel(
                        object_type="order_retry",
                        object_id=broker_order_id or planned.symbol,
                        message=f"{planned.symbol} 未在 1 分钟内成交完成，已尝试撤单",
                        payload={"attempt": attempts, "cancel_result": cancel_result, "remaining_qty": final_remaining_qty},
                    )
                )
                if cancel_result != 0:
                    return {"broker_order_id": broker_order_id, "attempts": attempts, "remaining_qty": final_remaining_qty}
            remaining_qty = final_remaining_qty
        return {"broker_order_id": broker_order_id, "attempts": attempts, "remaining_qty": remaining_qty}

    def _wait_for_order_resolution(self, broker_order_id: str, submitted_qty: int) -> dict[str, Any]:
        deadline = time.time() + float(self.ORDER_RETRY_TIMEOUT_SECONDS)
        last_result = {
            "status_available": False,
            "status_text": "",
            "status_code": -1,
            "filled_qty": 0,
            "is_active": False,
            "timed_out": False,
        }
        while time.time() <= deadline:
            try:
                payload = self.bridge.get_order_status(broker_order_id)
            except QmtUnavailableError:
                return last_result
            current = self._parse_order_status(payload, submitted_qty)
            last_result = current
            if current["filled_qty"] >= submitted_qty:
                return current
            if not current["is_active"]:
                return current
            time.sleep(max(1, int(self.ORDER_RETRY_POLL_SECONDS)))
        last_result["timed_out"] = True
        return last_result

    def _parse_order_status(self, payload: dict[str, Any], submitted_qty: int) -> dict[str, Any]:
        status_text = self._pick_text(payload, "status_msg", "status", "order_status_msg", default="")
        status_code = self._coerce_int(payload.get("order_status", payload.get("status")))
        filled_qty = int(round(self._pick_number(payload, "traded_volume", "filled_qty", "fill_qty", default=0.0)))
        if filled_qty <= 0 and status_code == 56:
            filled_qty = int(submitted_qty)
        final_status_codes = {52, 53, 54, 56, 57}
        normalized_text = status_text.lower()
        is_filled = filled_qty >= submitted_qty or status_code == 56 or any(token in normalized_text for token in ["filled", "succeeded", "全部成交", "已成"])
        is_final = status_code in final_status_codes or any(token in normalized_text for token in ["cancel", "rejected", "junk", "撤", "废", "拒"])
        is_active = not is_filled and (self._is_active_order_status(status_code) or not is_final)
        return {
            "status_available": True,
            "status_text": status_text,
            "status_code": status_code,
            "filled_qty": max(0, min(filled_qty, submitted_qty)),
            "is_active": is_active,
            "timed_out": False,
        }

    def _cancel_broker_order(self, broker_order_id: str) -> int:
        if not broker_order_id:
            return -2
        try:
            payload = self.bridge.cancel_order(broker_order_id)
        except QmtUnavailableError:
            return -9
        try:
            return int(payload.get("cancel_result"))
        except (TypeError, ValueError):
            return -8

    def _resolve_resubmit_price(self, planned: PlannedOrder, prices: dict[str, Decimal]) -> Decimal:
        try:
            quotes = self.bridge.get_quotes([planned.symbol])
            quote = quotes.get(planned.symbol, {}) if isinstance(quotes, dict) else {}
            latest = self._resolve_quote_price(quote, is_buy=planned.side == OrderSide.BUY)
            if latest > 0:
                return Decimal(str(round(latest, 4)))
        except QmtUnavailableError:
            pass
        return prices.get(planned.symbol, planned.price)

    def _is_order_stale(self, payload: dict[str, Any]) -> bool:
        order_time = self._coerce_order_time(payload.get("order_time", payload.get("created_at", payload.get("entrust_time"))))
        if order_time is None:
            return False
        return (datetime.now() - order_time).total_seconds() >= float(self.ORDER_RETRY_TIMEOUT_SECONDS)

    @staticmethod
    def _is_active_order_status(status_code: int) -> bool:
        return status_code in {48, 49, 50, 51, 55, 255}

    @staticmethod
    def _coerce_order_time(value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            timestamp = float(value)
            if timestamp > 1_000_000_000_000:
                timestamp = timestamp / 1000.0
            try:
                return datetime.fromtimestamp(timestamp)
            except (OverflowError, OSError, ValueError):
                return None
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.to_pydatetime()

    @staticmethod
    def _coerce_int(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return -1

    def _build_account_state(
        self,
        payload: dict[str, Any],
        fallback_prices: dict[str, Decimal],
    ) -> tuple[AccountState, Decimal]:
        asset = payload.get("asset") or {}
        account_id = str(payload.get("account_id") or asset.get("account_id") or self.app_settings.qmt_account_id or "qmt-paper")
        cash = Decimal(str(self._pick_number(asset, "cash", "m_dCash")))
        frozen_cash = Decimal(str(self._pick_number(asset, "frozen_cash", "m_dFrozenCash")))
        positions: dict[str, Position] = {}
        for item in payload.get("positions", []) or []:
            symbol = self._pick_text(item, "stock_code", "symbol", "m_strInstrumentID", "instrument_id", "ticker")
            qty = int(round(self._pick_number(item, "volume", "qty", "current_amount", "total_qty", "m_nVolume")))
            if not symbol or qty <= 0:
                continue
            available_qty = int(round(self._pick_number(item, "can_use_volume", "available_qty", "m_nCanUseVolume", "enable_amount", default=qty)))
            cost_price = Decimal(str(self._pick_number(item, "open_price", "cost_price", "avg_price", "m_dOpenPrice", default=0.0)))
            market_price = Decimal(str(self._pick_number(item, "market_price", "last_price", "price", default=float(fallback_prices.get(symbol, cost_price or Decimal("0"))))))
            if market_price <= 0:
                market_price = fallback_prices.get(symbol, cost_price if cost_price > 0 else Decimal("0.01"))
            positions[symbol] = Position(
                symbol=symbol,
                qty=qty,
                available_qty=max(0, available_qty),
                cost_price=cost_price if cost_price > 0 else market_price,
                last_price=market_price,
            )
        state = AccountState(
            account_id=account_id,
            cash=cash,
            frozen_cash=frozen_cash,
            positions=positions,
        )
        reported_total = Decimal(str(self._pick_number(asset, "total_asset", "m_dTotalAsset", default=0.0)))
        if reported_total <= 0:
            reported_total = state.total_asset(fallback_prices)
        state.peak_total_asset = reported_total
        return state, reported_total

    def _build_snapshot_rows(
        self,
        payload: dict[str, Any],
        prices: dict[str, Decimal],
        strategy_total_asset: Decimal | None = None,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        state, total_asset = self._build_account_state(payload, prices)
        if strategy_total_asset is not None and strategy_total_asset > 0:
            total_asset = self._apply_strategy_capital(state, strategy_total_asset, prices)
        asset_row = {
            "account_id": state.account_id,
            "cash": state.cash,
            "frozen_cash": state.frozen_cash,
            "total_asset": total_asset,
        }
        positions = [
            {
                "account_id": state.account_id,
                "symbol": position.symbol,
                "qty": position.qty,
                "available_qty": position.available_qty,
                "cost_price": position.cost_price,
                "market_price": prices.get(position.symbol, position.last_price),
            }
            for position in state.positions.values()
        ]
        return asset_row, positions

    def _build_snapshot_payload_from_state(
        self,
        state: AccountState,
        prices: dict[str, Decimal],
    ) -> dict[str, Any]:
        total_asset = state.total_asset(prices)
        return {
            "account_id": state.account_id,
            "asset": {
                "cash": float(state.cash),
                "frozen_cash": float(state.frozen_cash),
                "total_asset": float(total_asset),
            },
            "positions": [
                {
                    "symbol": position.symbol,
                    "qty": position.qty,
                    "available_qty": position.available_qty,
                    "cost_price": float(position.cost_price),
                    "market_price": float(prices.get(position.symbol, position.last_price)),
                }
                for position in state.positions.values()
            ],
            "orders": [],
            "trades": [],
            "source": "planned_state_fallback",
        }

    def _build_equity_curve_snapshot(
        self,
        latest_date: pd.Timestamp,
        state: AccountState,
        prices: dict[str, Decimal],
        *,
        turnover: Decimal,
    ) -> pd.DataFrame:
        total_asset = state.total_asset(prices)
        return pd.DataFrame(
            [
                {
                    "trading_date": latest_date.date(),
                    "equity": float(total_asset),
                    "cash": float(state.cash),
                    "turnover": float(turnover),
                }
            ]
        )

    def _resolve_strategy_total_asset(self, initial_cash: Decimal, reported_total_asset: Decimal) -> Decimal:
        if self.app_settings.environment != Environment.PAPER:
            return reported_total_asset
        try:
            requested_total_asset = Decimal(str(initial_cash))
        except Exception:
            return reported_total_asset
        if requested_total_asset <= 0:
            return reported_total_asset
        return min(reported_total_asset, requested_total_asset)

    def _apply_strategy_capital(
        self,
        state: AccountState,
        strategy_total_asset: Decimal,
        prices: dict[str, Decimal],
    ) -> Decimal:
        if strategy_total_asset <= 0:
            return state.total_asset(prices)
        position_value = Decimal("0")
        for symbol, position in state.positions.items():
            current_price = prices.get(symbol, position.last_price)
            position.last_price = current_price
            position_value += current_price * position.qty
        capped_total_asset = max(position_value, strategy_total_asset)
        available_cash = capped_total_asset - position_value
        if available_cash < 0:
            available_cash = Decimal("0")
        state.cash = min(state.cash, available_cash)
        state.frozen_cash = min(state.frozen_cash, max(capped_total_asset - position_value - state.cash, Decimal("0")))
        state.peak_total_asset = capped_total_asset
        return capped_total_asset

    def _clone_account_state(self, state: AccountState) -> AccountState:
        return AccountState(
            account_id=state.account_id,
            cash=Decimal(str(state.cash)),
            frozen_cash=Decimal(str(state.frozen_cash)),
            positions={
                symbol: Position(
                    symbol=position.symbol,
                    qty=position.qty,
                    available_qty=position.available_qty,
                    cost_price=Decimal(str(position.cost_price)),
                    last_price=Decimal(str(position.last_price)),
                )
                for symbol, position in state.positions.items()
            },
            turnover=Decimal(str(state.turnover)),
            realized_pnl=Decimal(str(state.realized_pnl)),
            peak_total_asset=Decimal(str(state.peak_total_asset)),
        )

    def _apply_planned_order(self, state: AccountState, planned: PlannedOrder) -> None:
        notional = Decimal(str(planned.qty)) * planned.price
        if planned.side == OrderSide.BUY:
            fee = Decimal(str(buy_fee(float(notional))))
            state.cash -= notional + fee
            position = state.positions.get(planned.symbol)
            if position is None:
                state.positions[planned.symbol] = Position(
                    symbol=planned.symbol,
                    qty=planned.qty,
                    available_qty=planned.qty,
                    cost_price=(notional + fee) / Decimal(str(planned.qty)),
                    last_price=planned.price,
                )
            else:
                total_cost = position.cost_price * Decimal(str(position.qty)) + notional + fee
                new_qty = position.qty + planned.qty
                position.qty = new_qty
                position.available_qty += planned.qty
                position.cost_price = total_cost / Decimal(str(new_qty))
                position.last_price = planned.price
        else:
            fee = Decimal(str(sell_fee(float(notional))))
            state.cash += notional - fee
            position = state.positions.get(planned.symbol)
            if position is None:
                return
            position.qty = max(0, position.qty - planned.qty)
            position.available_qty = max(0, position.available_qty - planned.qty)
            position.last_price = planned.price
            if position.qty == 0:
                state.positions.pop(planned.symbol, None)
        state.turnover += notional

    @staticmethod
    def _resolve_quote_price(quote: dict[str, Any], *, is_buy: bool) -> float:
        preferred_key = "ask_price" if is_buy else "bid_price"
        preferred = quote.get(preferred_key)
        if isinstance(preferred, (list, tuple)):
            for item in preferred:
                value = QmtMicrocapTradingEngine._coerce_float(item)
                if value > 0:
                    return value
        value = QmtMicrocapTradingEngine._coerce_float(preferred)
        if value > 0:
            return value
        return QmtMicrocapTradingEngine._pick_number(quote, "last_price", "open", default=0.0)

    @staticmethod
    def _normalize_activity_side(value: Any) -> str:
        if value in (None, ""):
            return ""
        raw = str(value).strip().lower()
        if raw in {"buy", "b", "long", "买入", "证券买入"}:
            return "buy"
        if raw in {"sell", "s", "short", "卖出", "证券卖出"}:
            return "sell"
        if raw.isdigit():
            number = int(raw)
            if number in {1, 23, 48}:
                return "buy"
            if number in {2, 24, 49}:
                return "sell"
        if "买" in raw:
            return "buy"
        if "卖" in raw:
            return "sell"
        return raw

    @staticmethod
    def _pick_text(payload: dict[str, Any], *keys: str, default: str = "") -> str:
        for key in keys:
            value = payload.get(key)
            if value not in (None, ""):
                return str(value).strip()
        return default

    @staticmethod
    def _pick_number(payload: dict[str, Any], *keys: str, default: float = 0.0) -> float:
        for key in keys:
            if key in payload:
                value = QmtMicrocapTradingEngine._coerce_float(payload.get(key))
                if value != 0.0 or payload.get(key) not in (None, "", [], ()):
                    return value
        return default

    @staticmethod
    def _coerce_float(value: Any) -> float:
        if isinstance(value, (list, tuple)):
            for item in value:
                number = QmtMicrocapTradingEngine._coerce_float(item)
                if number > 0:
                    return number
            return 0.0
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def _reset_non_live_state(self, session) -> None:
        if self.app_settings.environment == Environment.LIVE:
            return
        for model in [
            AuditLogModel,
            TradeModel,
            OrderEventModel,
            OrderModel,
            RiskDecisionModel,
            OrderIntentModel,
            PositionSnapshotModel,
            AssetSnapshotModel,
        ]:
            session.execute(delete(model))
        session.flush()
