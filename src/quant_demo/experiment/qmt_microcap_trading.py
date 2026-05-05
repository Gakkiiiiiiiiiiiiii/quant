from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import delete, select
from sqlalchemy.orm import sessionmaker

from quant_demo.adapters.qmt.bridge_client import QmtBridgeClient
from quant_demo.adapters.qmt.gateway import QmtBrokerClient, create_bridge_client
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
    _apply_st_risk_announcement_flags,
    _special_treatment_flags_from_name,
    adjust_target_amount_for_rules,
    available_trade_shares,
    build_portfolio_selection,
    buy_fee,
    calc_amount_by_cash,
    calc_target_amount_by_value,
    calendar_hedge_ratio,
    can_trade,
    fit_target_count_by_cash,
    resolve_effective_microcap_config,
    sell_fee,
    volume_units_to_shares,
)
from quant_demo.marketdata.ingestion import load_history_metadata
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


@dataclass(slots=True)
class PendingRetryOrder:
    planned: PlannedOrder
    broker_order_id: str
    submitted_qty: int
    remaining_qty: int
    attempts: int
    submitted_at: datetime
    price_source: str


class QmtMicrocapTradingEngine:
    ORDER_RETRY_TIMEOUT_SECONDS = 60
    ORDER_RETRY_POLL_SECONDS = 5
    ORDER_RETRY_MAX_ATTEMPTS = 3

    @staticmethod
    def _normalize_trade_date(raw_value: Any) -> pd.Timestamp:
        return pd.Timestamp(pd.to_datetime(raw_value, errors="coerce")).normalize()

    def _protected_sell_symbols(self) -> set[str]:
        return {
            str(symbol).strip()
            for symbol in (self.app_settings.qmt_protected_sell_symbols or [])
            if str(symbol).strip()
        }

    def _load_current_special_treatment_flags(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        normalized_symbols = [str(symbol).strip() for symbol in symbols if str(symbol).strip()]
        if not normalized_symbols:
            return {}
        try:
            details = self.bridge.get_instrument_details(normalized_symbols)
        except QmtUnavailableError:
            return {}
        flags: dict[str, dict[str, Any]] = {}
        for symbol in normalized_symbols:
            payload = details.get(symbol) or {}
            instrument_name = str(payload.get("InstrumentName") or payload.get("instrument_name") or symbol)
            is_st_name, is_star_st_name, is_delisting_name = _special_treatment_flags_from_name(instrument_name)
            if not (is_st_name or is_star_st_name or is_delisting_name):
                continue
            flags[symbol] = {
                "symbol": symbol,
                "instrument_name": instrument_name,
                "is_st_name": bool(is_st_name),
                "is_star_st_name": bool(is_star_st_name),
                "is_delisting_name": bool(is_delisting_name),
                "reason": "execution_day_special_treatment",
                "title": instrument_name,
                "risk_stage": "effective",
                "expected_effective_date": "",
                "url": "",
            }
        return flags

    def _build_execution_day_special_treatment_controls(
        self,
        *,
        execution_date: pd.Timestamp,
        live_state: AccountState,
        target_symbols: list[str],
        risk_sell_watch: list[dict[str, Any]],
    ) -> dict[str, Any]:
        execution_dt = pd.Timestamp(execution_date).normalize()
        held_symbols = sorted(set(live_state.positions))
        current_flags = self._load_current_special_treatment_flags(sorted(set(held_symbols) | set(target_symbols)))
        blocked_buy_symbols: set[str] = set(current_flags)
        forced_exit_details: dict[str, dict[str, Any]] = {symbol: dict(payload) for symbol, payload in current_flags.items() if symbol in held_symbols}
        for item in risk_sell_watch:
            symbol = str(item.get("symbol") or "").strip()
            if not symbol:
                continue
            if bool(item.get("block_buy")):
                blocked_buy_symbols.add(symbol)
            risk_stage = str(item.get("risk_stage") or "").strip().lower()
            expected_effective_date = pd.to_datetime(item.get("expected_effective_date"), errors="coerce")
            if risk_stage != "final" or pd.isna(expected_effective_date):
                continue
            expected_dt = pd.Timestamp(expected_effective_date).normalize()
            if expected_dt > execution_dt:
                continue
            blocked_buy_symbols.add(symbol)
            if symbol in held_symbols and symbol not in forced_exit_details:
                forced_exit_details[symbol] = {
                    "symbol": symbol,
                    "instrument_name": str(item.get("title") or symbol),
                    "is_st_name": True,
                    "is_star_st_name": False,
                    "is_delisting_name": False,
                    "reason": "execution_day_final_st_notice",
                    "title": str(item.get("title") or ""),
                    "risk_stage": risk_stage,
                    "expected_effective_date": expected_dt.date().isoformat(),
                    "url": str(item.get("url") or ""),
                }
        forced_exit_symbols = sorted(forced_exit_details)
        blocked_buy_list = sorted(blocked_buy_symbols)
        return {
            "execution_day_buy_blocked_symbols": blocked_buy_list,
            "execution_day_forced_exit_symbols": forced_exit_symbols,
            "execution_day_forced_exit_details": [forced_exit_details[symbol] for symbol in forced_exit_symbols],
        }

    @staticmethod
    def _forced_exit_detail_map(target_meta: dict[str, Any]) -> dict[str, dict[str, Any]]:
        details = {}
        for item in target_meta.get("execution_day_forced_exit_details") or []:
            symbol = str(item.get("symbol") or "").strip()
            if symbol:
                details[symbol] = dict(item)
        return details

    def __init__(
        self,
        session_factory: sessionmaker,
        app_settings: AppSettings,
        strategy_settings: StrategySettings,
        bridge_client: QmtBrokerClient | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.app_settings = app_settings
        self.strategy_settings = strategy_settings
        self.cfg = MicrocapStrategyConfig.from_strategy_settings(strategy_settings)
        self.bridge = bridge_client or create_bridge_client(app_settings)

    def _now_local(self) -> datetime:
        return datetime.now()

    def _emit_execution_log(self, event: str, **payload: Any) -> None:
        print(
            json.dumps(
                {
                    "ts": self._now_local().isoformat(timespec="seconds"),
                    "event": event,
                    **payload,
                },
                ensure_ascii=False,
            )
        )

    def preview(self, initial_cash: Decimal) -> tuple[Path, dict[str, Any]]:
        context = self._prepare_trade_context(
            initial_cash,
            allow_snapshot_fallback=False,
            allow_quote_fallback=True,
            prefer_quote_fallback=True,
            require_realtime_positions=True,
            assume_all_positions_available_next_session=True,
        )
        planned_orders, blocked_sell_symbols = self._filter_protected_sell_orders(context["planned_orders"])
        plan_payload = self._build_plan_payload(
            latest_date=context["latest_date"],
            strategy_total_asset=context["strategy_total_asset"],
            target_meta=context["target_meta"],
            planned_orders=planned_orders,
            live_state=context["live_state"],
        )
        plan_payload["blocked_sell_symbols"] = blocked_sell_symbols
        plan_path = self._write_trade_plan(plan_payload)
        return plan_path, plan_payload

    def _filter_protected_sell_orders(self, planned_orders: list[PlannedOrder]) -> tuple[list[PlannedOrder], list[str]]:
        protected_sell_symbols = self._protected_sell_symbols()
        if not protected_sell_symbols:
            return planned_orders, []
        remaining_orders: list[PlannedOrder] = []
        blocked_symbols: list[str] = []
        for item in planned_orders:
            if item.side == OrderSide.SELL and item.symbol in protected_sell_symbols:
                blocked_symbols.append(item.symbol)
                continue
            remaining_orders.append(item)
        return remaining_orders, sorted(set(blocked_symbols))

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
        planned_orders, protected_sell_symbols = self._filter_protected_sell_orders(planned_orders)
        fallback_prices = {item.symbol: item.price for item in planned_orders}
        account_snapshot = self.bridge.get_account_snapshot()
        live_state, reported_total_asset = self._build_account_state(account_snapshot, fallback_prices)
        protected_sell_cancellations = self._cancel_protected_sell_open_orders(
            account_snapshot=account_snapshot,
            trade_date=str(payload.get("planned_execution_date") or "").strip(),
        )
        stale_cancellations = self._cancel_stale_open_orders(
            account_snapshot=account_snapshot,
            trade_date=str(payload.get("planned_execution_date") or "").strip(),
        )
        planned_orders, execution_day_st_summary = self._recheck_execution_day_special_treatment(payload, planned_orders, live_state)
        same_direction_cancellations = self._cancel_same_direction_open_orders(
            account_snapshot=account_snapshot,
            planned_orders=planned_orders,
            trade_date=str(payload.get("planned_execution_date") or "").strip(),
        )
        if protected_sell_cancellations or stale_cancellations or same_direction_cancellations:
            account_snapshot = self.bridge.get_account_snapshot()
            live_state, reported_total_asset = self._build_account_state(account_snapshot, fallback_prices)
        self._emit_execution_log(
            "execution_day_special_treatment_recheck",
            planned_execution_date=str(payload.get("planned_execution_date") or "").strip(),
            blocked_buy_symbols=execution_day_st_summary["blocked_buy_symbols"],
            forced_exit_symbols=execution_day_st_summary["forced_exit_symbols"],
            skipped_unavailable_symbols=execution_day_st_summary["skipped_unavailable_symbols"],
            skipped_halted_buy_symbols=execution_day_st_summary.get("skipped_halted_buy_symbols", []),
            same_direction_canceled_symbols=[item["symbol"] for item in same_direction_cancellations],
            protected_sell_symbols=protected_sell_symbols,
            protected_sell_canceled_symbols=[item["symbol"] for item in protected_sell_cancellations],
            blocked_sell_symbols=execution_day_st_summary.get("blocked_sell_symbols", []),
        )
        diff_summary = self._diff_plan_against_today_activity(
            account_snapshot=account_snapshot,
            planned_orders=planned_orders,
            planned_execution_date=str(payload.get("planned_execution_date") or "").strip(),
            stale_cancellations=stale_cancellations,
            same_direction_cancellations=same_direction_cancellations,
        )
        planned_orders = diff_summary["remaining_orders"]
        self._emit_execution_log(
            "execute_plan_loaded",
            plan_path=str(plan_path),
            signal_trade_date=expected_signal_date,
            planned_execution_date=str(payload.get("planned_execution_date") or "").strip(),
            order_count=len(planned_orders),
            buy_count=sum(1 for item in planned_orders if item.side == OrderSide.BUY),
            sell_count=sum(1 for item in planned_orders if item.side == OrderSide.SELL),
            buy_symbols=[item.symbol for item in planned_orders if item.side == OrderSide.BUY],
            sell_symbols=[item.symbol for item in planned_orders if item.side == OrderSide.SELL],
            protected_sell_symbols=protected_sell_symbols,
            blocked_sell_symbols=execution_day_st_summary.get("blocked_sell_symbols", []),
        )
        quote_symbols = sorted(set(fallback_prices) | set(live_state.positions))
        quotes = self._get_quotes_payload(quote_symbols, allow_quote_fallback=True)
        price_map, volume_map = self._build_quote_maps_from_quotes(quotes, fallback_prices)
        strategy_total_asset = self._resolve_strategy_total_asset(strategy_total_asset, reported_total_asset)
        strategy_total_asset = self._apply_strategy_capital(live_state, strategy_total_asset, price_map)
        if not planned_orders:
            equity_curve = self._build_equity_curve_snapshot(pd.Timestamp(actual_signal_date), live_state, price_map, turnover=Decimal("0"))
            metrics = Evaluator().evaluate(equity_curve)
            receipt_path = self._write_execution_receipt(payload, [], Path(plan_path), execution_summary=diff_summary)
            self._emit_execution_log(
                "execute_plan_noop",
                receipt_path=str(receipt_path),
                signal_trade_date=expected_signal_date,
                planned_execution_date=str(payload.get("planned_execution_date") or "").strip(),
            )
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
            quotes=quotes,
            risk_service=risk_service,
            target_meta=target_meta,
            instrument_frame=pd.DataFrame(),
            strategy_total_asset=strategy_total_asset,
        )
        metrics = Evaluator().evaluate(equity_curve)
        self._write_execution_receipt(payload, planned_orders, report_path, execution_summary=diff_summary)
        self._emit_execution_log(
            "execute_plan_finished",
            report_path=str(report_path),
            signal_trade_date=expected_signal_date,
            planned_execution_date=str(payload.get("planned_execution_date") or "").strip(),
            turnover=float(metrics.turnover),
        )
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
            quotes=context["quotes"],
            risk_service=context["risk_service"],
            target_meta=context["target_meta"],
            instrument_frame=context["instrument_frame"],
            strategy_total_asset=context["strategy_total_asset"],
        )
        metrics = Evaluator().evaluate(equity_curve)
        return report_path, metrics, equity_curve

        self._refresh_history()
        prepared, instrument_frame, loader = self._load_prepared_history()
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

        day_frame = self._apply_st_risk_notice_state(day_frame, latest_date, live_state, loader)
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
        assume_all_positions_available_next_session: bool = False,
    ) -> dict[str, Any]:
        self._refresh_history()
        prepared, instrument_frame, loader = self._load_prepared_history()
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
        if assume_all_positions_available_next_session:
            live_state = self._normalize_preview_live_state_for_next_session(live_state)
        strategy_total_asset = self._resolve_strategy_total_asset(initial_cash, reported_total_asset)
        strategy_total_asset = self._apply_strategy_capital(live_state, strategy_total_asset, fallback_prices)
        day_frame = self._apply_st_risk_notice_state(day_frame, latest_date, live_state, loader)
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
            "quotes": quotes,
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
            "st_risk_blocked_count": int(planned_target_meta.get("st_risk_blocked_count", 0) or 0),
            "st_risk_blocked_symbols": list(planned_target_meta.get("st_risk_blocked_symbols") or []),
            "st_risk_sell_watch": list(planned_target_meta.get("st_risk_sell_watch") or []),
            "execution_day_buy_blocked_symbols": list(planned_target_meta.get("execution_day_buy_blocked_symbols") or []),
            "execution_day_forced_exit_symbols": list(planned_target_meta.get("execution_day_forced_exit_symbols") or []),
            "execution_day_forced_exit_details": list(planned_target_meta.get("execution_day_forced_exit_details") or []),
            "forced_exit_untradable_symbols": list(planned_target_meta.get("forced_exit_untradable_symbols") or []),
        }

    def _refresh_history(self) -> None:
        if self.app_settings.history_source.lower() != "qmt":
            return
        from quant_demo.marketdata.history_manager import refresh_history

        history_path = Path(self.app_settings.history_parquet)
        if self._history_refresh_up_to_date(history_path):
            self._emit_execution_log(
                "history_refresh_skipped",
                history_path=str(history_path),
                reason="history_already_updated_today",
            )
            return
        try:
            refresh_history(self.app_settings, mode="incremental")
        except QmtUnavailableError:
            if not history_path.exists():
                raise

    def _history_refresh_up_to_date(self, history_path: Path) -> bool:
        if not history_path.exists():
            return False
        try:
            metadata = load_history_metadata(history_path)
            latest_date = self._load_raw_history_latest_date()
        except Exception:
            return False
        now = pd.Timestamp(self._now_local()).tz_localize(None)
        today = pd.Timestamp(now.date()).normalize()
        if latest_date < today:
            return False
        raw_updated_at = str((metadata or {}).get("updated_at") or "").strip()
        if not raw_updated_at:
            return latest_date >= today
        try:
            updated_at = pd.Timestamp(raw_updated_at).tz_localize(None) if pd.Timestamp(raw_updated_at).tzinfo is not None else pd.Timestamp(raw_updated_at)
        except Exception:
            return latest_date >= today
        return updated_at.normalize() >= today

    def _load_raw_history_latest_date(self) -> pd.Timestamp:
        history_path = Path(self.app_settings.history_parquet)
        frame = pd.read_parquet(history_path, columns=["trading_date"])
        frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.normalize()
        if frame.empty:
            raise RuntimeError("微盘交易引擎未找到可用历史数据")
        return pd.Timestamp(frame["trading_date"].max()).normalize()

    def _load_prepared_history(self) -> tuple[pd.DataFrame, pd.DataFrame, JoinQuantMicrocapBacktestEngine]:
        loader = JoinQuantMicrocapBacktestEngine(self.session_factory, self.app_settings, self.strategy_settings)
        history = self._load_recent_history_window()
        symbols = history["symbol"].dropna().astype(str).unique().tolist()
        instrument_frame = loader._load_instrument_frame(symbols)
        capital_frame = loader._load_capital_frame(symbols)
        prepared = loader._prepare_history(history, instrument_frame, capital_frame)
        return prepared, instrument_frame, loader

    def _load_benchmark_close_series(self) -> pd.Series:
        loader = JoinQuantMicrocapBacktestEngine(self.session_factory, self.app_settings, self.strategy_settings)
        benchmark = loader._load_benchmark()
        if benchmark.empty:
            return pd.Series(dtype=float)
        return benchmark.set_index("trading_date")["close"].astype(float).sort_index()

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
        frame = pd.read_parquet(
            history_path,
            columns=["trading_date", "symbol", "open", "high", "low", "close", "volume", "amount"],
        )
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
        benchmark_close = self._load_benchmark_close_series()
        execution_date = pd.Timestamp(latest_date + pd.offsets.BDay(1)).normalize()
        effective_cfg, profile_meta = resolve_effective_microcap_config(
            latest_date,
            self.cfg,
            benchmark_close,
            allocation_trade_date=execution_date,
        )
        hedge_ratio = float(profile_meta["hedge_ratio"])
        invest_value = total_value * (1.0 - effective_cfg.cash_buffer)
        stock_invest_value = invest_value * (1.0 - hedge_ratio)
        selection = build_portfolio_selection(
            day_frame.reset_index(drop=True),
            stock_holdings,
            total_value * (1.0 - hedge_ratio),
            effective_cfg,
        )
        target_stocks = list(selection["targets"])
        ranked_count = int(selection["ranked_count"])
        price_lookup = {str(symbol): float(row.get("open") or 0.0) for symbol, row in day_frame.iterrows()}
        risk_sell_watch: list[dict[str, Any]] = []
        if "st_preannounce_prompt_sell" in day_frame.columns:
            risk_rows = day_frame[
                day_frame["symbol"].astype(str).isin(set(stock_holdings))
                & day_frame["st_preannounce_prompt_sell"].fillna(False).astype(bool)
            ].copy()
            if not risk_rows.empty:
                risk_rows = risk_rows.reset_index(drop=True).sort_values(["symbol"])
                for row in risk_rows.itertuples(index=False):
                    risk_sell_watch.append(
                        {
                            "symbol": str(row.symbol),
                            "title": str(getattr(row, "st_preannounce_title", "") or ""),
                            "announce_date": (
                                pd.Timestamp(getattr(row, "st_preannounce_notice_date")).date().isoformat()
                                if pd.notna(getattr(row, "st_preannounce_notice_date", pd.NaT))
                                else ""
                            ),
                            "expected_effective_date": (
                                pd.Timestamp(getattr(row, "st_preannounce_effective_date")).date().isoformat()
                                if pd.notna(getattr(row, "st_preannounce_effective_date", pd.NaT))
                                else ""
                            ),
                            "risk_stage": str(getattr(row, "st_preannounce_stage", "") or ""),
                            "url": str(getattr(row, "st_preannounce_url", "") or ""),
                        }
                    )
        execution_controls = self._build_execution_day_special_treatment_controls(
            execution_date=execution_date,
            live_state=live_state,
            target_symbols=target_stocks,
            risk_sell_watch=risk_sell_watch,
        )
        blocked_buy_symbols = set(execution_controls["execution_day_buy_blocked_symbols"])
        eligible_targets = [symbol for symbol in target_stocks if symbol not in blocked_buy_symbols]
        fitted_targets = fit_target_count_by_cash(
            eligible_targets,
            price_lookup=price_lookup,
            invest_value=stock_invest_value,
            cfg=effective_cfg,
        )
        target_count = len(fitted_targets)
        each_target_value = stock_invest_value / float(target_count) if target_count > 0 else 0.0
        blocked_symbols = list(selection.get("st_risk_blocked_symbols") or [])
        return {
            "trade_date": latest_date.date().isoformat(),
            "profile_trade_date": str(profile_meta.get("profile_trade_date") or execution_date.date().isoformat()),
            "targets": fitted_targets,
            "ranked_count": ranked_count,
            "target_count": target_count,
            "hedge_ratio": hedge_ratio,
            "cash_reserve_ratio": round(hedge_ratio + effective_cfg.cash_buffer, 4),
            "stock_invest_value": stock_invest_value,
            "each_target_value": each_target_value,
            "profile_name": str(profile_meta["profile_name"]),
            "profile_type": str(profile_meta["profile_type"]),
            "micro_weight": float(profile_meta["micro_weight"]),
            "cash_weight": float(profile_meta["cash_weight"]),
            "instant_crash_level": int(profile_meta["instant_crash_level"]),
            "crash_add_level": int(profile_meta["crash_add_level"]),
            "bias12": float(profile_meta["bias12"]),
            "ret20": float(profile_meta["ret20"]),
            "drawdown60": float(profile_meta["drawdown60"]),
            "signal_trade_date": str(profile_meta.get("signal_trade_date") or latest_date.date().isoformat()),
            "effective_target_hold_num": int(effective_cfg.target_hold_num),
            "effective_buy_rank": int(effective_cfg.buy_rank),
            "effective_keep_rank": int(effective_cfg.keep_rank),
            "effective_min_avg_money_20": float(effective_cfg.min_avg_money_20),
            "st_risk_blocked_count": int(selection.get("st_risk_blocked_count", len(blocked_symbols)) or 0),
            "st_risk_blocked_symbols": blocked_symbols,
            "st_risk_sell_watch": risk_sell_watch,
            "execution_day_buy_blocked_symbols": list(execution_controls["execution_day_buy_blocked_symbols"]),
            "execution_day_forced_exit_symbols": list(execution_controls["execution_day_forced_exit_symbols"]),
            "execution_day_forced_exit_details": list(execution_controls["execution_day_forced_exit_details"]),
            "protected_sell_symbols": sorted(self._protected_sell_symbols()),
        }

    def _apply_st_risk_notice_state(
        self,
        day_frame: pd.DataFrame,
        latest_date: pd.Timestamp,
        live_state: AccountState,
        loader: JoinQuantMicrocapBacktestEngine,
    ) -> pd.DataFrame:
        if day_frame.empty or not self.cfg.st_risk_announcement_enabled:
            return day_frame
        rank_limit = max(int(self.cfg.query_limit), int(self.cfg.keep_rank) + 60, int(self.cfg.target_hold_num) * 8)
        candidate_symbols = (
            day_frame.reset_index(drop=True)
            .sort_values(["market_cap_prev", "symbol"], ascending=[True, True])["symbol"]
            .astype(str)
            .head(rank_limit)
            .tolist()
        )
        symbols = sorted(set(candidate_symbols) | set(live_state.positions))
        if not symbols:
            return day_frame
        start_date = pd.Timestamp(latest_date).normalize() - pd.Timedelta(days=max(5, int(self.cfg.st_risk_announcement_lookback_days)))
        announcements = loader._load_st_risk_announcement_frame(symbols, start_date, pd.Timestamp(latest_date).normalize())
        return _apply_st_risk_announcement_flags(day_frame.reset_index(drop=True), announcements, latest_date).set_index("symbol", drop=False)

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
                "profile_trade_date": str(target_meta.get("profile_trade_date") or planned_execution_date),
                "targets": list(target_meta.get("targets") or []),
                "ranked_count": int(target_meta.get("ranked_count", 0) or 0),
                "target_count": int(target_meta.get("target_count", 0) or 0),
                "hedge_ratio": float(target_meta.get("hedge_ratio", 0.0) or 0.0),
                "cash_reserve_ratio": float(target_meta.get("cash_reserve_ratio", 0.0) or 0.0),
                "stock_invest_value": float(target_meta.get("stock_invest_value", 0.0) or 0.0),
                "each_target_value": float(target_meta.get("each_target_value", 0.0) or 0.0),
                "profile_name": str(target_meta.get("profile_name") or ""),
                "profile_type": str(target_meta.get("profile_type") or ""),
                "micro_weight": float(target_meta.get("micro_weight", 0.0) or 0.0),
                "cash_weight": float(target_meta.get("cash_weight", 0.0) or 0.0),
                "instant_crash_level": int(target_meta.get("instant_crash_level", 0) or 0),
                "crash_add_level": int(target_meta.get("crash_add_level", 0) or 0),
                "bias12": float(target_meta.get("bias12", 0.0) or 0.0),
                "ret20": float(target_meta.get("ret20", 0.0) or 0.0),
                "drawdown60": float(target_meta.get("drawdown60", 0.0) or 0.0),
                "signal_trade_date": str(target_meta.get("signal_trade_date") or signal_trade_date),
                "effective_target_hold_num": int(target_meta.get("effective_target_hold_num", 0) or 0),
                "effective_buy_rank": int(target_meta.get("effective_buy_rank", 0) or 0),
                "effective_keep_rank": int(target_meta.get("effective_keep_rank", 0) or 0),
                "effective_min_avg_money_20": float(target_meta.get("effective_min_avg_money_20", 0.0) or 0.0),
                "st_risk_blocked_count": int(target_meta.get("st_risk_blocked_count", 0) or 0),
                "st_risk_blocked_symbols": list(target_meta.get("st_risk_blocked_symbols") or []),
                "st_risk_sell_watch": list(target_meta.get("st_risk_sell_watch") or []),
                "execution_day_buy_blocked_symbols": list(target_meta.get("execution_day_buy_blocked_symbols") or []),
                "execution_day_forced_exit_symbols": list(target_meta.get("execution_day_forced_exit_symbols") or []),
                "execution_day_forced_exit_details": list(target_meta.get("execution_day_forced_exit_details") or []),
                "forced_exit_untradable_symbols": list(target_meta.get("forced_exit_untradable_symbols") or []),
                "protected_sell_symbols": list(target_meta.get("protected_sell_symbols") or []),
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
            "blocked_sell_symbols": [],
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

    def _recheck_execution_day_special_treatment(
        self,
        payload: dict[str, Any],
        planned_orders: list[PlannedOrder],
        live_state: AccountState,
    ) -> tuple[list[PlannedOrder], dict[str, Any]]:
        target_meta = dict(payload.get("target_meta") or {})
        execution_date = self._normalize_trade_date(payload.get("planned_execution_date"))
        if pd.isna(execution_date):
            return planned_orders, {
                "blocked_buy_symbols": [],
                "forced_exit_symbols": [],
                "skipped_unavailable_symbols": [],
                "skipped_halted_buy_symbols": [],
            }
        execution_controls = self._build_execution_day_special_treatment_controls(
            execution_date=execution_date,
            live_state=live_state,
            target_symbols=[str(symbol) for symbol in target_meta.get("targets") or []],
            risk_sell_watch=list(target_meta.get("st_risk_sell_watch") or []),
        )
        blocked_buy_symbols = set(execution_controls["execution_day_buy_blocked_symbols"])
        forced_exit_symbols = set(execution_controls["execution_day_forced_exit_symbols"])
        protected_sell_symbols = self._protected_sell_symbols()
        forced_exit_detail_map = {
            str(item.get("symbol") or ""): dict(item)
            for item in execution_controls["execution_day_forced_exit_details"]
            if str(item.get("symbol") or "").strip()
        }
        adjusted_orders: list[PlannedOrder] = []
        skipped_buy_symbols: list[str] = []
        skipped_halted_buy_symbols: list[str] = []
        existing_sell_symbols: set[str] = set()
        for order in planned_orders:
            if order.side == OrderSide.BUY and order.symbol in blocked_buy_symbols:
                skipped_buy_symbols.append(order.symbol)
                continue
            if order.side == OrderSide.SELL and order.symbol in protected_sell_symbols:
                continue
            if order.side == OrderSide.SELL:
                existing_sell_symbols.add(order.symbol)
            adjusted_orders.append(order)
        pending_buy_symbols = sorted(
            {
                order.symbol
                for order in adjusted_orders
                if order.side == OrderSide.BUY and order.symbol not in blocked_buy_symbols
            }
        )
        if pending_buy_symbols:
            quotes = self._get_quotes_payload(pending_buy_symbols, allow_quote_fallback=True)
            for symbol in pending_buy_symbols:
                quote = quotes.get(symbol, {}) if isinstance(quotes, dict) else {}
                if self._quote_indicates_halt(quote):
                    blocked_buy_symbols.add(symbol)
                    skipped_halted_buy_symbols.append(symbol)
        if blocked_buy_symbols:
            rechecked_orders: list[PlannedOrder] = []
            for order in adjusted_orders:
                if order.side == OrderSide.BUY and order.symbol in blocked_buy_symbols:
                    skipped_buy_symbols.append(order.symbol)
                    continue
                rechecked_orders.append(order)
            adjusted_orders = rechecked_orders
        skipped_unavailable_symbols: list[str] = []
        for symbol in sorted(forced_exit_symbols):
            if symbol in protected_sell_symbols:
                continue
            if symbol in existing_sell_symbols:
                continue
            position = live_state.positions.get(symbol)
            if position is None or int(position.available_qty) <= 0:
                skipped_unavailable_symbols.append(symbol)
                continue
            adjusted_orders.append(
                PlannedOrder(
                    symbol=symbol,
                    side=OrderSide.SELL,
                    qty=int(position.available_qty),
                    price=Decimal(str(max(float(position.last_price), 0.01))),
                    reason="execution_day_special_treatment_exit",
                    metadata={"forced_exit": forced_exit_detail_map.get(symbol, {})},
                )
            )
        return adjusted_orders, {
            "blocked_buy_symbols": sorted(set(skipped_buy_symbols) | blocked_buy_symbols),
            "forced_exit_symbols": sorted(forced_exit_symbols),
            "skipped_unavailable_symbols": sorted(skipped_unavailable_symbols),
            "skipped_halted_buy_symbols": sorted(set(skipped_halted_buy_symbols)),
            "blocked_sell_symbols": sorted(
                protected_sell_symbols
                & (
                    {item.symbol for item in planned_orders if item.side == OrderSide.SELL}
                    | forced_exit_symbols
                )
            ),
        }

    def _diff_plan_against_today_activity(
        self,
        *,
        account_snapshot: dict[str, Any],
        planned_orders: list[PlannedOrder],
        planned_execution_date: str,
        stale_cancellations: list[dict[str, Any]] | None = None,
        same_direction_cancellations: list[dict[str, Any]] | None = None,
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
            "same_direction_cancellations": same_direction_cancellations or [],
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
            traded_qty = int(round(self._pick_number(item, "traded_volume", "filled_qty", "volume", default=0.0)))
            qty = traded_qty
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

    def _cancel_same_direction_open_orders(
        self,
        account_snapshot: dict[str, Any],
        planned_orders: list[PlannedOrder],
        trade_date: str,
    ) -> list[dict[str, Any]]:
        cancellations: list[dict[str, Any]] = []
        target_trade_date = pd.to_datetime(trade_date, errors="coerce")
        planned_keys = {(item.symbol, item.side.value) for item in planned_orders}
        if not planned_keys:
            return cancellations
        for item in account_snapshot.get("orders", []) or []:
            status_code = self._coerce_int(item.get("order_status", item.get("status")))
            if not self._is_active_order_status(status_code):
                continue
            side = self._normalize_activity_side(
                self._pick_text(item, "side", "order_side", "direction", "entrust_bs", "bs_flag", "operation")
                or item.get("order_type")
                or item.get("m_nOrderType")
            )
            symbol = self._pick_text(item, "stock_code", "symbol", "ticker", "instrument_id")
            if (symbol, side) not in planned_keys:
                continue
            occurred_at = self._pick_text(item, "order_time", "created_at", "entrust_time", default="")
            occurred_day = pd.to_datetime(occurred_at, errors="coerce")
            if pd.notna(target_trade_date) and pd.notna(occurred_day) and occurred_day.date() != target_trade_date.date():
                continue
            order_qty = int(round(self._pick_number(item, "order_volume", "volume", "qty", "entrust_amount", default=0.0)))
            traded_qty = int(round(self._pick_number(item, "traded_volume", "filled_qty", default=0.0)))
            remaining_qty = max(order_qty - traded_qty, 0)
            if remaining_qty <= 0:
                continue
            broker_order_id = self._pick_text(item, "order_id", "order_sysid", "entrust_no", default="")
            cancel_result = self._cancel_broker_order(broker_order_id)
            cancellations.append(
                {
                    "symbol": symbol,
                    "side": side,
                    "broker_order_id": broker_order_id,
                    "order_status": status_code,
                    "cancel_result": cancel_result,
                    "order_qty": order_qty,
                    "traded_volume": traded_qty,
                    "remaining_qty": remaining_qty,
                    "reason": "same_direction_replace",
                }
            )
            self._emit_execution_log(
                "same_direction_open_order_cancelled",
                symbol=symbol,
                side=side,
                broker_order_id=broker_order_id,
                order_status=status_code,
                remaining_qty=remaining_qty,
                cancel_result=cancel_result,
            )
        return cancellations

    def _cancel_protected_sell_open_orders(self, account_snapshot: dict[str, Any], trade_date: str) -> list[dict[str, Any]]:
        cancellations: list[dict[str, Any]] = []
        protected_sell_symbols = self._protected_sell_symbols()
        if not protected_sell_symbols:
            return cancellations
        target_trade_date = pd.to_datetime(trade_date, errors="coerce")
        for item in account_snapshot.get("orders", []) or []:
            status_code = self._coerce_int(item.get("order_status", item.get("status")))
            if not self._is_active_order_status(status_code):
                continue
            side = self._normalize_activity_side(
                self._pick_text(item, "side", "order_side", "direction", "entrust_bs", "bs_flag", "operation")
                or item.get("order_type")
                or item.get("m_nOrderType")
            )
            symbol = self._pick_text(item, "stock_code", "symbol", "ticker", "instrument_id")
            if side != "sell" or symbol not in protected_sell_symbols:
                continue
            occurred_at = self._pick_text(item, "order_time", "created_at", "entrust_time", default="")
            occurred_day = pd.to_datetime(occurred_at, errors="coerce")
            if pd.notna(target_trade_date) and pd.notna(occurred_day) and occurred_day.date() != target_trade_date.date():
                continue
            broker_order_id = self._pick_text(item, "order_id", "order_sysid", "entrust_no", default="")
            cancel_result = self._cancel_broker_order(broker_order_id)
            cancellations.append(
                {
                    "symbol": symbol,
                    "side": side,
                    "broker_order_id": broker_order_id,
                    "order_status": status_code,
                    "cancel_result": cancel_result,
                    "reason": "protected_sell_cancel",
                }
            )
            self._emit_execution_log(
                "protected_sell_open_order_cancelled",
                symbol=symbol,
                broker_order_id=broker_order_id,
                order_status=status_code,
                cancel_result=cancel_result,
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
        def _plan_price(symbol: str, row: pd.Series) -> Decimal:
            close_price = float(row.get("close") or 0.0)
            if close_price <= 0:
                raise RuntimeError(
                    f"计划价格缺少有效收盘价: symbol={symbol} trade_date={latest_date.date()}"
                )
            return Decimal(str(round(close_price, 4)))

        remaining_trade_capacity = {
            str(symbol): int(volume_units_to_shares(float(row.get("avg_volume_20_prev", 0.0) or 0.0)) * self.cfg.max_trade_volume_ratio)
            for symbol, row in day_frame.iterrows()
        }
        target_set = set(target_meta["targets"])
        forced_exit_symbols = set(target_meta.get("execution_day_forced_exit_symbols") or [])
        protected_sell_symbols = self._protected_sell_symbols()
        forced_exit_details = self._forced_exit_detail_map(target_meta)
        each_target_value = float(target_meta["each_target_value"])
        planning_state = self._clone_account_state(live_state)
        planned_orders: list[PlannedOrder] = []
        forced_exit_untradable_symbols: set[str] = set()

        for symbol in list(planning_state.positions):
            if symbol in protected_sell_symbols:
                continue
            if symbol not in forced_exit_symbols and symbol in target_set:
                continue
            if symbol not in day_frame.index:
                if symbol in forced_exit_symbols:
                    forced_exit_untradable_symbols.add(symbol)
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
                if symbol in forced_exit_symbols:
                    forced_exit_untradable_symbols.add(symbol)
                continue
            row = day_frame.loc[symbol]
            sell_price = _plan_price(symbol, row)
            if not can_trade(symbol, latest_date, float(sell_price), volumes.get(symbol, 0.0), float(row.get("prev_close") or 0.0), is_buy=False):
                if symbol in forced_exit_symbols:
                    forced_exit_untradable_symbols.add(symbol)
                continue
            order = PlannedOrder(
                symbol=symbol,
                side=OrderSide.SELL,
                qty=sell_qty,
                price=sell_price,
                reason="execution_day_special_treatment_exit" if symbol in forced_exit_symbols else "not_in_target",
                metadata=(
                    {"forced_exit": forced_exit_details.get(symbol, {}), "target_symbols": target_meta["targets"]}
                    if symbol in forced_exit_symbols
                    else {"target_symbols": target_meta["targets"]}
                ),
            )
            planned_orders.append(order)
            self._apply_planned_order(planning_state, order)
            remaining_trade_capacity[symbol] = max(0, remaining_trade_capacity.get(symbol, 0) - sell_qty)

        target_meta["forced_exit_untradable_symbols"] = sorted(forced_exit_untradable_symbols)

        for symbol in target_meta["targets"]:
            if symbol in protected_sell_symbols or symbol in forced_exit_symbols or symbol not in planning_state.positions or symbol not in day_frame.index:
                continue
            position = planning_state.positions[symbol]
            row = day_frame.loc[symbol]
            sell_price = _plan_price(symbol, row)
            current_value = float(position.qty) * float(sell_price)
            if current_value <= each_target_value * self.cfg.max_overweight_ratio:
                continue
            target_amount = calc_target_amount_by_value(symbol, each_target_value, float(sell_price))
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
            if symbol in forced_exit_symbols or symbol not in day_frame.index:
                continue
            row = day_frame.loc[symbol]
            if bool(row.get("st_preannounce_block_buy")):
                continue
            buy_price = _plan_price(symbol, row)
            current_qty = planning_state.positions.get(symbol).qty if symbol in planning_state.positions else 0
            target_qty = calc_target_amount_by_value(symbol, each_target_value, float(buy_price))
            desired_buy = max(int(target_qty) - int(current_qty), 0)
            if desired_buy <= 0:
                continue
            if not can_trade(symbol, latest_date, float(buy_price), volumes.get(symbol, 0.0), float(row.get("prev_close") or 0.0), is_buy=True):
                continue
            current_value = float(current_qty) * float(buy_price)
            gap_value = each_target_value - current_value
            buy_plan.append((symbol, gap_value))
        buy_plan.sort(key=lambda item: item[1], reverse=True)

        for symbol, gap_value in buy_plan:
            row = day_frame.loc[symbol]
            if bool(row.get("st_preannounce_block_buy")):
                continue
            buy_price = _plan_price(symbol, row)
            current_qty = planning_state.positions.get(symbol).qty if symbol in planning_state.positions else 0
            target_qty = calc_target_amount_by_value(symbol, each_target_value, float(buy_price))
            desired_buy = max(int(target_qty) - int(current_qty), 0)
            buy_qty = available_trade_shares(
                symbol,
                desired_shares=desired_buy,
                remaining_shares=remaining_trade_capacity.get(symbol, 0),
                current_amount=current_qty,
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
                metadata={
                    "target_value": round(each_target_value, 2),
                    "gap_value": round(gap_value, 2),
                    "target_qty": int(target_qty),
                },
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
        quotes: dict[str, Any],
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
            sell_orders = [item for item in planned_orders if item.side == OrderSide.SELL]
            buy_orders = [item for item in planned_orders if item.side == OrderSide.BUY]
            sell_turnover, sell_pending_orders = self._submit_planned_order_batch(
                sell_orders,
                latest_date=latest_date,
                planning_state=planning_state,
                prices=prices,
                quotes=quotes,
                risk_service=risk_service,
                oms_service=oms_service,
                session=session,
                qmt_enabled=qmt_enabled,
                drain_pending=False,
            )
            turnover += sell_turnover
            if qmt_enabled and sell_orders and buy_orders:
                planning_state = self._refresh_planning_state_after_sell_batch(planning_state, prices, quotes, session)
            buy_turnover, buy_pending_orders = self._submit_planned_order_batch(
                buy_orders,
                latest_date=latest_date,
                planning_state=planning_state,
                prices=prices,
                quotes=quotes,
                risk_service=risk_service,
                oms_service=oms_service,
                session=session,
                qmt_enabled=qmt_enabled,
                drain_pending=False,
            )
            turnover += buy_turnover
            if qmt_enabled:
                pending_retry_orders = list(sell_pending_orders) + list(buy_pending_orders)
                if pending_retry_orders:
                    self._drain_pending_retry_orders(pending_retry_orders, prices, session)

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
                        "st_risk_blocked_count": target_meta.get("st_risk_blocked_count", 0),
                        "st_risk_blocked_symbols": target_meta.get("st_risk_blocked_symbols", []),
                        "st_risk_sell_watch": target_meta.get("st_risk_sell_watch", []),
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

    def _submit_planned_order_batch(
        self,
        planned_orders: list[PlannedOrder],
        *,
        latest_date: pd.Timestamp,
        planning_state: AccountState,
        prices: dict[str, Decimal],
        quotes: dict[str, Any],
        risk_service: RiskService,
        oms_service: OmsService,
        session,
        qmt_enabled: bool,
        drain_pending: bool = True,
    ) -> tuple[Decimal, list[PendingRetryOrder]]:
        turnover = Decimal("0")
        pending_retry_orders: list[PendingRetryOrder] = []
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
                self._emit_execution_log(
                    "risk_rejected",
                    symbol=planned.symbol,
                    side=planned.side.value,
                    qty=planned.qty,
                    reason=planned.reason,
                )
                continue
            order = oms_service.create_order(intent, decision)
            executed_planned = planned
            if qmt_enabled:
                submission_result = self._submit_order_without_blocking(planned, prices, quotes, session)
                broker_order_id = submission_result["broker_order_id"]
                executed_price = submission_result["submitted_price"]
                executed_planned = replace(planned, price=executed_price)
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
                            "price": float(executed_price),
                            "price_source": submission_result.get("price_source"),
                            "retry_attempts": submission_result["attempts"],
                            "remaining_qty": submission_result["remaining_qty"],
                            "pending_retry": bool(submission_result.get("pending_retry")),
                        },
                    )
                )
                pending_order = submission_result.get("pending_order")
                if isinstance(pending_order, PendingRetryOrder):
                    pending_retry_orders.append(pending_order)
                self._emit_execution_log(
                    "order_submitted",
                    symbol=planned.symbol,
                    side=planned.side.value,
                    qty=planned.qty,
                    submitted_price=float(executed_price),
                    price_source=submission_result.get("price_source"),
                    broker_order_id=broker_order_id,
                    retry_attempts=submission_result["attempts"],
                    remaining_qty=submission_result["remaining_qty"],
                    pending_retry=bool(submission_result.get("pending_retry")),
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
            turnover += Decimal(str(executed_planned.qty)) * executed_planned.price
            self._apply_planned_order(planning_state, executed_planned)
        if qmt_enabled and drain_pending and pending_retry_orders:
            self._drain_pending_retry_orders(pending_retry_orders, prices, session)
            pending_retry_orders = []
        return turnover, pending_retry_orders

    def _refresh_planning_state_after_sell_batch(
        self,
        planning_state: AccountState,
        prices: dict[str, Decimal],
        quotes: dict[str, Any],
        session,
    ) -> AccountState:
        try:
            snapshot_payload = self.bridge.get_account_snapshot()
            refreshed_state, _reported_total_asset = self._build_account_state(snapshot_payload, prices)
            refreshed_quotes = self._get_quotes_payload(sorted(set(quotes) | set(refreshed_state.positions)), allow_quote_fallback=True)
            quotes.clear()
            quotes.update(refreshed_quotes)
            merged_state = self._clone_account_state(planning_state)
            merged_state.cash = max(Decimal(str(planning_state.cash)), Decimal(str(refreshed_state.cash)))
            merged_state.frozen_cash = Decimal(str(refreshed_state.frozen_cash))
            merged_state.peak_total_asset = max(
                Decimal(str(planning_state.peak_total_asset)),
                Decimal(str(refreshed_state.peak_total_asset)),
            )
            for symbol, position in refreshed_state.positions.items():
                if symbol in merged_state.positions:
                    merged_state.positions[symbol].available_qty = position.available_qty
                    merged_state.positions[symbol].last_price = position.last_price
                else:
                    merged_state.positions[symbol] = Position(
                        symbol=position.symbol,
                        qty=position.qty,
                        available_qty=position.available_qty,
                        cost_price=Decimal(str(position.cost_price)),
                        last_price=Decimal(str(position.last_price)),
                    )
            self._emit_execution_log(
                "sell_batch_refreshed_snapshot",
                account_id=refreshed_state.account_id,
                cash=float(refreshed_state.cash),
                optimistic_cash=float(merged_state.cash),
                position_count=len(refreshed_state.positions),
            )
            session.add(
                AuditLogModel(
                    object_type="account_snapshot",
                    object_id=refreshed_state.account_id,
                    message="卖出批次后已刷新账户状态，后续买单将基于最新快照与卖出乐观现金合并评估",
                    payload={
                        "positions": len(refreshed_state.positions),
                        "cash": float(refreshed_state.cash),
                        "optimistic_cash": float(merged_state.cash),
                    },
                )
            )
            return merged_state
        except QmtUnavailableError as exc:
            self._emit_execution_log(
                "sell_batch_refresh_failed",
                account_id=planning_state.account_id,
                error=str(exc),
            )
            session.add(
                AuditLogModel(
                    object_type="account_snapshot",
                    object_id=planning_state.account_id,
                    message="卖出批次后账户快照刷新失败，后续买单继续使用本地计划状态",
                    payload={"error": str(exc)},
                )
            )
            return planning_state

    def _submit_order_without_blocking(
        self,
        planned: PlannedOrder,
        prices: dict[str, Decimal],
        quotes: dict[str, Any],
        session,
    ) -> dict[str, Any]:
        return self._submit_order_attempt(planned, int(planned.qty), prices, session, attempts=1, quote=quotes.get(planned.symbol, {}))

    def _submit_order_attempt(
        self,
        planned: PlannedOrder,
        submitted_qty: int,
        prices: dict[str, Decimal],
        session,
        *,
        attempts: int,
        quote: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        current_price, price_source = self._resolve_submit_price(planned, prices, quote=quote)
        result = self.bridge.submit_order(
            planned.symbol,
            planned.side.value,
            submitted_qty,
            current_price,
            strategy_name="joinquant-microcap-paper" if self.app_settings.environment == Environment.PAPER else "joinquant-microcap-live",
            order_remark=planned.reason,
        )
        broker_order_id = str(result.get("order_id") or "")
        wait_result = self._check_order_status_once(broker_order_id, submitted_qty)
        filled_qty = int(wait_result.get("filled_qty") or 0)
        remaining_qty = max(submitted_qty - filled_qty, 0)
        pending_order = None
        if broker_order_id and remaining_qty > 0 and wait_result.get("status_available", True):
            pending_order = PendingRetryOrder(
                planned=planned,
                broker_order_id=broker_order_id,
                submitted_qty=submitted_qty,
                remaining_qty=remaining_qty,
                attempts=attempts,
                submitted_at=datetime.now(),
                price_source=price_source,
            )
        session.add(
            AuditLogModel(
                object_type="order_retry",
                object_id=broker_order_id or planned.symbol,
                message=f"{planned.symbol} 第 {attempts} 次委托提交后已检查状态",
                payload={
                    "attempt": attempts,
                    "submitted_qty": submitted_qty,
                    "filled_qty": filled_qty,
                    "remaining_qty": remaining_qty,
                    "status_text": wait_result.get("status_text"),
                    "status_code": wait_result.get("status_code"),
                    "timed_out": bool(wait_result.get("timed_out")),
                    "pending_retry": pending_order is not None,
                    "submitted_price": float(current_price),
                    "price_source": price_source,
                },
            )
        )
        self._emit_execution_log(
            "order_checked",
            symbol=planned.symbol,
            side=planned.side.value,
            qty=submitted_qty,
            attempt=attempts,
            broker_order_id=broker_order_id,
            submitted_price=float(current_price),
            price_source=price_source,
            filled_qty=filled_qty,
            remaining_qty=remaining_qty,
            status_text=wait_result.get("status_text"),
            status_code=wait_result.get("status_code"),
            pending_retry=pending_order is not None,
        )
        return {
            "broker_order_id": broker_order_id,
            "attempts": attempts,
            "remaining_qty": remaining_qty,
            "pending_retry": pending_order is not None,
            "pending_order": pending_order,
            "submitted_price": current_price,
            "price_source": price_source,
        }

    def _drain_pending_retry_orders(
        self,
        pending_orders: list[PendingRetryOrder],
        prices: dict[str, Decimal],
        session,
    ) -> None:
        active_orders = list(pending_orders)
        while active_orders:
            time.sleep(max(1, int(self.ORDER_RETRY_POLL_SECONDS)))
            next_round: list[PendingRetryOrder] = []
            for pending in active_orders:
                wait_result = self._check_order_status_once(pending.broker_order_id, pending.submitted_qty)
                filled_qty = int(wait_result.get("filled_qty") or 0)
                remaining_qty = max(pending.submitted_qty - filled_qty, 0)
                session.add(
                    AuditLogModel(
                        object_type="order_retry",
                        object_id=pending.broker_order_id or pending.planned.symbol,
                        message=f"{pending.planned.symbol} 挂单轮询检查完成",
                        payload={
                            "attempt": pending.attempts,
                            "submitted_qty": pending.submitted_qty,
                            "filled_qty": filled_qty,
                            "remaining_qty": remaining_qty,
                            "status_text": wait_result.get("status_text"),
                            "status_code": wait_result.get("status_code"),
                            "timed_out": bool(wait_result.get("timed_out")),
                        },
                    )
                )
                self._emit_execution_log(
                    "pending_order_checked",
                    symbol=pending.planned.symbol,
                    side=pending.planned.side.value,
                    attempt=pending.attempts,
                    broker_order_id=pending.broker_order_id,
                    filled_qty=filled_qty,
                    remaining_qty=remaining_qty,
                    status_text=wait_result.get("status_text"),
                    status_code=wait_result.get("status_code"),
                )
                if remaining_qty <= 0:
                    continue
                if not wait_result.get("status_available", True):
                    session.add(
                        AuditLogModel(
                            object_type="order_retry",
                            object_id=pending.broker_order_id or pending.planned.symbol,
                            message=f"{pending.planned.symbol} 暂时无法读取订单状态，本轮不执行补挂",
                            payload={"attempt": pending.attempts, "remaining_qty": remaining_qty},
                        )
                    )
                    self._emit_execution_log(
                        "pending_order_status_unavailable",
                        symbol=pending.planned.symbol,
                        side=pending.planned.side.value,
                        attempt=pending.attempts,
                        broker_order_id=pending.broker_order_id,
                        remaining_qty=remaining_qty,
                    )
                    continue
                refreshed_price, refreshed_price_source = self._resolve_submit_price(pending.planned, prices)
                should_requote = pending.price_source == "plan_fallback" and refreshed_price_source != "plan_fallback"
                canceled_active_order = False
                if wait_result.get("is_active", False) and pending.planned.side == OrderSide.SELL:
                    latest_quote = self._get_quotes_payload([pending.planned.symbol], allow_quote_fallback=True).get(pending.planned.symbol, {})
                    hold_special_treatment_exit = pending.planned.reason == "execution_day_special_treatment_exit"
                    if self._quote_indicates_limit_down(latest_quote) or hold_special_treatment_exit:
                        session.add(
                            AuditLogModel(
                                object_type="order_retry",
                                object_id=pending.broker_order_id or pending.planned.symbol,
                                message=f"{pending.planned.symbol} 卖单检测到跌停或属于特殊处理强制卖出，保留当前挂单，不执行撤单补挂",
                                payload={
                                    "attempt": pending.attempts,
                                    "remaining_qty": remaining_qty,
                                    "price_source": pending.price_source,
                                    "reason": pending.planned.reason,
                                    "limit_down_detected": self._quote_indicates_limit_down(latest_quote),
                                },
                            )
                        )
                        self._emit_execution_log(
                            "pending_order_limit_down_hold",
                            symbol=pending.planned.symbol,
                            side=pending.planned.side.value,
                            attempt=pending.attempts,
                            broker_order_id=pending.broker_order_id,
                            remaining_qty=remaining_qty,
                            reason=pending.planned.reason,
                        )
                        continue
                if should_requote and wait_result.get("is_active", False):
                    cancel_result = self._cancel_broker_order(pending.broker_order_id)
                    session.add(
                        AuditLogModel(
                            object_type="order_retry",
                            object_id=pending.broker_order_id or pending.planned.symbol,
                            message=f"{pending.planned.symbol} 已获取新的盘口买一/卖一，撤销计划价旧单并准备按盘口重挂",
                            payload={
                                "attempt": pending.attempts,
                                "cancel_result": cancel_result,
                                "remaining_qty": remaining_qty,
                                "previous_price_source": pending.price_source,
                                "next_price_source": refreshed_price_source,
                                "next_price": float(refreshed_price),
                            },
                        )
                    )
                    self._emit_execution_log(
                        "pending_order_requote",
                        symbol=pending.planned.symbol,
                        side=pending.planned.side.value,
                        attempt=pending.attempts,
                        broker_order_id=pending.broker_order_id,
                        remaining_qty=remaining_qty,
                        previous_price_source=pending.price_source,
                        next_price_source=refreshed_price_source,
                        next_price=float(refreshed_price),
                        cancel_result=cancel_result,
                    )
                    if cancel_result != 0:
                        pending.remaining_qty = remaining_qty
                        pending.submitted_at = datetime.now()
                        next_round.append(pending)
                        continue
                    canceled_active_order = True
                is_stale = (datetime.now() - pending.submitted_at).total_seconds() >= float(self.ORDER_RETRY_TIMEOUT_SECONDS)
                if wait_result.get("is_active", False) and not is_stale and not should_requote:
                    pending.remaining_qty = remaining_qty
                    next_round.append(pending)
                    continue
                if wait_result.get("is_active", False) and not canceled_active_order:
                    cancel_result = self._cancel_broker_order(pending.broker_order_id)
                    session.add(
                        AuditLogModel(
                            object_type="order_retry",
                            object_id=pending.broker_order_id or pending.planned.symbol,
                            message=f"{pending.planned.symbol} 未在超时前完成成交，已尝试撤单并准备补挂",
                            payload={
                                "attempt": pending.attempts,
                                "cancel_result": cancel_result,
                                "remaining_qty": remaining_qty,
                            },
                        )
                    )
                    self._emit_execution_log(
                        "pending_order_stale_cancel",
                        symbol=pending.planned.symbol,
                        side=pending.planned.side.value,
                        attempt=pending.attempts,
                        broker_order_id=pending.broker_order_id,
                        remaining_qty=remaining_qty,
                        cancel_result=cancel_result,
                    )
                    if cancel_result != 0:
                        pending.remaining_qty = remaining_qty
                        pending.submitted_at = datetime.now()
                        next_round.append(pending)
                        continue
                if pending.attempts >= self.ORDER_RETRY_MAX_ATTEMPTS:
                    session.add(
                        AuditLogModel(
                            object_type="order_retry",
                            object_id=pending.broker_order_id or pending.planned.symbol,
                            message=f"{pending.planned.symbol} 未成交剩余数量已达到最大补挂次数，停止继续补挂",
                            payload={"attempt": pending.attempts, "remaining_qty": remaining_qty},
                        )
                    )
                    self._emit_execution_log(
                        "pending_order_retry_stopped",
                        symbol=pending.planned.symbol,
                        side=pending.planned.side.value,
                        attempt=pending.attempts,
                        remaining_qty=remaining_qty,
                    )
                    continue
                resubmit_result = self._submit_order_attempt(
                    pending.planned,
                    remaining_qty,
                    prices,
                    session,
                    attempts=pending.attempts + 1,
                    quote={},
                )
                next_pending = resubmit_result.get("pending_order")
                if isinstance(next_pending, PendingRetryOrder):
                    next_round.append(next_pending)
            active_orders = next_round

    def _check_order_status_once(self, broker_order_id: str, submitted_qty: int) -> dict[str, Any]:
        if not broker_order_id:
            return {
                "status_available": False,
                "status_text": "",
                "status_code": -1,
                "filled_qty": 0,
                "is_active": False,
                "timed_out": False,
            }
        try:
            payload = self.bridge.get_order_status(broker_order_id)
        except QmtUnavailableError:
            return {
                "status_available": False,
                "status_text": "",
                "status_code": -1,
                "filled_qty": 0,
                "is_active": False,
                "timed_out": False,
            }
        return self._parse_order_status(payload, submitted_qty)

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

    def _resolve_submit_price(
        self,
        planned: PlannedOrder,
        prices: dict[str, Decimal],
        *,
        quote: dict[str, Any] | None = None,
    ) -> tuple[Decimal, str]:
        latest = self._resolve_quote_price(quote or {}, is_buy=planned.side == OrderSide.BUY, allow_last_price=False)
        if latest > 0:
            source = "best_ask" if planned.side == OrderSide.BUY else "best_bid"
            return Decimal(str(round(latest, 4))), source
        try:
            quotes = self.bridge.get_quotes([planned.symbol])
            refreshed_quote = quotes.get(planned.symbol, {}) if isinstance(quotes, dict) else {}
            latest = self._resolve_quote_price(refreshed_quote, is_buy=planned.side == OrderSide.BUY, allow_last_price=False)
            if latest > 0:
                source = "best_ask_refresh" if planned.side == OrderSide.BUY else "best_bid_refresh"
                return Decimal(str(round(latest, 4))), source
        except QmtUnavailableError:
            pass
        return prices.get(planned.symbol, planned.price), "plan_fallback"

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
            if market_price.is_nan() or market_price <= 0:
                market_price = fallback_prices.get(symbol, cost_price if cost_price > 0 else Decimal("0.01"))
            if market_price.is_nan() or market_price <= 0:
                market_price = Decimal("0.01")
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
        if reported_total.is_nan() or reported_total <= 0:
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
        try:
            requested_total_asset = Decimal(str(initial_cash))
        except Exception:
            return reported_total_asset
        if reported_total_asset.is_nan() or reported_total_asset <= 0:
            return requested_total_asset if requested_total_asset > 0 else Decimal("0")
        if requested_total_asset <= 0:
            return reported_total_asset
        return min(reported_total_asset, requested_total_asset)

    def _apply_strategy_capital(
        self,
        state: AccountState,
        strategy_total_asset: Decimal,
        prices: dict[str, Decimal],
    ) -> Decimal:
        if strategy_total_asset.is_nan() or strategy_total_asset <= 0:
            return state.total_asset(prices)
        position_value = Decimal("0")
        for symbol, position in state.positions.items():
            current_price = prices.get(symbol, position.last_price)
            if current_price.is_nan() or current_price <= 0:
                current_price = Decimal("0.01")
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

    def _normalize_preview_live_state_for_next_session(self, state: AccountState) -> AccountState:
        normalized = self._clone_account_state(state)
        for position in normalized.positions.values():
            # 预览的是下一交易日计划，收盘后当日买入的仓位到次日开盘即转为可卖。
            position.available_qty = max(position.available_qty, position.qty)
        return normalized

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
    def _resolve_quote_price(quote: dict[str, Any], *, is_buy: bool, allow_last_price: bool = True) -> float:
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
        if not allow_last_price:
            return 0.0
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
            number = float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0
        if pd.isna(number) or number == float("inf") or number == float("-inf"):
            return 0.0
        return number

    @staticmethod
    def _coerce_boolish(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        text = str(value or "").strip().lower()
        return text in {"1", "true", "yes", "y", "on", "halt", "paused", "suspended", "停牌"}

    @classmethod
    def _quote_indicates_halt(cls, quote: dict[str, Any]) -> bool:
        if not isinstance(quote, dict) or not quote:
            return False
        for key in (
            "paused",
            "is_paused",
            "suspended",
            "is_suspended",
            "halted",
            "is_halted",
            "停牌",
            "是否停牌",
        ):
            if key in quote and cls._coerce_boolish(quote.get(key)):
                return True
        status_text = cls._pick_text(quote, "status", "trade_status", "security_status", default="").lower()
        return any(token in status_text for token in ("halt", "pause", "suspend", "停牌"))

    @classmethod
    def _quote_indicates_limit_down(cls, quote: dict[str, Any]) -> bool:
        if not isinstance(quote, dict) or not quote:
            return False
        low_limit = cls._pick_number(quote, "low_limit", "limit_down", "跌停价", default=0.0)
        if low_limit <= 0:
            return False
        last_price = cls._pick_number(quote, "last_price", "price", default=0.0)
        bid_price = cls._pick_number(quote, "bid_price", "bid1", "buy1_price", default=0.0)
        compare_price = 0.0
        if bid_price > 0:
            compare_price = bid_price
        elif last_price > 0:
            compare_price = last_price
        else:
            compare_price = low_limit
        return compare_price <= low_limit * 1.001

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
