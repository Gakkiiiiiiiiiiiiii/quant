from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import delete
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
    def __init__(self, session_factory: sessionmaker, app_settings: AppSettings, strategy_settings: StrategySettings) -> None:
        self.session_factory = session_factory
        self.app_settings = app_settings
        self.strategy_settings = strategy_settings
        self.cfg = MicrocapStrategyConfig.from_strategy_settings(strategy_settings)
        self.bridge = QmtBridgeClient(app_settings)

    def run(self, initial_cash: Decimal) -> tuple[Path, EvaluationResult, pd.DataFrame]:
        _ = initial_cash
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

        target_meta = self._build_target_metadata(latest_date, day_frame, live_state, reported_total_asset)
        quote_symbols = sorted(set(target_meta["targets"]) | set(live_state.positions))
        quotes = self.bridge.get_quotes(quote_symbols) if quote_symbols else {}
        price_map, volume_map = self._build_quote_maps(day_frame, quotes)
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
        )
        metrics = Evaluator().evaluate(equity_curve)
        return report_path, metrics, equity_curve

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
        history = loader._load_history()
        symbols = history["symbol"].dropna().astype(str).unique().tolist()
        instrument_frame = loader._load_instrument_frame(symbols)
        capital_frame = loader._load_capital_frame(symbols)
        prepared = loader._prepare_history(history, instrument_frame, capital_frame)
        return prepared, instrument_frame

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
                        message=f"{planned.symbol} 风控{'通过' if decision.is_approved() else '拒绝'}",
                        payload={"reason": planned.reason, "rules": [asdict(result) for result in decision.rule_results]},
                    )
                )
                if not decision.is_approved():
                    continue
                order = oms_service.create_order(intent, decision)
                if qmt_enabled:
                    result = self.bridge.submit_order(
                        planned.symbol,
                        planned.side.value,
                        planned.qty,
                        planned.price,
                        strategy_name="joinquant-microcap-paper" if self.app_settings.environment == Environment.PAPER else "joinquant-microcap-live",
                        order_remark=planned.reason,
                    )
                    broker_order_id = str(result.get("order_id") or "")
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
                            message=f"{planned.symbol} 已提交 QMT 委托",
                            payload={"broker_order_id": broker_order_id, "side": planned.side.value, "qty": planned.qty, "price": float(planned.price)},
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
                            message=f"{planned.symbol} 生成预演委托，未提交 QMT",
                            payload={"side": planned.side.value, "qty": planned.qty, "price": float(planned.price)},
                        )
                    )
                turnover += Decimal(str(planned.qty)) * planned.price
                self._apply_planned_order(planning_state, planned)

            snapshot_payload = self.bridge.get_account_snapshot() if qmt_enabled else account_snapshot
            asset_row, position_rows = self._build_snapshot_rows(snapshot_payload, prices)
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
                        "qmt_trade_enabled": qmt_enabled,
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
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        state, total_asset = self._build_account_state(payload, prices)
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
