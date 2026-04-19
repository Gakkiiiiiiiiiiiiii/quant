from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
from sqlalchemy import delete
from sqlalchemy.orm import sessionmaker

from quant_demo.adapters.qmt.gateway import create_gateway
from quant_demo.audit.report_service import AuditReportService
from quant_demo.core.config import AppSettings, StrategySettings
from quant_demo.core.enums import Environment
from quant_demo.core.events import AccountState, AuditRecord
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
from quant_demo.db.repositories.audit import AuditRepository
from quant_demo.db.repositories.snapshots import SnapshotsRepository
from quant_demo.db.repositories.trades import TradesRepository
from quant_demo.execution.router import build_trade_client
from quant_demo.execution.service import ExecutionService
from quant_demo.experiment.evaluator import EvaluationResult, Evaluator
from quant_demo.experiment.joinquant_microcap_engine import JoinQuantMicrocapBacktestEngine
from quant_demo.experiment.qmt_microcap_trading import QmtMicrocapTradingEngine
from quant_demo.experiment.qlib_engine import QlibBacktestEngine
from quant_demo.marketdata.readers import group_bars_by_date, prices_from_bars
from quant_demo.oms.intent_builder import build_order_intents
from quant_demo.oms.service import OmsService
from quant_demo.portfolio.rebalancer import Rebalancer
from quant_demo.risk.rules.cash_check import CashCheckRule
from quant_demo.risk.rules.daily_loss_limit import DailyLossLimitRule
from quant_demo.risk.rules.position_limit import PositionLimitRule
from quant_demo.risk.rules.trading_window import TradingWindowRule
from quant_demo.risk.service import RiskService
from quant_demo.strategy.base import StrategyContext
from quant_demo.strategy.implementations.etf_rotation import EtfRotationStrategy
from quant_demo.strategy.implementations.first_alpha import FirstAlphaStrategy
from quant_demo.strategy.implementations.joinquant_style import JoinQuantStyleStrategy
from quant_demo.strategy.implementations.stock_ranking import StockRankingStrategy
from quant_demo.strategy.registry import StrategyRegistry


@dataclass(slots=True)
class RunResult:
    report_path: Path
    metrics: EvaluationResult
    equity_curve: pd.DataFrame


class ExperimentManager:
    def __init__(self, session_factory: sessionmaker, app_settings: AppSettings, strategy_settings: StrategySettings) -> None:
        self.session_factory = session_factory
        self.app_settings = app_settings
        self.strategy_settings = strategy_settings
        self.registry = StrategyRegistry()
        self.registry.register(EtfRotationStrategy(strategy_settings.lookback_days, strategy_settings.top_n))
        self.registry.register(StockRankingStrategy(strategy_settings.lookback_days, strategy_settings.top_n))
        self.registry.register(FirstAlphaStrategy(strategy_settings.lookback_days, strategy_settings.top_n))
        self.registry.register(
            JoinQuantStyleStrategy(
                strategy_settings.lookback_days,
                strategy_settings.top_n,
                strategy_settings.extra,
            )
        )

    def _should_rebalance(self, index: int) -> bool:
        if self.strategy_settings.rebalance_frequency == "daily":
            return True
        return index == 0 or index % 5 == 0

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

    def run(self, initial_cash: Decimal = Decimal("100000")) -> RunResult:
        if self.strategy_settings.implementation in {
            "joinquant_microcap_alpha",
            "joinquant_microcap_alpha_zf",
            "joinquant_microcap_alpha_zfe",
            "joinquant_microcap_alpha_zr",
            "joinquant_microcap_alpha_zro",
            "monster_prelude_alpha",
            "microcap_100b_layer_rot",
            "microcap_50b_layer_rot",
            "industry_weighted_microcap_alpha",
        }:
            engine_cls = JoinQuantMicrocapBacktestEngine if self.app_settings.environment == Environment.BACKTEST else QmtMicrocapTradingEngine
            report_path, metrics, equity_curve = engine_cls(
                self.session_factory,
                self.app_settings,
                self.strategy_settings,
            ).run(initial_cash)
            return RunResult(report_path=report_path, metrics=metrics, equity_curve=equity_curve)
        if self.app_settings.environment == Environment.BACKTEST and self.app_settings.backtest_engine.lower() == "qlib":
            report_path, metrics, equity_curve = QlibBacktestEngine(
                self.session_factory,
                self.app_settings,
                self.strategy_settings,
            ).run(initial_cash)
            return RunResult(report_path=report_path, metrics=metrics, equity_curve=equity_curve)
        return self._run_native(initial_cash)

    def _run_native(self, initial_cash: Decimal) -> RunResult:
        gateway = create_gateway(self.app_settings)
        history = gateway.quote_client.load_history(self.app_settings.symbols, self.app_settings.history_parquet)
        grouped = group_bars_by_date(history)
        strategy = self.registry.get(self.strategy_settings.implementation)
        account_state = AccountState(account_id="demo-account", cash=initial_cash)
        rebalancer = Rebalancer(lot_size=self.strategy_settings.lot_size, max_position_ratio=self.app_settings.risk.max_position_ratio)
        risk_service = RiskService(
            [
                CashCheckRule(),
                PositionLimitRule(self.app_settings.risk.max_position_ratio),
                TradingWindowRule(self.app_settings.environment, self.app_settings.risk.trading_start, self.app_settings.risk.trading_end),
                DailyLossLimitRule(self.app_settings.risk.daily_loss_limit),
            ]
        )
        execution_service = ExecutionService(build_trade_client(self.app_settings))
        equity_rows: list[dict] = []

        with self.session_factory() as session:
            self._reset_non_live_state(session)
            oms_service = OmsService(session)
            trades_repo = TradesRepository(session)
            snapshots_repo = SnapshotsRepository(session)
            audit_repo = AuditRepository(session)
            for index, (trading_date, bars) in enumerate(sorted(grouped.items(), key=lambda item: item[0])):
                prices = prices_from_bars(bars)
                if self._should_rebalance(index):
                    context = StrategyContext(
                        trading_date=trading_date,
                        account_state=account_state,
                        history=history,
                        bars=bars,
                        prices=prices,
                    )
                    target_weights = strategy.target_weights(context)
                    instructions = rebalancer.build_instructions(account_state, target_weights, prices)
                    intents = build_order_intents(account_state.account_id, trading_date, instructions)
                    for intent in intents:
                        decision = risk_service.evaluate(intent, account_state, prices)
                        oms_service.register_intent(intent, decision)
                        audit_repo.add_log(
                            AuditRecord(
                                object_type="risk_decision",
                                object_id=decision.risk_decision_id,
                                message=f"{intent.symbol} 风控{'通过' if decision.is_approved() else '拒绝'}",
                                payload={"rules": [asdict(result) for result in decision.rule_results]},
                            )
                        )
                        if not decision.is_approved():
                            continue
                        order = oms_service.create_order(intent, decision)
                        oms_service.submit_order(order)
                        fill = execution_service.execute(order, prices[intent.symbol])
                        execution_service.apply_fill(account_state, fill)
                        oms_service.fill_order(order, fill.fill_qty, fill.fill_price)
                        trades_repo.add_trade(fill)
                        audit_repo.add_log(
                            AuditRecord(
                                object_type="trade",
                                object_id=fill.trade_id,
                                message=f"{fill.symbol} 成交 {fill.fill_qty} 股",
                                payload={"price": float(fill.fill_price), "commission": float(fill.commission)},
                            )
                        )
                equity = account_state.total_asset(prices)
                peak = account_state.peak_total_asset or equity
                drawdown = (equity - peak) / peak if peak else Decimal("0")
                snapshots_repo.add_snapshots(account_state, prices, datetime.combine(trading_date, datetime.min.time()), equity - initial_cash, drawdown)
                equity_rows.append({"trading_date": trading_date, "equity": float(equity), "turnover": float(account_state.turnover)})
            session.commit()
            report_path = AuditReportService().write_daily_report(session, self.app_settings.report_dir)
        equity_curve = pd.DataFrame(equity_rows)
        metrics = Evaluator().evaluate(equity_curve)
        return RunResult(report_path=report_path, metrics=metrics, equity_curve=equity_curve)
