"""Quant Paper Trading 服务（设计文档 §20 / §21 / §81）。

Factor Paper 的迁入目标：stock_factor 只输出 Alpha/Signal，
Quant 负责 Order / Position / Account。与回测共享 TransactionCostModel、
T+1、涨跌停/停牌阻断等执行语义（§53）。
"""
from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy import select

from quant_demo.backtest.transaction_cost import TransactionCostModel
from quant_demo.core.error_codes import EXECUTION_REJECTED
from quant_demo.db.models_market import (
    PaperAccountRow,
    PaperEquityRow,
    PaperOrderRow,
    PaperPlanRow,
    PaperPositionRow,
    PaperTradeRow,
)

DEFAULT_LOT_SIZE = 100


class PaperService:
    def __init__(self, session_factory, market_service=None) -> None:
        self._session_factory = session_factory
        self._market = market_service
        self._cost = TransactionCostModel()

    # -------------------------------------------------------------- accounts
    def create_account(self, name: str = "paper", initial_cash: float = 1_000_000.0) -> dict:
        with self._session_factory() as session:
            account = PaperAccountRow(name=name, initial_cash=initial_cash, cash=initial_cash)
            session.add(account)
            session.commit()
            return self._account_payload(account)

    def get_account(self, account_id: str) -> dict | None:
        with self._session_factory() as session:
            account = session.get(PaperAccountRow, account_id)
            return self._account_payload(account) if account else None

    # ----------------------------------------------------------------- plans
    def create_plan(self, account_id: str, targets: list[dict], time_contract: dict | None = None) -> dict:
        contract = time_contract or {}
        with self._session_factory() as session:
            if session.get(PaperAccountRow, account_id) is None:
                raise KeyError(account_id)
            plan = PaperPlanRow(
                account_id=account_id,
                signal_time=contract.get("signal_time"),
                available_at=contract.get("available_at"),
                executable_from=contract.get("executable_from"),
                targets=targets,
            )
            session.add(plan)
            session.commit()
            return {"plan_id": plan.plan_id, "account_id": account_id, "targets": targets, "time_contract": contract}

    # ---------------------------------------------------------------- orders
    def generate_orders(
        self,
        account_id: str,
        as_of: str,
        scores: list[dict] | None = None,
        plan_id: str | None = None,
        top_k: int = 10,
        idempotency_key: str | None = None,
    ) -> dict:
        execute_on = self._next_trading_day(date.fromisoformat(as_of[:10]))
        with self._session_factory() as session:
            account = session.get(PaperAccountRow, account_id)
            if account is None:
                raise KeyError(account_id)
            if idempotency_key:
                existing = session.scalars(
                    select(PaperOrderRow).where(
                        PaperOrderRow.account_id == account_id,
                        PaperOrderRow.idempotency_key == idempotency_key,
                    )
                ).all()
                if existing:
                    return {
                        "account_id": account_id,
                        "execute_on": execute_on.isoformat(),
                        "orders": [self._order_payload(order) for order in existing],
                        "reused": True,
                    }
            plan = session.get(PaperPlanRow, plan_id) if plan_id else None
            targets = plan.targets if plan is not None else self._targets_from_scores(scores, top_k)
            weight = round(0.95 / len(targets), 6) if targets else 0.0
            positions = {row.symbol: row for row in session.scalars(
                select(PaperPositionRow).where(
                    PaperPositionRow.account_id == account_id,
                    PaperPositionRow.trading_date == execute_on,
                )
            )}
            orders: list[PaperOrderRow] = []
            target_symbols = {target["symbol"] for target in targets}
            for symbol, position in sorted(positions.items()):
                if symbol not in target_symbols and position.sellable_qty > 0:
                    orders.append(
                        PaperOrderRow(
                            account_id=account_id, plan_id=plan_id, idempotency_key=idempotency_key,
                            trading_date=execute_on, symbol=symbol, side="sell", qty=position.sellable_qty,
                        )
                    )
            for target in targets:
                budget = account.cash * weight
                price = float(target.get("reference_price") or 0.0)
                if price <= 0:
                    continue
                qty = int(budget / price) // DEFAULT_LOT_SIZE * DEFAULT_LOT_SIZE
                if qty <= 0:
                    continue
                orders.append(
                    PaperOrderRow(
                        account_id=account_id, plan_id=plan_id, idempotency_key=idempotency_key,
                        trading_date=execute_on, symbol=target["symbol"], side="buy", qty=qty,
                    )
                )
            for order in orders:
                session.add(order)
            session.commit()
            return {
                "account_id": account_id,
                "execute_on": execute_on.isoformat(),
                "orders": [self._order_payload(order) for order in orders],
            }

    # ------------------------------------------------------------------- run
    def run(self, account_id: str, as_of: str, market_prices: dict[str, dict]) -> dict:
        trading_date = date.fromisoformat(as_of[:10])
        filled: list[dict] = []
        rejected: list[dict] = []
        with self._session_factory() as session:
            account = session.get(PaperAccountRow, account_id)
            if account is None:
                raise KeyError(account_id)
            orders = session.scalars(
                select(PaperOrderRow).where(
                    PaperOrderRow.account_id == account_id,
                    PaperOrderRow.trading_date == trading_date,
                    PaperOrderRow.status == "PENDING",
                )
            ).all()
            status_map = self._market.status_map(list(market_prices), trading_date, trading_date) if self._market else {}
            limit_map = self._market.limit_map(list(market_prices), trading_date, trading_date) if self._market else {}
            positions = {
                row.symbol: row
                for row in session.scalars(
                    select(PaperPositionRow).where(PaperPositionRow.account_id == account_id)
                    .order_by(PaperPositionRow.trading_date)
                )
            }
            sells = [order for order in orders if order.side == "sell"]
            buys = [order for order in orders if order.side == "buy"]
            for order in sells + buys:
                prices = market_prices.get(order.symbol)
                status = status_map.get((trading_date, order.symbol))
                if prices is None or (status is not None and status.is_suspended):
                    order.status = "REJECTED"
                    order.reject_reason = "SUSPENDED_OR_NO_QUOTE"
                    rejected.append({"order_id": order.order_id, "symbol": order.symbol, "reason": order.reject_reason})
                    continue
                fill_price = float(prices.get("open") or prices.get("close") or 0.0)
                if fill_price <= 0:
                    order.status = "REJECTED"
                    order.reject_reason = "NO_FILL_PRICE"
                    rejected.append({"order_id": order.order_id, "symbol": order.symbol, "reason": order.reject_reason})
                    continue
                limit = limit_map.get((trading_date, order.symbol))
                if limit is not None:
                    if order.side == "buy" and limit.upper_limit_price and fill_price >= limit.upper_limit_price:
                        order.status = "REJECTED"
                        order.reject_reason = "PRICE_LIMIT_UP"
                        rejected.append({"order_id": order.order_id, "symbol": order.symbol, "reason": order.reject_reason})
                        continue
                    if order.side == "sell" and limit.lower_limit_price and fill_price <= limit.lower_limit_price:
                        order.status = "REJECTED"
                        order.reject_reason = "PRICE_LIMIT_DOWN"
                        rejected.append({"order_id": order.order_id, "symbol": order.symbol, "reason": order.reject_reason})
                        continue
                fill_price = self._cost.apply_slippage(fill_price, order.side)
                position = positions.get(order.symbol)
                if order.side == "sell":
                    sellable = position.sellable_qty if position else 0
                    qty = min(order.qty, sellable)
                    if qty <= 0:
                        order.status = "REJECTED"
                        order.reject_reason = EXECUTION_REJECTED + ":T1_NO_SELLABLE"
                        rejected.append({"order_id": order.order_id, "symbol": order.symbol, "reason": "T1_NO_SELLABLE"})
                        continue
                    cost = self._cost.sell_cost(fill_price, qty)
                    account.cash += fill_price * qty - self._cost.total(cost)
                    position.qty -= qty
                    position.sellable_qty -= qty
                else:
                    cost = self._cost.buy_cost(fill_price, order.qty)
                    total = fill_price * order.qty + self._cost.total(cost)
                    if total > account.cash:
                        order.status = "REJECTED"
                        order.reject_reason = "INSUFFICIENT_CASH"
                        rejected.append({"order_id": order.order_id, "symbol": order.symbol, "reason": "INSUFFICIENT_CASH"})
                        continue
                    account.cash -= total
                    if position is None:
                        position = PaperPositionRow(
                            account_id=account_id, trading_date=trading_date, symbol=order.symbol,
                            qty=0, sellable_qty=0, avg_cost=0.0,
                        )
                        positions[order.symbol] = position
                        session.add(position)
                    new_qty = position.qty + order.qty
                    position.avg_cost = (position.avg_cost * position.qty + fill_price * order.qty) / new_qty
                    position.qty = new_qty  # T+1：当日买入不进入 sellable_qty
                order.status = "FILLED"
                trade = PaperTradeRow(
                    account_id=account_id, order_id=order.order_id, trading_date=trading_date,
                    symbol=order.symbol, side=order.side, qty=order.qty if order.side == "buy" else qty,
                    price=round(fill_price, 6), commission=cost["commission"], stamp_duty=cost["stamp_duty"],
                )
                session.add(trade)
                filled.append(self._trade_payload(trade))
            market_value = 0.0
            for symbol, position in positions.items():
                if position.qty <= 0:
                    continue
                prices = market_prices.get(symbol) or {}
                price = float(prices.get("close") or position.avg_cost)
                market_value += price * position.qty
            equity = session.scalar(
                select(PaperEquityRow).where(
                    PaperEquityRow.account_id == account_id, PaperEquityRow.trading_date == trading_date
                )
            )
            if equity is None:
                equity = PaperEquityRow(account_id=account_id, trading_date=trading_date, total_asset=0.0, cash=0.0)
                session.add(equity)
            equity.total_asset = round(account.cash + market_value, 2)
            equity.cash = round(account.cash, 2)
            equity.market_value = round(market_value, 2)
            session.commit()
        return {
            "account_id": account_id,
            "as_of": as_of,
            "filled": filled,
            "rejected": rejected,
            "equity": {"total_asset": equity.total_asset, "cash": equity.cash, "market_value": equity.market_value},
        }

    # ------------------------------------------------------------------ GETs
    def positions(self, account_id: str) -> list[dict]:
        with self._session_factory() as session:
            rows = session.scalars(
                select(PaperPositionRow)
                .where(PaperPositionRow.account_id == account_id)
                .order_by(PaperPositionRow.trading_date.desc())
            ).all()
            latest: dict[str, dict] = {}
            for row in rows:
                if row.symbol not in latest:
                    latest[row.symbol] = {
                        "symbol": row.symbol, "qty": row.qty, "sellable_qty": row.sellable_qty,
                        "avg_cost": row.avg_cost, "as_of": row.trading_date.isoformat(),
                    }
            return list(latest.values())

    def orders(self, account_id: str) -> list[dict]:
        with self._session_factory() as session:
            rows = session.scalars(
                select(PaperOrderRow).where(PaperOrderRow.account_id == account_id).order_by(PaperOrderRow.created_at)
            ).all()
            return [self._order_payload(row) for row in rows]

    def trades(self, account_id: str) -> list[dict]:
        with self._session_factory() as session:
            rows = session.scalars(
                select(PaperTradeRow).where(PaperTradeRow.account_id == account_id).order_by(PaperTradeRow.trading_date)
            ).all()
            return [self._trade_payload(row) for row in rows]

    def equity(self, account_id: str) -> list[dict]:
        with self._session_factory() as session:
            rows = session.scalars(
                select(PaperEquityRow)
                .where(PaperEquityRow.account_id == account_id)
                .order_by(PaperEquityRow.trading_date)
            ).all()
            return [
                {"date": row.trading_date.isoformat(), "total_asset": row.total_asset, "cash": row.cash, "market_value": row.market_value}
                for row in rows
            ]

    # ------------------------------------------------------------- internals
    def _targets_from_scores(self, scores: list[dict] | None, top_k: int) -> list[dict]:
        if not scores:
            return []
        ranked = sorted(scores, key=lambda item: float(item.get("score", 0.0)), reverse=True)[: max(top_k, 1)]
        return [
            {"symbol": item["symbol"], "reference_price": float(item.get("reference_price") or item.get("price") or 0.0)}
            for item in ranked
            if item.get("symbol")
        ]

    def _next_trading_day(self, day: date) -> date:
        if self._market is not None:
            candidates = self._market.trading_days(day + timedelta(days=1), day + timedelta(days=15))
            future = [candidate for candidate in candidates if candidate > day]
            if future:
                return future[0]
        next_day = day + timedelta(days=1)
        while next_day.weekday() >= 5:
            next_day += timedelta(days=1)
        return next_day

    @staticmethod
    def _account_payload(account: PaperAccountRow) -> dict:
        return {
            "account_id": account.account_id,
            "name": account.name,
            "initial_cash": account.initial_cash,
            "cash": account.cash,
            "status": account.status,
            "created_at": account.created_at.isoformat(timespec="seconds"),
        }

    @staticmethod
    def _order_payload(order: PaperOrderRow) -> dict:
        return {
            "order_id": order.order_id,
            "plan_id": order.plan_id,
            "trading_date": order.trading_date.isoformat(),
            "symbol": order.symbol,
            "side": order.side,
            "qty": order.qty,
            "status": order.status,
            "reject_reason": order.reject_reason,
        }

    @staticmethod
    def _trade_payload(trade: PaperTradeRow) -> dict:
        return {
            "trade_id": trade.trade_id,
            "order_id": trade.order_id,
            "date": trade.trading_date.isoformat(),
            "symbol": trade.symbol,
            "side": trade.side,
            "qty": trade.qty,
            "price": trade.price,
            "commission": trade.commission,
            "stamp_duty": trade.stamp_duty,
        }


__all__ = ["PaperService"]
