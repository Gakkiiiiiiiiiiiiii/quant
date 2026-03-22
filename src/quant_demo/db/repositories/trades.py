from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant_demo.core.events import TradeFill
from quant_demo.db.models import TradeModel


class TradesRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add_trade(self, trade: TradeFill) -> TradeModel:
        model = TradeModel(
            trade_id=trade.trade_id,
            order_id=trade.order_id,
            symbol=trade.symbol,
            side=trade.side.value,
            fill_qty=trade.fill_qty,
            fill_price=trade.fill_price,
            commission=trade.commission,
            trade_time=trade.trade_time,
        )
        self.session.add(model)
        return model

    def list_trades(self) -> list[TradeModel]:
        return list(self.session.scalars(select(TradeModel).order_by(TradeModel.trade_time)))
