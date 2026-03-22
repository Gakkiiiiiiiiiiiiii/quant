from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant_demo.core.events import AccountState
from quant_demo.db.models import AssetSnapshotModel, PositionSnapshotModel


class SnapshotsRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add_snapshots(
        self,
        account_state: AccountState,
        prices: dict[str, Decimal],
        snapshot_time: datetime,
        total_pnl: Decimal,
        max_drawdown: Decimal,
    ) -> None:
        for position in account_state.positions.values():
            self.session.add(
                PositionSnapshotModel(
                    account_id=account_state.account_id,
                    symbol=position.symbol,
                    qty=position.qty,
                    available_qty=position.available_qty,
                    cost_price=position.cost_price,
                    market_price=prices.get(position.symbol, position.last_price),
                    snapshot_time=snapshot_time,
                )
            )
        self.session.add(
            AssetSnapshotModel(
                account_id=account_state.account_id,
                cash=account_state.cash,
                frozen_cash=account_state.frozen_cash,
                total_asset=account_state.total_asset(prices),
                total_pnl=total_pnl,
                turnover=account_state.turnover,
                max_drawdown=max_drawdown,
                snapshot_time=snapshot_time,
            )
        )

    def latest_assets(self) -> list[AssetSnapshotModel]:
        return list(self.session.scalars(select(AssetSnapshotModel).order_by(AssetSnapshotModel.snapshot_time)))
