from __future__ import annotations

from sqlalchemy.orm import Session

from quant_demo.db.models import AssetSnapshotModel, OrderModel, TradeModel


class ReconciliationService:
    def reconcile(self, session: Session) -> dict:
        order_count = session.query(OrderModel).count()
        trade_count = session.query(TradeModel).count()
        asset_count = session.query(AssetSnapshotModel).count()
        issues: list[str] = []
        if trade_count > order_count:
            issues.append("成交数大于订单数，存在数据异常")
        if asset_count == 0:
            issues.append("未发现资产快照")
        return {
            "order_count": order_count,
            "trade_count": trade_count,
            "asset_snapshot_count": asset_count,
            "issues": issues,
        }
