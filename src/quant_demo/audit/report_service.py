from __future__ import annotations

from datetime import datetime
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from quant_demo.db.models import AssetSnapshotModel, OrderModel, RiskDecisionModel, TradeModel


class AuditReportService:
    def write_daily_report(self, session: Session, output_dir: str | Path, report_name: str = "daily_report.md") -> Path:
        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)

        last_asset = session.scalar(select(AssetSnapshotModel).order_by(AssetSnapshotModel.snapshot_time.desc()))
        order_count = session.scalar(select(func.count()).select_from(OrderModel)) or 0
        trade_count = session.scalar(select(func.count()).select_from(TradeModel)) or 0
        risk_reject_count = session.scalar(
            select(func.count()).select_from(RiskDecisionModel).where(RiskDecisionModel.status == "rejected")
        ) or 0

        lines = [
            "# 日终审计报告",
            "",
            f"生成时间：{datetime.now().isoformat()}",
            f"总订单数：{order_count}",
            f"总成交数：{trade_count}",
            f"风控拒绝数：{risk_reject_count}",
        ]
        if last_asset is not None:
            lines.extend(
                [
                    f"最新总资产：{float(last_asset.total_asset):.2f}",
                    f"最新现金：{float(last_asset.cash):.2f}",
                    f"累计换手：{float(last_asset.turnover):.2f}",
                    f"最大回撤：{float(last_asset.max_drawdown):.4f}",
                ]
            )
        report_path = path / report_name
        report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return report_path
