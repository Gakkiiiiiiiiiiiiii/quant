from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

from quant_demo.audit.report_service import AuditReportService
from quant_demo.db.models import AssetSnapshotModel
from quant_demo.db.session import create_session_factory, session_scope


def test_write_daily_report_uses_full_period_max_drawdown(tmp_path: Path) -> None:
    database_url = f"sqlite+pysqlite:///{(tmp_path / 'report_service.db').as_posix()}"
    session_factory = create_session_factory(database_url)

    with session_scope(session_factory) as session:
        session.add(
            AssetSnapshotModel(
                account_id="demo",
                cash=Decimal("50000"),
                frozen_cash=Decimal("0"),
                total_asset=Decimal("100000"),
                total_pnl=Decimal("0"),
                turnover=Decimal("1000"),
                max_drawdown=Decimal("-0.08"),
                snapshot_time=datetime(2026, 3, 23),
            )
        )
        session.add(
            AssetSnapshotModel(
                account_id="demo",
                cash=Decimal("48000"),
                frozen_cash=Decimal("0"),
                total_asset=Decimal("98000"),
                total_pnl=Decimal("-2000"),
                turnover=Decimal("1200"),
                max_drawdown=Decimal("-0.33"),
                snapshot_time=datetime(2026, 3, 24),
            )
        )

    with session_factory() as session:
        report_path = AuditReportService().write_daily_report(session, tmp_path)

    report_text = report_path.read_text(encoding="utf-8")

    assert "最大回撤：-0.3300" in report_text
