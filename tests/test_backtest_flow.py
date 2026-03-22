from __future__ import annotations

from pathlib import Path

from quant_demo.core.config import load_app_settings, load_strategy_settings
from quant_demo.db.session import create_session_factory
from quant_demo.experiment.manager import ExperimentManager


def test_backtest_end_to_end(tmp_path: Path) -> None:
    database_path = tmp_path / "demo.db"
    parquet_path = tmp_path / "history.parquet"
    report_dir = tmp_path / "reports"
    app_file = tmp_path / "app.yaml"
    strategy_file = tmp_path / "strategy.yaml"

    app_file.write_text(
        "\n".join(
            [
                "app_name: test-demo",
                "environment: backtest",
                f"database_url: sqlite:///{database_path.as_posix()}",
                f"history_parquet: {parquet_path.as_posix()}",
                f"report_dir: {report_dir.as_posix()}",
                "qmt_install_dir: runtime/qmt_client/client",
                "qmt_download_url: https://example.com/qmt.rar",
                "default_strategy: etf_rotation",
                "symbols:",
                "  - 510300.SH",
                "  - 159915.SZ",
                "risk:",
                "  max_position_ratio: 0.5",
                "  daily_loss_limit: -0.2",
                "  trading_start: '09:35'",
                "  trading_end: '14:55'",
            ]
        ),
        encoding="utf-8",
    )
    strategy_file.write_text(
        "\n".join(
            [
                "name: etf_rotation",
                "implementation: etf_rotation",
                "rebalance_frequency: weekly",
                "lookback_days: 10",
                "top_n: 1",
                "lot_size: 100",
            ]
        ),
        encoding="utf-8",
    )

    app_settings = load_app_settings(app_file)
    strategy_settings = load_strategy_settings(strategy_file)
    session_factory = create_session_factory(app_settings.database_url)
    result = ExperimentManager(session_factory, app_settings, strategy_settings).run()

    assert result.report_path.exists()
    assert not result.equity_curve.empty
    assert result.metrics.turnover >= 0
