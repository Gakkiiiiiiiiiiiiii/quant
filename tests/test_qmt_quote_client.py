from __future__ import annotations

from pathlib import Path

from quant_demo.adapters.qmt.quote_client import XtQuantQuoteClient
from quant_demo.core.config import load_app_settings
from quant_demo.marketdata.ingestion import write_history_metadata


def _build_settings(tmp_path: Path):
    history_path = tmp_path / "history.parquet"
    history_path.write_text("", encoding="utf-8")
    app_file = tmp_path / "app.yaml"
    app_file.write_text(
        "\n".join(
            [
                "app_name: quote-client-test",
                "environment: paper",
                "database_url: sqlite:///demo.db",
                f"history_parquet: {history_path.as_posix()}",
                "history_source: qmt",
                "history_period: 1d",
                "history_adjustment: front",
                "history_start: '20200101'",
                "history_end: ''",
                "history_fill_data: true",
                "history_force_refresh: false",
                "history_universe_sector: 沪深京A股",
                "history_universe_limit: 0",
                "report_dir: data/reports/paper",
                "qmt_install_dir: runtime/qmt_client/installed",
                "qmt_download_url: https://example.com/qmt.rar",
                "qmt_userdata_dir: runtime/qmt_client/installed/userdata_mini",
                "qmt_bridge_python: .venv-qmt36/Scripts/python.exe",
                "qmt_bridge_script: scripts/qmt_bridge.py",
                "qmt_trade_enabled: true",
                "default_strategy: joinquant_microcap_alpha",
                "symbols: []",
            ]
        ),
        encoding="utf-8",
    )
    return load_app_settings(app_file), history_path


def test_quote_client_explicit_incremental_does_not_downgrade_to_full(tmp_path: Path) -> None:
    settings, history_path = _build_settings(tmp_path)
    write_history_metadata(
        history_path,
        {
            "source": "qmt",
            "symbols_count": 1,
            "symbols_digest": "old-digest",
            "period": "1d",
            "adjustment": "front",
            "start_time": "20200101",
            "end_time": "",
            "fill_data": True,
            "universe_sector": "沪深京A股",
            "universe_limit": 0,
        },
    )

    client = XtQuantQuoteClient(settings)
    mode = client._resolve_mode(
        history_path,
        {
            "source": "qmt",
            "symbols_count": 2,
            "symbols_digest": "new-digest",
            "period": "1d",
            "adjustment": "front",
            "start_time": "20200101",
            "end_time": "",
            "fill_data": True,
            "universe_sector": "沪深京A股",
            "universe_limit": 0,
        },
        "incremental",
    )

    assert mode == "incremental"
