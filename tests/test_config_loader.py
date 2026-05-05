from __future__ import annotations

from pathlib import Path

from quant_demo.core.config import load_app_settings


def test_load_app_settings_applies_local_overlay(tmp_path: Path) -> None:
    shared_path = tmp_path / "paper.yaml"
    local_path = tmp_path / "paper.local.yaml"

    shared_path.write_text(
        "\n".join(
            [
                "app_name: demo",
                "environment: paper",
                "database_url: sqlite:///demo.db",
                "history_parquet: data/history.parquet",
                "report_dir: data/reports",
                "qmt_install_dir: runtime/qmt_client/installed",
                "qmt_download_url: https://example.com/qmt.rar",
                "default_strategy: joinquant_microcap_alpha_cc",
                "qmt_trade_enabled: false",
                "risk:",
                "  max_position_ratio: 0.30",
                "  daily_loss_limit: -0.03",
            ]
        ),
        encoding="utf-8",
    )
    local_path.write_text(
        "\n".join(
            [
                "qmt_trade_enabled: true",
                "qmt_account_id: \"acct-001\"",
                "risk:",
                "  daily_loss_limit: -0.05",
            ]
        ),
        encoding="utf-8",
    )

    settings = load_app_settings(shared_path)

    assert settings.qmt_trade_enabled is True
    assert settings.qmt_account_id == "acct-001"
    assert settings.risk.max_position_ratio == 0.30
    assert settings.risk.daily_loss_limit == -0.05
