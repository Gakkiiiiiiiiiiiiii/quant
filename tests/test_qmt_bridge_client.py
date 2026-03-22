from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from quant_demo.adapters.qmt.bridge_client import QmtBridgeClient
from quant_demo.core.config import load_app_settings
from quant_demo.core.exceptions import QmtUnavailableError


def build_settings(tmp_path: Path) -> Path:
    install_dir = tmp_path / "runtime" / "qmt_client" / "installed"
    userdata_dir = install_dir / "userdata_mini"
    bridge_python = tmp_path / ".venv-qmt36" / "Scripts" / "python.exe"
    bridge_script = tmp_path / "scripts" / "qmt_bridge.py"
    userdata_dir.mkdir(parents=True)
    (install_dir / "bin.x64").mkdir(parents=True)
    bridge_python.parent.mkdir(parents=True)
    bridge_script.parent.mkdir(parents=True)
    bridge_python.write_text("", encoding="utf-8")
    bridge_script.write_text("", encoding="utf-8")

    app_file = tmp_path / "app.yaml"
    app_file.write_text(
        "\n".join(
            [
                "app_name: live-test",
                "environment: live",
                "database_url: sqlite:///demo.db",
                "history_parquet: data/parquet/history.parquet",
                "history_source: qmt",
                "history_period: 1d",
                "history_adjustment: front",
                "history_start: '20200101'",
                "history_end: ''",
                "history_fill_data: true",
                "history_force_refresh: false",
                "report_dir: data/reports/live",
                f"qmt_install_dir: {install_dir.as_posix()}",
                "qmt_download_url: https://example.com/qmt.rar",
                f"qmt_userdata_dir: {userdata_dir.as_posix()}",
                f"qmt_bridge_python: {bridge_python.as_posix()}",
                f"qmt_bridge_script: {bridge_script.as_posix()}",
                "qmt_account_id: '39957041'",
                "qmt_trade_enabled: false",
                "default_strategy: etf_rotation",
                "symbols:",
                "  - 000001.SZ",
            ]
        ),
        encoding="utf-8",
    )
    return app_file


def test_bridge_client_parses_latest_prices(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = load_app_settings(build_settings(tmp_path))
    captured: dict[str, list[str]] = {}

    def fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        captured["cmd"] = cmd
        return SimpleNamespace(
            returncode=0,
            stdout='{"ok": true, "data": {"quotes": {"000001.SZ": {"last_price": 12.34}}}}',
            stderr="",
        )

    monkeypatch.setattr("subprocess.run", fake_run)

    client = QmtBridgeClient(settings)
    prices = client.get_latest_prices(["000001.SZ"])

    assert prices == {"000001.SZ": Decimal("12.34")}
    assert captured["cmd"][0].endswith("python.exe")
    assert "quote" in captured["cmd"]
    assert "--account-id" in captured["cmd"]


def test_bridge_client_parses_history_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = load_app_settings(build_settings(tmp_path))

    def fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        return SimpleNamespace(
            returncode=0,
            stdout='{"ok": true, "data": {"rows": [{"trading_date": "2026-03-20", "symbol": "000001.SZ", "open": 10.1, "high": 10.5, "low": 9.9, "close": 10.3, "volume": 123456, "amount": 987654.0}]}}',
            stderr="",
        )

    monkeypatch.setattr("subprocess.run", fake_run)

    client = QmtBridgeClient(settings)
    frame = client.get_history(["000001.SZ"], "1d", "20200101", "20260321", "front", True)

    assert len(frame) == 1
    assert frame.iloc[0]["symbol"] == "000001.SZ"
    assert frame.iloc[0]["close"] == 10.3
    assert str(frame.iloc[0]["trading_date"]) == "2026-03-20"


def test_bridge_client_raises_on_bridge_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = load_app_settings(build_settings(tmp_path))

    def fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        return SimpleNamespace(returncode=1, stdout="", stderr="boom")

    monkeypatch.setattr("subprocess.run", fake_run)

    client = QmtBridgeClient(settings)
    with pytest.raises(QmtUnavailableError, match="boom"):
        client.healthcheck()
