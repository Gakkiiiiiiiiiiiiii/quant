from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from quant_demo.core.enums import Environment
from quant_demo.core.exceptions import ConfigError


class RiskSettings(BaseModel):
    max_position_ratio: float = 0.35
    daily_loss_limit: float = -0.04
    trading_start: str = "09:35"
    trading_end: str = "14:55"


class AppSettings(BaseModel):
    app_name: str
    environment: Environment
    database_url: str
    history_parquet: str
    history_source: str = "qmt"
    history_period: str = "1d"
    history_adjustment: str = "front"
    history_start: str = ""
    history_end: str = ""
    history_fill_data: bool = True
    history_force_refresh: bool = False
    history_batch_size: int = 200
    history_universe_sector: str = ""
    history_universe_limit: int = 0
    report_dir: str
    qmt_install_dir: str
    qmt_download_url: str
    qmt_userdata_dir: str = "runtime/qmt_client/installed/userdata_mini"
    qmt_bridge_python: str = ".venv-qmt36/Scripts/python.exe"
    qmt_bridge_script: str = "scripts/qmt_bridge.py"
    qmt_account_id: str = ""
    qmt_trade_enabled: bool = False
    default_strategy: str
    symbols: list[str] = Field(default_factory=list)
    backtest_engine: str = "native"
    qlib_source_dir: str = "runtime/qlib_source"
    qlib_provider_dir: str = "runtime/qlib_data/cn_data"
    qlib_dataset_dir: str = "runtime/qlib_data/source"
    qlib_region: str = "cn"
    qlib_benchmark: str = "SH000300"
    qlib_benchmark_symbol: str = "000300.SH"
    qlib_n_drop: int = 1
    qlib_force_rebuild: bool = False
    risk: RiskSettings = Field(default_factory=RiskSettings)


class StrategySettings(BaseModel):
    name: str
    implementation: str
    rebalance_frequency: str = "weekly"
    lookback_days: int = 20
    top_n: int = 2
    lot_size: int = 100
    extra: dict[str, Any] = Field(default_factory=dict)


def _load_yaml(path: str | Path) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        raise ConfigError(f"配置文件不存在: {file_path}")
    with file_path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def load_app_settings(path: str | Path) -> AppSettings:
    return AppSettings.model_validate(_load_yaml(path))


def load_strategy_settings(path: str | Path) -> StrategySettings:
    data = _load_yaml(path)
    extra_keys = set(data) - {"name", "implementation", "rebalance_frequency", "lookback_days", "top_n", "lot_size"}
    if extra_keys:
        data["extra"] = {key: data[key] for key in extra_keys}
    return StrategySettings.model_validate(data)
