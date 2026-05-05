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
    qmt_client_name: str = ""
    qmt_install_dir: str
    qmt_download_url: str
    qmt_userdata_dir: str = "runtime/qmt_client/installed/userdata_mini"
    qmt_bridge_python: str = ".venv-qmt36/Scripts/python.exe"
    qmt_bridge_script: str = "scripts/qmt_bridge.py"
    qmt_account_id: str = ""
    qmt_trade_enabled: bool = False
    qmt_protected_sell_symbols: list[str] = Field(default_factory=list)
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


def _merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


def _local_overlay_path(path: str | Path) -> Path:
    file_path = Path(path)
    return file_path.with_name(f"{file_path.stem}.local{file_path.suffix}")


def _load_yaml_with_local_overlay(path: str | Path) -> dict[str, Any]:
    data = _load_yaml(path)
    overlay_path = _local_overlay_path(path)
    if overlay_path.exists():
        data = _merge_dicts(data, _load_yaml(overlay_path))
    return data


def load_app_settings(path: str | Path) -> AppSettings:
    return AppSettings.model_validate(_load_yaml_with_local_overlay(path))


def load_strategy_settings(path: str | Path) -> StrategySettings:
    data = _load_yaml(path)
    extra_keys = set(data) - {"name", "implementation", "rebalance_frequency", "lookback_days", "top_n", "lot_size"}
    if extra_keys:
        data["extra"] = {key: data[key] for key in extra_keys}
    return StrategySettings.model_validate(data)


def resolve_strategy_config_path(
    strategy: str | Path | None,
    strategy_dir: str | Path,
    default_implementation: str = "",
) -> Path:
    """解析策略配置路径。

    规则：
    1. 显式传入存在的文件路径时，直接使用。
    2. 显式传入实现键时，在策略目录中查找 implementation 匹配的 yaml。
    3. 未显式传入时，使用应用配置中的 default_strategy 作为实现键解析。
    """

    directory = Path(strategy_dir)
    if not directory.exists():
        raise ConfigError(f"策略目录不存在: {directory}")

    if isinstance(strategy, Path):
        strategy_text = str(strategy)
    else:
        strategy_text = str(strategy or "").strip()

    if strategy_text:
        candidate = Path(strategy_text)
        if not candidate.is_absolute():
            candidate = (Path.cwd() / candidate).resolve()
        if candidate.exists():
            return candidate
        target_implementation = strategy_text
    else:
        target_implementation = str(default_implementation or "").strip()

    if not target_implementation:
        raise ConfigError("未提供策略配置路径，且应用配置中 default_strategy 为空")

    for file_path in sorted(directory.glob("*.yaml")):
        data = _load_yaml(file_path)
        if str(data.get("implementation") or "").strip() == target_implementation:
            return file_path.resolve()

    raise ConfigError(f"未找到 implementation={target_implementation} 对应的策略配置文件，目录: {directory}")
