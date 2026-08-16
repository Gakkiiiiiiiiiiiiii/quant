"""v1 契约测试的共享装配。"""
from __future__ import annotations

from pathlib import Path

from quant_demo.api.v1_app import V1State, set_state
from quant_demo.application.backtest_service import BacktestService
from quant_demo.application.market_data_service import MarketDataService
from quant_demo.application.paper_service import PaperService
from quant_demo.db.session import create_session_factory
from quant_demo.marketdata.ingestion import generate_sample_history
from quant_demo.snapshot.store import LocalParquetSnapshotStore

SAMPLE_SYMBOLS = ["600519.SH", "000858.SZ", "300750.SZ", "000001.SZ"]


def build_state(tmp_path: Path, symbols: list[str] | None = None, days: int = 200) -> V1State:
    symbols = symbols or SAMPLE_SYMBOLS
    history_path = tmp_path / "history.parquet"
    generate_sample_history(symbols, history_path, days=days)
    database_url = f"sqlite:///{(tmp_path / 'v1.db').as_posix()}"
    session_factory = create_session_factory(database_url)
    snapshot_store = LocalParquetSnapshotStore(tmp_path / "market_snapshots")
    market = MarketDataService(history_path, session_factory, source="qmt", snapshot_store=snapshot_store)
    state = V1State(database_url=database_url, history_path=str(history_path), snapshot_dir=str(tmp_path / "market_snapshots"))
    state.market = market
    state.backtests = BacktestService(session_factory, market)
    state.paper = PaperService(session_factory, market)
    set_state(state)
    return state


def history_dates(state: V1State, symbols: list[str] | None = None) -> list[str]:
    """返回样例历史行情实际覆盖的交易日列表。"""
    data = state.market.bars_batch(symbols or SAMPLE_SYMBOLS, "1990-01-01", "2099-12-31")
    return data["dates"]


def rewrite_history(history_path: Path, close_multiplier: float = 1.2) -> None:
    """模拟“原始行情后来修正”（快照不可变性测试用）。"""
    import pandas as pd

    frame = pd.read_parquet(history_path)
    frame["close"] = frame["close"] * close_multiplier
    frame.to_parquet(history_path, index=False)


def fresh_market(tmp_path: Path, source: str = "qmt"):
    """重新构造 MarketDataService（新 frame cache，模拟读取“当前行情”）。"""
    database_url = f"sqlite:///{(tmp_path / 'v1.db').as_posix()}"
    session_factory = create_session_factory(database_url)
    return MarketDataService(
        tmp_path / "history.parquet",
        session_factory,
        source=source,
        snapshot_store=LocalParquetSnapshotStore(tmp_path / "market_snapshots"),
    )
