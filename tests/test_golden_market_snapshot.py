"""Golden Market Snapshot（详细修改方案 §3.6）。

固定小数据集：10 symbols × 60 trading days，覆盖
ST / suspension / limit up / limit down / corporate action / index membership change。

Golden fixture 永久保留于 tests/fixtures/golden_market/（首次运行生成并入库）。
"""
from __future__ import annotations

import math
import os
from pathlib import Path

import pandas as pd
import pytest
import yaml

from quant_demo.application.market_data_service import MarketDataService
from quant_demo.db.session import create_session_factory
from quant_demo.snapshot.store import LocalParquetSnapshotStore

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "golden_market"
GOLDEN_END = pd.Timestamp("2026-08-14")
GOLDEN_DAYS = 60

# §3.6 要求的 PIT 样例标的（含 4 个普通标的共 10 个）
GOLDEN_SYMBOLS = [
    "600000.SH", "600519.SH", "000001.SZ", "300750.SZ",
    "600901.SH",  # ST 样例
    "600902.SH",  # suspension 样例
    "600903.SH",  # limit up 样例
    "600904.SH",  # limit down 样例
    "600905.SH",  # corporate action（分红）样例
    "600906.SH",  # index membership change 样例
]
SAMPLE_SYMBOLS = {
    "st": "600901.SH",
    "suspension": "600902.SH",
    "limit_up": "600903.SH",
    "limit_down": "600904.SH",
    "corporate_action": "600905.SH",
    "index_membership_change": "600906.SH",
}


def generate_golden_frame() -> pd.DataFrame:
    """确定性行情：固定日期锚点 + 纯公式，无任何随机数。"""
    dates = pd.bdate_range(end=GOLDEN_END, periods=GOLDEN_DAYS)
    rows: list[dict] = []
    for offset, symbol in enumerate(GOLDEN_SYMBOLS):
        base = 1.0 + offset * 0.3
        for index, trading_day in enumerate(dates):
            drift = 0.0008 * (index + 1)
            wave = math.sin(index / (5 + offset)) * 0.02
            close_price = round(3.0 + base + drift + wave, 4)
            rows.append(
                {
                    "trading_date": trading_day.date(),
                    "symbol": symbol,
                    "open": round(close_price * 0.995, 4),
                    "high": round(close_price * 1.01, 4),
                    "low": round(close_price * 0.99, 4),
                    "close": close_price,
                    "volume": 1_000_000 + index * 500 + offset * 10_000,
                }
            )
    return pd.DataFrame(rows)


def ensure_fixture() -> Path:
    """首次运行生成 fixture；已存在时校验可复算性后保持不可变。"""
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    history = FIXTURE_DIR / "history.parquet"
    descriptor = FIXTURE_DIR / "golden_market.yaml"
    frame = generate_golden_frame()
    if not history.exists():
        frame.to_parquet(history, index=False)
    else:
        existing = pd.read_parquet(history)
        assert existing["close"].tolist() == frame["close"].tolist(), "golden fixture 必须可复算"
    if not descriptor.exists():
        descriptor.write_text(
            yaml.safe_dump(
                {
                    "dataset": "golden_market",
                    "contract_version": "market-data.v1",
                    "symbols": GOLDEN_SYMBOLS,
                    "days": GOLDEN_DAYS,
                    "end": GOLDEN_END.date().isoformat(),
                    "samples": SAMPLE_SYMBOLS,
                },
                allow_unicode=True,
                sort_keys=False,
            ),
            encoding="utf-8",
        )
    return history


def _golden_market(tmp_path: Path) -> MarketDataService:
    history = ensure_fixture()
    tmp_path.mkdir(parents=True, exist_ok=True)
    session_factory = create_session_factory(f"sqlite:///{(tmp_path / 'golden.db').as_posix()}")
    return MarketDataService(
        history,
        session_factory,
        source="golden",
        snapshot_store=LocalParquetSnapshotStore(tmp_path / "market_snapshots"),
    )


def _seed_pit_metadata(market: MarketDataService) -> None:
    """按 §3.6 注入 ST/停牌/涨跌停/分红/指数成分变更的 PIT 元数据。"""
    dates = pd.bdate_range(end=GOLDEN_END, periods=GOLDEN_DAYS)
    mid = dates[GOLDEN_DAYS // 2].date().isoformat()
    market.upsert_security_status(
        [
            {"trading_date": mid, "symbol": SAMPLE_SYMBOLS["st"], "is_st": True, "available_at": f"{mid}T08:00:00"},
            {"trading_date": mid, "symbol": SAMPLE_SYMBOLS["suspension"], "is_suspended": True, "available_at": f"{mid}T08:00:00"},
        ]
    )
    market.upsert_price_limits(
        [
            {"trading_date": mid, "symbol": SAMPLE_SYMBOLS["limit_up"], "limit_rate": 0.1,
             "upper_limit_price": 3.3, "lower_limit_price": 2.7, "rule_version": "limit-rule-v1"},
            {"trading_date": mid, "symbol": SAMPLE_SYMBOLS["limit_down"], "limit_rate": 0.1,
             "upper_limit_price": 4.4, "lower_limit_price": 3.6, "rule_version": "limit-rule-v1"},
        ]
    )
    market.upsert_corporate_actions(
        [
            {"symbol": SAMPLE_SYMBOLS["corporate_action"], "ex_date": mid, "action_type": "DIVIDEND",
             "cash_dividend": 0.5, "available_at": f"{mid}T08:00:00"},
        ]
    )
    market.upsert_index_constituents(
        [
            {"index_code": "000300.SH", "symbol": SAMPLE_SYMBOLS["index_membership_change"],
             "valid_from": dates[0].date().isoformat(), "valid_to": mid},
            {"index_code": "000905.SH", "symbol": SAMPLE_SYMBOLS["index_membership_change"],
             "valid_from": mid, "valid_to": None},
        ]
    )


def test_golden_fixture_shape_and_samples():
    ensure_fixture()
    descriptor = yaml.safe_load((FIXTURE_DIR / "golden_market.yaml").read_text(encoding="utf-8"))
    assert descriptor["symbols"] == GOLDEN_SYMBOLS
    assert descriptor["days"] == GOLDEN_DAYS
    for key, symbol in SAMPLE_SYMBOLS.items():
        assert descriptor["samples"][key] == symbol


def test_golden_snapshot_is_immutable_and_replayable(tmp_path):
    market = _golden_market(tmp_path)
    _seed_pit_metadata(market)
    start = (GOLDEN_END - pd.Timedelta(days=120)).date().isoformat()
    end = GOLDEN_END.date().isoformat()
    created = market.create_snapshot(GOLDEN_SYMBOLS, start, end)
    snapshot_id = created["snapshot_id"]
    assert snapshot_id.startswith("mds-")

    first = market.get_snapshot(snapshot_id)
    second = market.get_snapshot(snapshot_id)
    assert first["manifest_hash"] and first["manifest_hash"] == second["manifest_hash"]

    loaded = market.load_snapshot(snapshot_id)
    assert loaded["symbols"] == GOLDEN_SYMBOLS
    assert loaded["data_snapshot_id"] == snapshot_id
    assert len(loaded["dates"]) > 0

    manifest = market._snapshot_store.read_manifest(snapshot_id)  # noqa: SLF001
    assert manifest["row_count"] == len(GOLDEN_SYMBOLS) * len(loaded["dates"])
    assert manifest["symbols_hash"]

    # PIT 元数据可查（历史重放依赖）
    st_status = market.get_security_status(SAMPLE_SYMBOLS["st"], start, end)
    assert any(item.get("is_st") for item in st_status)
    actions = market.get_corporate_actions(SAMPLE_SYMBOLS["corporate_action"], start, end)
    assert actions and actions[0]["action_type"] == "DIVIDEND"
    # 指数成分变更：as_of 在变更前命中 000300，之后命中 000905
    golden_dates = pd.bdate_range(end=GOLDEN_END, periods=GOLDEN_DAYS)
    before_as_of = golden_dates[GOLDEN_DAYS // 4].date().isoformat()
    before_change = market.get_index_constituents("000300.SH", before_as_of)
    assert any(item["symbol"] == SAMPLE_SYMBOLS["index_membership_change"] for item in before_change)
    after_change = market.get_index_constituents("000905.SH", end)
    assert any(item["symbol"] == SAMPLE_SYMBOLS["index_membership_change"] for item in after_change)


def test_golden_snapshot_content_address_stable(tmp_path):
    """同一 golden 数据两次独立构建得到同一 snapshot_id（内容寻址）。"""
    if os.getenv("QUANT_GOLDEN_MODE", "small") == "small":
        pytest.skip("full golden 专用：双构建内容寻址比对在 nightly full 模式运行")
    market_a = _golden_market(tmp_path / "a")
    market_b = _golden_market(tmp_path / "b")
    start = (GOLDEN_END - pd.Timedelta(days=120)).date().isoformat()
    end = GOLDEN_END.date().isoformat()
    created_a = market_a.create_snapshot(GOLDEN_SYMBOLS, start, end)
    created_b = market_b.create_snapshot(GOLDEN_SYMBOLS, start, end)
    assert created_a["snapshot_id"] == created_b["snapshot_id"]
    assert created_a["data_version"] == created_b["data_version"]


def test_golden_mode_env_recognized():
    """P0 Q-05：QUANT_GOLDEN_MODE=small 可在 PR 跑，full 可在 nightly 跑。"""
    mode = os.getenv("QUANT_GOLDEN_MODE", "small")
    assert mode in {"small", "full"}
    # small 模式仅跑核心用例（上方 skip 逻辑）；fixture 本身两种模式均可加载。
    ensure_fixture()


def test_golden_pit_membership_uses_only_valid_members(tmp_path):
    """P0 Q-05：membership 只能使用当时有效成员，不得读取未来状态。"""
    market = _golden_market(tmp_path)
    _seed_pit_metadata(market)
    golden_dates = pd.bdate_range(end=GOLDEN_END, periods=GOLDEN_DAYS)
    symbol = SAMPLE_SYMBOLS["index_membership_change"]
    # valid_from 之前：两个指数均不得包含该标的。
    before_all = (golden_dates[0] - pd.Timedelta(days=1)).date().isoformat()
    assert not any(item["symbol"] == symbol for item in market.get_index_constituents("000300.SH", before_all))
    assert not any(item["symbol"] == symbol for item in market.get_index_constituents("000905.SH", before_all))
    # 变更日之后：只命中新指数，不得回读旧 membership。
    end = GOLDEN_END.date().isoformat()
    assert not any(item["symbol"] == symbol for item in market.get_index_constituents("000300.SH", end))
    assert any(item["symbol"] == symbol for item in market.get_index_constituents("000905.SH", end))


def test_golden_suspension_flagged_on_exact_day(tmp_path):
    """P0 Q-05：停牌状态精确到日（停牌日不可成交的前提）。"""
    market = _golden_market(tmp_path)
    _seed_pit_metadata(market)
    dates = pd.bdate_range(end=GOLDEN_END, periods=GOLDEN_DAYS)
    mid = dates[GOLDEN_DAYS // 2].date()
    symbol = SAMPLE_SYMBOLS["suspension"]
    statuses = market.get_security_status(symbol, (mid - pd.Timedelta(days=3)).isoformat(), (mid + pd.Timedelta(days=3)).isoformat())
    by_date = {item["trading_date"]: item for item in statuses}
    assert by_date[mid.isoformat()]["is_suspended"] is True
    prior = (mid - pd.Timedelta(days=1)).isoformat()
    if prior in by_date:
        assert not by_date[prior].get("is_suspended")


def test_golden_corporate_action_no_double_adjustment(tmp_path):
    """P0 Q-05：corporate action 重复注入不得产生双重调整记录。"""
    market = _golden_market(tmp_path)
    _seed_pit_metadata(market)
    _seed_pit_metadata(market)  # 重复注入（幂等 upsert）
    start = (GOLDEN_END - pd.Timedelta(days=120)).date().isoformat()
    end = GOLDEN_END.date().isoformat()
    actions = market.get_corporate_actions(SAMPLE_SYMBOLS["corporate_action"], start, end)
    assert len(actions) == 1, "重复 upsert 不得产生 double adjustment 记录"
    assert actions[0]["cash_dividend"] == 0.5


def test_golden_snapshot_small_mode_fast_path(tmp_path):
    """P0 Q-05：small 模式下核心快照路径仍可完整运行。"""
    market = _golden_market(tmp_path)
    _seed_pit_metadata(market)
    start = (GOLDEN_END - pd.Timedelta(days=60)).date().isoformat()
    end = GOLDEN_END.date().isoformat()
    created = market.create_snapshot(GOLDEN_SYMBOLS[:4], start, end)
    assert created["snapshot_id"].startswith("mds-")
    loaded = market.load_snapshot(created["snapshot_id"])
    assert loaded["symbols"] == GOLDEN_SYMBOLS[:4]
