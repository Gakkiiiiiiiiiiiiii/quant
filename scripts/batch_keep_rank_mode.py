"""对比 keep_rank=80 的两种持仓保留策略：

方案A（优先级降低）：滑出 keep_rank 但仍在 ranked_cap 内的持仓保留，优先级降低
方案B（硬截断）：滑出 keep_rank 的持仓直接卖出，不保留

使用方式：
    python scripts/batch_keep_rank_mode.py --config configs/app_backtest_kr.yaml
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import pandas as pd

from quant_demo.core.config import load_app_settings, load_strategy_settings
from quant_demo.db.session import create_session_factory
from quant_demo.experiment.joinquant_microcap_engine import (
    JoinQuantMicrocapBacktestEngine,
    MicrocapStrategyConfig,
)
from quant_demo.experiment.evaluator import Evaluator


def main() -> None:
    parser = argparse.ArgumentParser(description="对比 keep_rank 两种保留策略")
    parser.add_argument("--config", default=str(ROOT / "configs" / "app_backtest_kr.yaml"))
    parser.add_argument("--strategy", default=str(ROOT / "configs" / "strategy" / "joinquant_microcap_alpha_calendar_crash.yaml"))
    parser.add_argument("--initial-cash", type=float, default=100000.0)
    args = parser.parse_args()

    backtest_config = load_app_settings(args.config)
    session_factory = create_session_factory(backtest_config.database_url)

    base_strategy = load_strategy_settings(args.strategy)

    print("[batch] 步骤 1: 加载数据（只需一次）", flush=True)
    engine = JoinQuantMicrocapBacktestEngine(session_factory, backtest_config, base_strategy)

    t0 = time.time()
    history = engine._load_history()
    print(f"[batch] 历史数据加载完成: {len(history)} 行, {time.time()-t0:.1f}s", flush=True)

    symbols = history["symbol"].dropna().astype(str).unique().tolist()
    instrument_frame = engine._load_instrument_frame(symbols)
    print(f"[batch] 证券元数据: {len(instrument_frame)} 条", flush=True)

    capital_frame = engine._load_capital_frame(symbols)
    print(f"[batch] 股本数据: {len(capital_frame)} 条", flush=True)

    print("[batch] 步骤 2: 构建回测截面（只需一次）", flush=True)
    t1 = time.time()
    prepared = engine._prepare_history(history, instrument_frame, capital_frame)
    overlay_history = engine._load_overlay_history()
    overlay_frame = engine._prepare_overlay_history(overlay_history)
    if not overlay_frame.empty:
        prepared = pd.concat([prepared, overlay_frame], ignore_index=True).sort_values(
            ["trading_date", "symbol"]
        ).reset_index(drop=True)
    benchmark = engine._load_benchmark()
    print(f"[batch] 截面构建完成: {len(prepared)} 行, {time.time()-t1:.1f}s", flush=True)

    base_cfg = MicrocapStrategyConfig.from_strategy_settings(base_strategy)
    cfg_kr80 = replace(base_cfg, keep_rank=80)
    evaluator = Evaluator()

    # 导入需要 monkey-patch 的函数
    import quant_demo.experiment.joinquant_microcap_engine as engine_mod
    orig_select = engine_mod._select_from_ranked_symbols

    def run_backtest(label: str) -> dict:
        engine.cfg = cfg_kr80
        t_start = time.time()
        summary = engine._simulate(prepared, benchmark, args.initial_cash, instrument_frame)
        t_elapsed = time.time() - t_start

        equity_curve = summary["equity_curve"].copy()
        metrics = evaluator.evaluate(equity_curve)

        daily_returns = equity_curve["equity"].pct_change().dropna()
        sharpe = 0.0
        if len(daily_returns) > 1 and daily_returns.std() > 0:
            sharpe = float((daily_returns.mean() / daily_returns.std()) * (252 ** 0.5))

        positions_df = summary.get("positions", pd.DataFrame())
        avg_holdings = 0.0
        if not positions_df.empty:
            avg_holdings = float(positions_df.groupby("trading_date")["symbol"].nunique().mean())

        total_days = len(equity_curve)

        # 计算每日换手率（卖出金额 / 总资产）
        trades_df = summary.get("trades", pd.DataFrame())
        sell_turnover = 0.0
        total_turnover = metrics.turnover
        if not trades_df.empty:
            sell_trades = trades_df[trades_df["side"].str.upper() == "SELL"]
            sell_turnover = float(sell_trades["amount"].sum()) if "amount" in sell_trades.columns else 0.0

        # 持仓天数统计
        hold_days_map = {}
        if not trades_df.empty and all(c in trades_df.columns for c in ["trading_date", "symbol", "side", "shares"]):
            buy_trades = trades_df[trades_df["side"].str.upper() == "BUY"]
            sell_trades_df = trades_df[trades_df["side"].str.upper() == "SELL"]
            buy_dates = {}
            for _, row in buy_trades.iterrows():
                symbol = str(row["symbol"])
                date = pd.Timestamp(row["trading_date"])
                buy_dates.setdefault(symbol, []).append(date)
            for _, row in sell_trades_df.iterrows():
                symbol = str(row["symbol"])
                date = pd.Timestamp(row["trading_date"])
                if symbol in buy_dates and buy_dates[symbol]:
                    buy_date = buy_dates[symbol].pop(0)
                    hold_days = (date - buy_date).days
                    hold_days_map.setdefault(symbol, []).append(hold_days)
            for symbol, dates in buy_dates.items():
                for buy_date in dates:
                    hold_days_map.setdefault(symbol, []).append(total_days)

        avg_hold_days = 0.0
        all_hold_days = [d for days in hold_days_map.values() for d in days]
        if all_hold_days:
            avg_hold_days = sum(all_hold_days) / len(all_hold_days)

        result = {
            "label": label,
            "keep_rank": 80,
            "total_return": round(metrics.total_return, 6),
            "annualized_return": round(metrics.annualized_return, 6),
            "max_drawdown": round(metrics.max_drawdown, 6),
            "sharpe": round(sharpe, 4),
            "turnover": round(metrics.turnover, 2),
            "sell_turnover": round(sell_turnover, 2),
            "avg_holdings": round(avg_holdings, 1),
            "avg_hold_days": round(avg_hold_days, 1),
            "total_days": total_days,
            "elapsed_seconds": round(t_elapsed, 1),
        }

        print(
            f"  [{label}] 总收益率: {metrics.total_return:.2%}  "
            f"年化: {metrics.annualized_return:.2%}  "
            f"最大回撤: {metrics.max_drawdown:.2%}  "
            f"Sharpe: {sharpe:.4f}  "
            f"换手: {metrics.turnover:,.0f}  "
            f"日均持仓: {avg_holdings:.1f}  "
            f"均持天数: {avg_hold_days:.1f}  "
            f"耗时: {t_elapsed:.1f}s",
            flush=True,
        )
        return result

    results = []

    # === 方案A：当前实现（优先级降低，ranked_cap内保留） ===
    print("\n[batch] ===== 方案A：优先级降低（滑出keep_rank但仍保留在ranked内） =====", flush=True)
    # 当前代码已经是方案A，直接运行
    results.append(run_backtest("A_优先级降低"))

    # === 方案B：硬截断（滑出keep_rank就卖出） ===
    print("\n[batch] ===== 方案B：硬截断（滑出keep_rank就卖出） =====", flush=True)

    def _select_hard_cutoff(ranked: list[str], holdings: list[str], target_count: int, cfg: MicrocapStrategyConfig) -> list[str]:
        """硬截断版：只在 keep_rank 范围内的持仓股才保留，超出即卖出"""
        if target_count <= 0 or not ranked:
            return []
        keep_rank = min(len(ranked), engine_mod._scaled_rank_limit(cfg.keep_rank, target_count, cfg.target_hold_num))
        buy_rank = min(len(ranked), engine_mod._scaled_rank_limit(cfg.buy_rank, target_count, cfg.target_hold_num))
        keep_rank_set = set(ranked[:keep_rank])
        target: list[str] = []
        # 只保留在 keep_rank 范围内的持仓股
        for symbol in holdings:
            if symbol in keep_rank_set and symbol not in target:
                target.append(symbol)
            if len(target) >= target_count:
                return target[:target_count]
        # buy_rank 以内的新候选
        for symbol in ranked[:buy_rank]:
            if symbol not in target:
                target.append(symbol)
            if len(target) >= target_count:
                return target[:target_count]
        # 其余补满
        for symbol in ranked:
            if symbol not in target:
                target.append(symbol)
            if len(target) >= target_count:
                break
        return target[:target_count]

    # Monkey-patch
    engine_mod._select_from_ranked_symbols = _select_hard_cutoff
    results.append(run_backtest("B_硬截断"))

    # 恢复
    engine_mod._select_from_ranked_symbols = orig_select

    # 输出汇总
    print("\n\n" + "=" * 110)
    print("keep_rank=80 两种保留策略对比")
    print("=" * 110)
    header = f"{'方案':>20} | {'总收益率':>12} | {'年化收益':>12} | {'最大回撤':>12} | {'Sharpe':>8} | {'换手总额':>14} | {'卖出额':>14} | {'日均持仓':>8} | {'均持天数':>8}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['label']:>20} | "
            f"{r['total_return']:>11.2%} | "
            f"{r['annualized_return']:>11.2%} | "
            f"{r['max_drawdown']:>11.2%} | "
            f"{r['sharpe']:>8.4f} | "
            f"{r['turnover']:>14,.0f} | "
            f"{r['sell_turnover']:>14,.0f} | "
            f"{r['avg_holdings']:>8.1f} | "
            f"{r['avg_hold_days']:>8.1f}"
        )

    output_path = ROOT / "data" / "reports" / "backtest_kr" / "keep_rank_mode_comparison.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存至: {output_path}")


if __name__ == "__main__":
    main()