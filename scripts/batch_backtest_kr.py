"""批量回测不同 keep_rank 值，复用数据加载，只跑一次 prepare。

使用方式：
    python scripts/batch_backtest_kr.py --config configs/app_backtest_kr.yaml
"""
from __future__ import annotations

import argparse
import copy
import json
import sys
import time
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

# Bootstrap path
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


KEEP_RANKS = [45, 50, 55, 60, 70, 80, 100]


def main() -> None:
    parser = argparse.ArgumentParser(description="批量回测 keep_rank 参数")
    parser.add_argument("--config", default=str(ROOT / "configs" / "app_backtest_kr.yaml"))
    parser.add_argument("--strategy", default=str(ROOT / "configs" / "strategy" / "joinquant_microcap_alpha_calendar_crash.yaml"))
    parser.add_argument("--initial-cash", type=float, default=100000.0)
    args = parser.parse_args()

    backtest_config = load_app_settings(args.config)
    # session_factory 用 backtest 配置的 database_url 字符串
    session_factory = create_session_factory(backtest_config.database_url)

    base_strategy = load_strategy_settings(args.strategy)

    # 一次性加载和 prepare，用 backtest 配置驱动数据加载
    print("[batch_kr] 步骤 1: 加载数据（只需一次）", flush=True)
    engine = JoinQuantMicrocapBacktestEngine(session_factory, backtest_config, base_strategy)

    t0 = time.time()
    history = engine._load_history()
    print(f"[batch_kr] 历史数据加载完成: {len(history)} 行, {time.time()-t0:.1f}s", flush=True)

    symbols = history["symbol"].dropna().astype(str).unique().tolist()
    instrument_frame = engine._load_instrument_frame(symbols)
    print(f"[batch_kr] 证券元数据: {len(instrument_frame)} 条", flush=True)

    capital_frame = engine._load_capital_frame(symbols)
    print(f"[batch_kr] 股本数据: {len(capital_frame)} 条", flush=True)

    print("[batch_kr] 步骤 2: 构建回测截面（只需一次）", flush=True)
    t1 = time.time()
    prepared = engine._prepare_history(history, instrument_frame, capital_frame)

    overlay_history = engine._load_overlay_history()
    overlay_frame = engine._prepare_overlay_history(overlay_history)
    if not overlay_frame.empty:
        prepared = pd.concat([prepared, overlay_frame], ignore_index=True).sort_values(
            ["trading_date", "symbol"]
        ).reset_index(drop=True)

    benchmark = engine._load_benchmark()
    print(f"[batch_kr] 截面构建完成: {len(prepared)} 行, {time.time()-t1:.1f}s", flush=True)

    base_cfg = MicrocapStrategyConfig.from_strategy_settings(base_strategy)
    evaluator = Evaluator()

    results = []

    for kr in KEEP_RANKS:
        print(f"\n[batch_kr] ===== keep_rank={kr} =====", flush=True)
        # 构造仅修改 keep_rank 的配置
        cfg = replace(base_cfg, keep_rank=kr)

        # 诊断：打印目标 cfg 的 keep_rank
        print(f"  cfg.keep_rank={cfg.keep_rank}, cfg.buy_rank={cfg.buy_rank}, cfg.target_hold_num={cfg.target_hold_num}", flush=True)

        # 深拷贝引擎，替换 cfg，复用 prepared 数据
        engine.cfg = cfg

        # 诊断：统计调仓日中 active_cfg.keep_rank 的分布
        from collections import Counter
        kr_counter = Counter()

        original_resolve = __import__("quant_demo.experiment.joinquant_microcap_engine", fromlist=["resolve_effective_microcap_config"]).resolve_effective_microcap_config
        _orig_resolve = original_resolve
        def patched_resolve(trade_date, config_arg, benchmark_close, **kwargs):
            result_cfg, result_meta = _orig_resolve(trade_date, config_arg, benchmark_close, **kwargs)
            kr_counter[result_cfg.keep_rank] += 1
            return result_cfg, result_meta

        import quant_demo.experiment.joinquant_microcap_engine as engine_mod
        engine_mod.resolve_effective_microcap_config = patched_resolve

        t_start = time.time()
        summary = engine._simulate(prepared, benchmark, args.initial_cash, instrument_frame)
        t_elapsed = time.time() - t_start

        equity_curve = summary["equity_curve"].copy()
        metrics = evaluator.evaluate(equity_curve)

        # 计算 Sharpe（无风险利率假设0）
        daily_returns = equity_curve["equity"].pct_change().dropna()
        sharpe = 0.0
        if len(daily_returns) > 1 and daily_returns.std() > 0:
            sharpe = float((daily_returns.mean() / daily_returns.std()) * (252 ** 0.5))

        # 计算日均持仓数
        target_col = "target_count" if "target_count" in summary.get("daily_targets", pd.DataFrame()).columns else None
        avg_holdings = 0.0
        positions_df = summary.get("positions", pd.DataFrame())
        if not positions_df.empty:
            avg_holdings = float(positions_df.groupby("trading_date")["symbol"].nunique().mean())

        # 计算换手率（日均换手）
        total_days = len(equity_curve)
        total_turnover = metrics.turnover
        daily_avg_turnover = total_turnover / max(1, total_days)

        result = {
            "keep_rank": kr,
            "total_return": round(metrics.total_return, 6),
            "annualized_return": round(metrics.annualized_return, 6),
            "max_drawdown": round(metrics.max_drawdown, 6),
            "sharpe": round(sharpe, 4),
            "turnover": round(metrics.turnover, 2),
            "avg_daily_turnover": round(daily_avg_turnover, 2),
            "avg_holdings": round(avg_holdings, 1),
            "total_days": total_days,
            "elapsed_seconds": round(t_elapsed, 1),
        }
        results.append(result)

        print(
            f"  总收益率: {metrics.total_return:.2%}  "
            f"年化: {metrics.annualized_return:.2%}  "
            f"最大回撤: {metrics.max_drawdown:.2%}  "
            f"Sharpe: {sharpe:.4f}  "
            f"换手: {metrics.turnover:,.0f}  "
            f"日均持仓: {avg_holdings:.1f}  "
            f"耗时: {t_elapsed:.1f}s",
            flush=True,
        )
        print(f"  active_cfg.keep_rank分布: {dict(kr_counter)}", flush=True)

        # 恢复原始函数
        engine_mod.resolve_effective_microcap_config = _orig_resolve

    # 输出汇总
    print("\n\n" + "=" * 100)
    print("keep_rank 回测对比汇总")
    print("=" * 100)
    header = f"{'keep_rank':>10} | {'总收益率':>12} | {'年化收益':>12} | {'最大回撤':>12} | {'Sharpe':>8} | {'换手总额':>14} | {'日均持仓':>8} | {'耗时':>8}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['keep_rank']:>10} | "
            f"{r['total_return']:>11.2%} | "
            f"{r['annualized_return']:>11.2%} | "
            f"{r['max_drawdown']:>11.2%} | "
            f"{r['sharpe']:>8.4f} | "
            f"{r['turnover']:>14,.0f} | "
            f"{r['avg_holdings']:>8.1f} | "
            f"{r['elapsed_seconds']:>7.1f}s"
        )

    # 保存 JSON
    output_path = ROOT / "data" / "reports" / "backtest_kr" / "kr_comparison.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存至: {output_path}")


if __name__ == "__main__":
    main()