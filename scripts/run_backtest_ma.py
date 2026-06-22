"""均线粘合放倍量突破策略回测脚本。

使用方式：
    python scripts/run_backtest_ma.py
"""
from __future__ import annotations

import sys
import time
from decimal import Decimal
from pathlib import Path

from _backtest_summary import build_summary_payload, write_summary_files

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from quant_demo.core.config import load_app_settings, load_strategy_settings
from quant_demo.db.session import create_session_factory
from quant_demo.experiment.manager import ExperimentManager
from quant_demo.experiment.evaluator import Evaluator


def main() -> None:
    config_path = ROOT / "configs" / "app_backtest_ma.yaml"
    strategy_path = ROOT / "configs" / "strategy" / "ma_convergence_breakout.yaml"

    print("[ma_backtest] 加载配置...", flush=True)
    app_settings = load_app_settings(str(config_path))
    strategy_settings = load_strategy_settings(str(strategy_path))
    session_factory = create_session_factory(app_settings.database_url)

    print(f"[ma_backtest] 策略: {strategy_settings.implementation}", flush=True)
    print(f"[ma_backtest] 参数: convergence_limit={strategy_settings.extra.get('convergence_limit')}, "
          f"volume_multiple={strategy_settings.extra.get('volume_multiple')}, "
          f"max_hold_num={strategy_settings.extra.get('max_hold_num', 10)}, "
          f"hold_days={strategy_settings.extra.get('hold_days')}, "
          f"stop_loss={strategy_settings.extra.get('stop_loss')}", flush=True)

    manager = ExperimentManager(session_factory, app_settings, strategy_settings)

    t0 = time.time()
    result = manager.run(Decimal("100000"))
    elapsed = time.time() - t0

    evaluator = Evaluator()
    metrics = evaluator.evaluate(result.equity_curve)
    summary_payload = build_summary_payload(
        strategy_name=strategy_settings.implementation,
        config_path=str(config_path),
        strategy_path=str(strategy_path),
        report_path=str(result.report_path),
        total_return=metrics.total_return,
        annualized_return=metrics.annualized_return,
        max_drawdown=metrics.max_drawdown,
        turnover=metrics.turnover,
        equity_curve=result.equity_curve,
        elapsed_seconds=elapsed,
        extra={
            "convergence_limit": strategy_settings.extra.get("convergence_limit"),
            "volume_multiple": strategy_settings.extra.get("volume_multiple"),
            "max_hold_num": strategy_settings.extra.get("max_hold_num", 10),
            "hold_days": strategy_settings.extra.get("hold_days"),
            "stop_loss": strategy_settings.extra.get("stop_loss"),
        },
    )
    summary_files = write_summary_files(summary_payload)

    print("\n" + "=" * 60)
    print("均线粘合放倍量突破策略 回测结果")
    print("=" * 60)
    print(f"  总收益率:     {metrics.total_return:.2%}")
    print(f"  年化收益率:   {metrics.annualized_return:.2%}")
    print(f"  最大回撤:     {metrics.max_drawdown:.2%}")
    print(f"  换手总额:     {metrics.turnover:,.0f}")
    print(f"  回测耗时:     {elapsed:.1f}s")
    print(f"  报告路径:     {result.report_path}")
    print(f"  摘要 JSON:    {summary_files['summary_json_path']}")
    print(f"  摘要 Markdown:{summary_files['summary_markdown_path']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
