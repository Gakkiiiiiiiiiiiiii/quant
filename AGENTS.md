# AGENTS.md

本文件只保留仓库级约束，避免代理在长会话里反复携带大段流程说明。

## 默认原则

- 先运行脚本，再读摘要，不要默认读取完整日志。
- 优先看 `data/reports/ai_summaries/latest_*.json` 或 `latest_*.md`。
- 做批量回测时，优先分析 `data/reports/backtest_kr/*.json` 这类汇总结果。
- `data/`、`runtime/`、虚拟环境、QMT 安装目录都视为大目录，只有任务明确要求时才进入。

## 常用入口

- `scripts/run_backtest.py`: 通用回测
- `scripts/run_backtest_ma.py`: MA 策略回测
- `scripts/batch_backtest_kr.py`: KR 批量参数比较
- `scripts/batch_keep_rank_mode.py`: keep_rank 模式比较

## 关键代码位置

- `src/quant_demo/experiment/manager.py`: 回测分发入口
- `src/quant_demo/experiment/joinquant_microcap_engine.py`: 微盘回测主逻辑
- `src/quant_demo/strategy/implementations/`: 原生策略实现

## 输出约定

- 新增回测脚本时，默认同时写摘要 JSON/Markdown。
- 让模型读摘要文件，不要依赖终端长输出。
- 代码注释和说明保持中文。
