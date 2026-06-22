# CLAUDE.md

本仓库给 Claude Code 的约束只保留项目必需信息，避免长会话反复注入大段上下文。

## 首选工作方式

- 回测、参数扫描、批量对比先运行本地脚本，再读取摘要文件，不要反复读取完整日志。
- 优先读取 `data/reports/ai_summaries/latest_*.json` 或 `latest_*.md`。
- 只有摘要不足时，才继续读取 `daily_report.md`、详细 CSV、完整日志。
- 不要默认扫描 `data/`、`runtime/`、虚拟环境、QMT 安装目录。

## 回测命令

```bash
# 通用回测
.\.venv\Scripts\python.exe scripts\run_backtest.py --config configs\app.yaml --strategy configs\strategy\first_alpha_v1.yaml

# MA 策略回测
.\.venv\Scripts\python.exe scripts\run_backtest_ma.py

# keep_rank 参数对比
.\.venv\Scripts\python.exe scripts\batch_keep_rank_mode.py --config configs\app_backtest_kr.yaml

# KR 组合批量回测
.\.venv\Scripts\python.exe scripts\batch_backtest_kr.py --config configs\app_backtest_kr.yaml
```

## 模型读取顺序

1. `data/reports/ai_summaries/latest_*.json`
2. `data/reports/ai_summaries/latest_*.md`
3. 批量回测输出的比较文件，例如 `data/reports/backtest_kr/*.json`
4. 仅在必要时读取 `data/reports/*.md`、`*.csv`、完整日志

## 关键路径

- `configs/app*.yaml`: 环境配置
- `configs/strategy/*.yaml`: 策略参数
- `scripts/`: 回测、批处理、执行入口
- `src/quant_demo/experiment/manager.py`: 回测入口分发
- `src/quant_demo/experiment/joinquant_microcap_engine.py`: 微盘主路径
- `src/quant_demo/strategy/implementations/`: 原生策略实现

## 架构只记两件事

- 微盘主策略走 `JoinQuantMicrocapBacktestEngine` / `QmtMicrocapTradingEngine`
- 原生策略走 `ExperimentManager._run_native()`

## 项目约定

- 代码注释默认使用中文。
- 密钥和本地连接配置放 `configs/*.local.yaml`。
- 做参数扫描时，优先分析汇总 JSON/CSV，不要把整段回测日志贴进对话。
