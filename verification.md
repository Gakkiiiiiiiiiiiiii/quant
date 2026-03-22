# 验证报告

日期：2026-03-21  
执行者：Codex

## 已验证内容

- 历史数据加载：可自动生成示例 ETF 数据并写入 `data/parquet/history.parquet`。
- 回测主链路：策略 -> 调仓 -> 风控 -> OMS -> 成交 -> 快照 -> 审计完整跑通。
- 仿真链路：使用模拟成交客户端在 `paper` 环境跑通。
- 对账链路：订单数、成交数、资产快照数一致，无异常告警。
- 策略晋升：可写入 `strategy_versions` 与 `promotion_requests`。
- 实盘接入保护：在 `xttrader` 缺失时明确报错并停止。

## 关键结果

- 回测结果：`total_return = -0.006721788999999978`，`annualized_return = -0.014063582883742765`，`max_drawdown = -0.007708586096344838`，`turnover = 866797.24`。
- 仿真报告：已生成 `data/reports/paper/daily_report.md`。
- 对账结果：`order_count = 30`，`trade_count = 30`，`asset_snapshot_count = 120`，`issues = []`。

## 已知限制

- 当前环境未安装 QMT SDK，`live` 仅验证到“未安装时明确失败”的保护路径。
- `pytest` 在当前沙箱里会在 session 收尾清理目录时触发权限异常，因此正式结果以脚本验证与手工调用测试函数为准。

## 追加验证：QMT Python 桥接联调

- 新增 `.venv-qmt36` 作为 xtquant 兼容运行时，并通过 `scripts/qmt_bridge.py` 将 Python 3.6 SDK 桥接到主系统。
- `scripts/run_live.py --mode probe` 已通过，验证了健康检查、账户状态、资产查询、ETF 合约详情和实时行情读取。
- 主程序探测结果：`account_id = 39957041`，`connect_result = 0`，`subscribe_result = 0`，`cash = 21000000.0`，持仓/委托/成交当前为空。
- 实盘策略模式默认关闭：`scripts/run_live.py --mode strategy` 会因 `qmt_trade_enabled = false` 明确拦截自动委托。

## 当前边界

- QMT 行情与账户查询已经打通。
- 真实下单已具备桥接入口，但主执行引擎仍按“同步成交”建模，暂未切换到异步回报驱动，因此未执行真实委托。
- `pytest` 仍会在当前沙箱的临时目录清理阶段报权限异常，本次新增测试采用直接调用测试函数的方式完成等价验证。

## 追加验证：终端式 UI

- Streamlit 面板已重构为终端式驾驶舱布局，支持回测、仿真、实盘监控三种视图切换。
- 回测库验证通过：可以同时展示资产曲线、持仓、订单簿、成交流、风控拆解、审计流、策略版本与日终报告。
- 实盘监控视图在 `live` 模式下会自动通过 QMT 桥接读取只读账户与行情状态。

## 追加验证：PostgreSQL 与第一版策略

- 本地 Docker 已部署 PostgreSQL 16，容器名 `quant-postgres`，端口 `5432`，项目数据目录为 `runtime/postgres_data`。
- 三套环境已切换为 PostgreSQL：`quant_backtest`、`quant_paper`、`quant_live`。
- PostgreSQL 驱动 `psycopg[binary]` 已安装到项目虚拟环境。
- 回测脚本已在 PostgreSQL 上成功执行两次，确认重复回测不会再触发唯一键冲突。
- 新策略 `first_alpha_v1` 已接入注册中心、脚本默认参数和 UI 模板列表。
- `run_live.py --mode probe` 在 PostgreSQL 配置下继续通过，说明数据库切换未影响 QMT 联调。

## 追加验证：QMT 历史K线与前复权回测

- 回测与仿真环境已切换为 `history_source = qmt`，通过 `scripts/qmt_bridge.py history` + `xtdata.get_market_data_ex()` 从 QMT 拉取历史日线。
- 当前默认复权配置为前复权：`history_adjustment = front`，默认周期为 `1d`，默认起始日期为 `20200101`。
- `scripts/load_history.py --config configs/app.yaml` 已通过，成功写入 `data/parquet/history.parquet`，共 `6016` 条记录，4 个标的各 `1504` 根日线。
- 历史缓存元数据文件 `data/parquet/history.parquet.meta.json` 已生成，明确记录了 `source=qmt`、`period=1d`、`adjustment=front`、股票列表与区间。
- 真实对比验证：对 `000001.SZ` 读取 `none/front` 两种口径时，`2024-01-02` 收盘价分别为 `9.21` 和 `7.647`，最新日期价格同为 `10.77`，说明前复权口径已实际生效。
- `scripts/run_backtest.py` 与 `scripts/run_paper.py` 已在 QMT 前复权历史数据上重新跑通。

## 补充说明

- `pytest` 仍会在当前环境的临时目录清理阶段触发权限异常，因此本次新增桥接单测继续以直接调用测试函数方式完成等价验证。

## 2026-03-21 Qlib integration verification
- 2026-03-21 23:52:43 scripts\load_history.py --config .tmp\qlib_smoke.yaml completed and wrote data/parquet/qlib_smoke_history.parquet with 11680 rows from QMT front-adjusted daily bars.
- 2026-03-21 23:52:43 scripts\run_backtest.py --config .tmp\qlib_smoke.yaml --strategy configs\strategy\first_alpha_v1.yaml completed through the Qlib engine path.
- Result metrics: total_return=-0.2026905618, annualized_return=-0.1892999106, max_drawdown=-0.4434639201, turnover=105.3371802943.
- PostgreSQL verification: asset_snapshots=272, position_snapshots=783, audit_logs=1, latest account_id=qlib-backtest, latest total_asset=79730.943819.



## 2026-03-22 Qlib UI and history maintenance
- `scripts/manage_history.py --config .tmp\qlib_smoke.yaml --mode status` passed and returned history file, Qlib provider, dataset, row count, symbol count, and latest trading date status.
- `scripts/manage_history.py --config .tmp\qlib_smoke.yaml --mode incremental` passed and performed an incremental refresh on the front-adjusted cache, fetching 1 batch and 40 rows; metadata now records `cache_mode=incremental` and `incremental_start=20260320`.
- `scripts/manage_history.py --config .tmp\qlib_smoke.yaml --mode cleanup-qlib` passed and removed `runtime/qlib_data/smoke_cn_data` and `runtime/qlib_data/smoke_source`.
- The first rebuild attempt exposed two regressions in `qlib_engine.py`: missing `hashlib/json` imports and stale `provider_meta.json` reuse when `qlib_force_rebuild=true`; both were fixed.
- After the fix, `scripts/run_backtest.py --config .tmp\qlib_smoke.yaml --strategy configs\strategy\first_alpha_v1.yaml` passed again, confirming provider rebuild after cache cleanup.
- With `.tmp\qlib_smoke_cache.yaml` (`qlib_force_rebuild=false`), repeated backtests passed and the modification times of `runtime/qlib_data/smoke_cn_data` and `runtime/qlib_data/smoke_source/provider_meta.json` stayed unchanged, confirming provider reuse.
- `scripts/manage_history.py --config .tmp\qlib_smoke.yaml --mode cleanup-history` passed and removed the history parquet plus metadata.
- `scripts/load_history.py --config .tmp\qlib_smoke.yaml --mode full` passed and rebuilt the front-adjusted QMT history cache with `11680` rows across `40` symbols.
- `python -m streamlit run scripts/run_dashboard.py --server.headless true --server.port 8517` started successfully, confirming the new Qlib full-market task panel initializes cleanly.


## 2026-03-22 user pattern strategy backtests
- Source strategy module: `D:/project/quant/strategy/strategy.py`; the file defines three standalone pattern strategies `B1`, `B2`, and `B3`.
- Backtest capital was set to `500000`, requested range `2023-01-01` to `2026-03-20`, benchmark `SH000300`.
- QMT full-market front-adjusted daily history was refreshed through `scripts/manage_history.py --config .tmp/user_pattern_app.yaml --mode full`, producing `4267224` rows across `5499` symbols.
- QMT returned data through `2026-03-19`; the actual backtest report range ended on `2026-03-18` because the current Qlib backtest loop requires the penultimate signal date to avoid a calendar boundary error.
- Final metrics:
  - `B1`: total_return `0.0497397972`, annualized_return `0.0159093105`, max_drawdown `-0.3733854126`, ending_equity `524869.90`.
  - `B2`: total_return `0.3994695808`, annualized_return `0.1154796917`, max_drawdown `-0.3339246013`, ending_equity `699734.79`.
  - `B3`: total_return `-0.2521030253`, annualized_return `-0.0901323393`, max_drawdown `-0.3138725684`, ending_equity `373948.49`.
- Reports were written to `data/reports/user_pattern_backtests/` including per-strategy report CSV, risk CSV, and summary files.

## 2026-03-22 UI pattern research update
- Generated user pattern equity comparison artifacts: data/reports/user_pattern_backtests/equity_comparison.csv, .png, and .html.
- Reworked the Streamlit layout into tabs for overview, Qlib full-market tasks, user pattern research, and logs/audit.
- Verified headless startup of scripts/run_dashboard.py on port 8518 after the layout update.

## 2026-03-22 QMT timezone fix and trade ledger refresh
- Confirmed the root cause of weekend trades was timezone handling in `scripts/qmt_bridge.py`: QMT `1d` bar timestamps are midnight in Asia/Shanghai, but the bridge previously parsed them as naive UTC and shifted them one day earlier.
- Direct QMT verification for `600546.SH` front-adjusted daily bars showed `2026-03-18=11.70`, `2026-03-19=11.87`, `2026-03-20=11.77`, matching the broker client after converting timestamps to `Asia/Shanghai`.
- `scripts/load_history.py --config .tmp/app_sqlite_backtest.yaml --mode full` passed and rebuilt `data/parquet/history.parquet` with `8083531` rows, `5499` symbols, and latest trading date `2026-03-20`.
- Post-fix validation confirmed `history.parquet` no longer contains `2026-03-08` or `2026-03-15`, and `600546.SH` no longer has any Sunday rows.
- `scripts/run_user_pattern_backtests.py --config .tmp/app_sqlite_backtest.yaml --strategy-file strategy/strategy.py --provider-strategy configs/strategy/first_alpha_v1.yaml --start 2023-01-01 --end 2026-03-20 --account 500000 --modes B1 B2 B3` completed and regenerated reports under `data/reports/user_pattern_backtests/`.
- New `*_daily_actions.csv` files now use one-row-per-trade-cycle ledger columns: `日期 / 策略 / 操作 / 标的 / 股票代码 / 标的名称 / BUY金额 / SELL日期 / 盈亏金额 / 收益率`.
- Verification confirmed `b1_report.csv`, `b1_daily_decisions.csv`, `b1_daily_actions.csv`, `b2_daily_actions.csv`, and `b3_daily_actions.csv` no longer contain `2026-03-08` or `2026-03-15`.

## 2026-03-22 Codex

- 修正 `B1` 执行语义：将 `2026-01-08` 启动日与后续 `1-3` 日缩量回踩入场拆分处理，`b1` 仅在回踩入场日生效，止损锚定启动 K 线低点。
- 修正用户策略回测成交语义：买入按 `T-1` 信号对应的 `T` 日开盘价执行，卖出按 `T` 日信号对应的 `T` 日收盘价执行，双边加入 `0.5%` 滑点。
- 使用 `configs/app.yaml` 重新回测 `B1`，区间 `2023-01-01` 至 `2026-03-20`，初始资金 `500000`。
- 验证 `data/reports/user_pattern_backtests/b1_daily_actions.csv` 中不存在 `2026-01-09` 买入 `301218.SZ` 的记录。
- 最新 `B1` 回测结果：期末权益 `179748.80`，总收益 `-64.05%`，年化 `-28.27%`，最大回撤 `-65.55%`。

## 2026-03-22 Codex B1 reimplementation

- 依据 `strategy/b1.pdf` 与 `strategy/B1_B2_B3_交易策略文档.docx` 重写 `B1` 信号：支持“启动日右侧试错”和“1-3 日缩量回踩再上”两类入场，并将启动 K 线低点持续作为回踩止损锚点。
- 新增 `quality_score / priority_score` 评分体系，并在用户回测脚本中加入 `评分换仓`，仅当 `T-1` 新候选显著高于持仓评分时才在 `T` 日换仓。
- 新增 `最小新开仓比例` 过滤，默认 `6%`，避免总资金 `50w` 时出现单笔约 `1w` 的过小新开仓。
- 回测执行层新增 `ST` 过滤、`上市天数 >= 60` 和 `20 日成交额均值 >= 3000 万` 过滤。
- 交易台账 `data/reports/user_pattern_backtests/b1_daily_actions.csv` 已新增 `买入价格 / 卖出价格 / 买入评分 / 卖出评分 / BUY股数 / 卖出原因` 列。
- `summary.json` / `summary.md` 的权益计算已修正为不再重复扣减手续费，现与 `b1_report.csv` 的 `account` 口径一致。
- 执行 `python -m pytest tests/test_b1_strategy_logic.py -q` 通过，共 3 个测试，覆盖 `B1` 启动/回踩止损锚点、评分换仓和最小新开仓过滤。
- 执行 `scripts/run_user_pattern_backtests.py --config .tmp/app_sqlite_backtest.yaml --strategy-file strategy/strategy.py --provider-strategy configs/strategy/first_alpha_v1.yaml --start 2022-06-01 --end 2026-03-20 --account 500000 --modes B1` 通过。
- 最新 `B1` 回测结果：期末权益 `14932.11`，总收益 `-97.01%`，年化 `-61.70%`，最大回撤 `-97.01%`，换手 `460.56`。

## 2026-03-22 Codex B1 no-lookahead execution

- 回测执行语义已改为 `T-1` 出信号、`T` 日开盘成交；买入与卖出都不再读取 `T` 日收盘信号做决策。
- `scripts/run_user_pattern_backtests.py` 新增 `_decide_exit_from_signal` 与 `_resolve_trade_price`，卖出决策只依赖 `previous_signal_df`，成交价默认取执行日 `open`。
- CLI 默认执行价已从 `close` 改为 `open`；`strategy/strategy.py` 的 `run_pattern_backtest()` 默认 `deal_price` 也改为 `open`。
- `python -m pytest tests/test_b1_strategy_logic.py -q` 通过，共 5 个测试，其中新增 2 个测试覆盖 `T-1` 卖出判定和 `T` 日开盘成交价。
- 执行 `scripts/run_user_pattern_backtests.py --config .tmp/app_sqlite_backtest.yaml --strategy-file strategy/strategy.py --provider-strategy configs/strategy/first_alpha_v1.yaml --start 2022-06-01 --end 2026-03-20 --account 500000 --modes B1` 通过。
- 最新 `B1` 回测结果：期末权益 `18486.07`，总收益 `-96.30%`，年化 `-59.40%`，最大回撤 `-96.30%`，换手 `457.98`。

## 2026-03-22 Codex B1 formula version

- 按用户提供的通达信公式重写 `B1`：`结构条件 + 启动基础 + 回调到位 + 极致缩量 + K线约束`，不再使用此前的“启动/回踩二段式”B1 判定。
- 保留 `T-1` 信号、`T` 日开盘成交语义。
- `python -m pytest tests/test_b1_strategy_logic.py -q` 通过，共 5 个测试。
- 执行 `scripts/run_user_pattern_backtests.py --config .tmp/app_sqlite_backtest.yaml --strategy-file strategy/strategy.py --provider-strategy configs/strategy/first_alpha_v1.yaml --start 2025-12-20 --end 2026-03-20 --account 500000 --modes B1 --output-dir data/reports/user_pattern_backtests_recent3m` 通过。
- 最近 3 个月回测结果：无交易记录，`signal_count=0`，期末权益 `500000.00`，总收益 `0.00%`。

## 2026-03-22 Codex B1 114 trading-day warmup

- 修正 `scripts/run_user_pattern_backtests.py` 的行情加载窗口：正式回测开始日前，额外补载 `114` 个历史交易日，用于 `LT=(MA(C,14)+MA(C,28)+MA(C,57)+MA(C,114))/4` 的预热计算。
- 正式交易循环仍只统计用户指定区间，但首个正式交易日允许读取预热期最后一个交易日的 `T-1` 信号。
- 新增 `_shift_start_by_trading_days()` 测试，`python -m pytest tests/test_b1_strategy_logic.py -q` 通过，共 `6` 个测试。
- 执行 `scripts/run_user_pattern_backtests.py --config .tmp/app_sqlite_backtest.yaml --strategy-file strategy/strategy.py --provider-strategy configs/strategy/first_alpha_v1.yaml --start 2025-09-20 --end 2026-03-20 --account 500000 --modes B1 --output-dir data/reports/user_pattern_backtests_recent6m` 通过。
- 修正后最近半年回测结果：期末权益 `185576.13`，总收益 `-62.88%`，年化 `-88.39%`，最大回撤 `-63.78%`，换手 `138.57`。
- 最近半年交易台账 `data/reports/user_pattern_backtests_recent6m/b1_daily_actions.csv` 共 `751` 条记录，首笔正式区间交易发生在 `2025-10-15`，未将预热期交易混入统计结果。

## 2026-03-22 Codex B1 md final version

- 依据 `strategy/b1.md` 重写 `B1`：买点改为 `B1核心 AND 非禁入`，其中 `禁入 = 最近30日出现过出货信号 AND 尚未修复`。
- 新增 `B1` 五类顶部出货形态：`b1_exit_1 ~ b1_exit_5`，统一汇总为 `b1_exit_flag`；`B1` 持仓只按该信号离场，不再使用 `stop_loss / time_stop / score_swap`。
- `b1.md` 中 `重新修复 := C>HHV(H,30)*1.03 OR COUNT(C>ST,10)>=8` 的第一项若按当日 `HHV(H,30)` 实现将恒不成立，本次按文档语义实现为“突破前30日高点3%”，即前30日高点使用 `shift(1)`。
- `python -m pytest tests/test_b1_strategy_logic.py -q` 通过，共 `8` 个测试；新增覆盖 `30日禁入` 与 `B1仅按出货信号离场`。
- 执行 `scripts/run_user_pattern_backtests.py --config .tmp/app_sqlite_backtest.yaml --strategy-file strategy/strategy.py --provider-strategy configs/strategy/first_alpha_v1.yaml --start 2025-09-20 --end 2026-03-20 --account 500000 --modes B1 --output-dir data/reports/user_pattern_backtests_recent6m_b1md` 通过。
- 由于旧目录中的 `b1_daily_actions.csv` 被占用，本次最近半年结果写入 `data/reports/user_pattern_backtests_recent6m_b1md/`。
- 最新最近半年回测结果：期末权益 `387800.35`，总收益 `-22.44%`，年化 `-42.42%`，最大回撤 `-24.19%`，换手 `1.72`。
- 最近半年交易台账 `data/reports/user_pattern_backtests_recent6m_b1md/b1_daily_actions.csv` 共 `8` 条记录，其中唯一已完成卖出为 `605333.SH`，卖出原因 `b1_distribution`。

## 2026-03-22 Codex B1 final version v2

- 依据 `strategy/B1_最终版策略说明_v2.md` 更新 `B1` 离场系统，在原有 `五类顶部出货形态` 基础上新增 `LT硬止损 / ST止损 / 平台止损 / 信号日低点止损 / 时间止损A / 时间止损B`。
- `B1` 回测执行层继续保持 `T-1` 出信号、`T` 日开盘成交`，新增防守止损同样按 `T-1` 信号确认、`T` 日开盘卖出。
- `python -m pytest tests/test_b1_strategy_logic.py -q` 通过，共 `10` 个测试；新增覆盖 `B1 v2 防守止损` 与 `B1 v2 时间止损`。
- 执行 `scripts/run_user_pattern_backtests.py --config .tmp/app_sqlite_backtest.yaml --strategy-file strategy/strategy.py --provider-strategy configs/strategy/first_alpha_v1.yaml --start 2025-09-20 --end 2026-03-20 --account 500000 --modes B1 --output-dir data/reports/user_pattern_backtests_recent6m_b1v2` 通过。
- 最新最近半年回测结果写入 `data/reports/user_pattern_backtests_recent6m_b1v2/`：期末权益 `293756.79`，总收益 `-41.25%`，年化 `-68.51%`，最大回撤 `-41.69%`，换手 `91.98`。
- 最近半年交易台账 `data/reports/user_pattern_backtests_recent6m_b1v2/b1_daily_actions.csv` 共 `529` 条记录；卖出原因分布为：`b1_st_stop=463`、`b1_lt_hard_stop=35`、`b1_signal_low_stop=16`、`b1_distribution=5`，其余 `10` 条为未平仓记录。

## 2026-03-22 Codex B1 v2 ST stop adjustment

- 根据用户反馈，删除 `B1 v2` 中不合理的 `COUNT(C<ST,2)>=2` 止损条件，因为 `B1` 买点本身允许出现在 `ST` 下方附近。
- `B1 ST止损` 现仅保留“放量走弱跌破 ST”条件：`C<ST AND V>MA(V,5)*1.2 AND C<REF(C,1)`。
- 新增测试覆盖“连续两天在 ST 下方但未放量走弱时，不应触发 `b1_st_stop`”，`python -m pytest tests/test_b1_strategy_logic.py -q` 通过，共 `11` 个测试。
- 执行 `scripts/run_user_pattern_backtests.py --config .tmp/app_sqlite_backtest.yaml --strategy-file strategy/strategy.py --provider-strategy configs/strategy/first_alpha_v1.yaml --start 2025-09-20 --end 2026-03-20 --account 500000 --modes B1 --output-dir data/reports/user_pattern_backtests_recent6m_b1v2_nost2day` 通过。
- 修正后最近半年回测结果：期末权益 `446308.02`，总收益 `-10.74%`，年化 `-21.87%`，最大回撤 `-19.41%`，换手 `26.24`。
- 修正后卖出原因分布显著变化：`b1_signal_low_stop=56`、`b1_lt_hard_stop=32`、`b1_st_stop=24`、`b1_platform_stop=10`、`b1_distribution=9`、`b1_time_stop_a=3`、`b1_time_stop_b=3`，未平仓记录 `10` 条。
