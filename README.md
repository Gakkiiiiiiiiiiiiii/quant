# QMT 事件驱动量化交易 Demo

日期：2026-04-20  
执行者：Codex

本项目是一个以 `Python + QMT + PostgreSQL + Vue` 为核心的量化交易示例工程，覆盖了：

- 历史数据加载与缓存
- 策略回测
- QMT 仿真盘计划生成与自动执行
- QMT 实盘探活与策略执行
- 本地 API 与前端看板展示

当前仓库的核心目标不是做单一策略研究，而是打通一条完整链路：

- `研究 / 回测`
- `仿真盘计划 / 自动执行`
- `QMT 实盘接入`
- `结果记录 / 报表展示`

## 目录说明

- `src/quant_demo/`：核心业务代码
- `scripts/`：运行脚本，包含历史数据、回测、仿真盘、实盘、对账与 API 启动
- `configs/`：环境与策略配置
- `frontend/joinquant-vue/`：Vue 3 + Vite 前端
- `data/`：本地 parquet、回测报告、仿真盘/实盘计划文件
- `runtime/qmt_client/`：QMT 客户端安装与用户目录
- `sql/schema.sql`：数据库初始化脚本
- `.codex/`：本地过程留痕、测试记录与审查文件

## 环境准备

项目默认使用本地虚拟环境 `.venv`。

常用命令：

```powershell
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe scripts\load_history.py
.\.venv\Scripts\python.exe scripts\run_backtest.py
.\.venv\Scripts\python.exe scripts\run_paper.py
.\.venv\Scripts\python.exe scripts\run_live.py --mode probe
```

数据库默认配置在各环境 YAML 中，当前主要使用 PostgreSQL。

## 项目启动

### 1. 启动本地 API

```powershell
.\.venv\Scripts\python.exe scripts\run_api.py
```

- 默认地址：[http://127.0.0.1:8011](http://127.0.0.1:8011)
- 健康检查：[http://127.0.0.1:8011/health](http://127.0.0.1:8011/health)

### 2. 启动前端开发服务

```powershell
cd frontend\joinquant-vue
D:\nodejs\npm.cmd run dev
```

- 默认地址：[http://127.0.0.1:8501](http://127.0.0.1:8501)
- 前端会将 `/api` 代理到 `http://127.0.0.1:8011`

### 3. 启动打包后的前端

```powershell
cd frontend\joinquant-vue
D:\nodejs\npm.cmd run build
cd ..\..
.\.venv\Scripts\python.exe scripts\run_dashboard.py
```

- `scripts/run_dashboard.py` 会托管 `frontend/joinquant-vue/dist/`
- 默认地址：[http://127.0.0.1:8501](http://127.0.0.1:8501)

## 当前策略清单

策略配置文件位于 [D:\project\quant\configs\strategy](/D:/project/quant/configs/strategy)。

### 微盘主线策略

- `joinquant_microcap_alpha.yaml`
  - 纯微盘 Alpha
- `joinquant_microcap_alpha_zhuang_filter.yaml`
  - 聚宽微盘 Alpha（庄股过滤）
  - 当前项目主线之一
  - 当前正式默认规则：
    - 不允许 `ST`
    - 不允许北交所
    - 继续排除 `*ST`
    - 继续排除退市整理
- `joinquant_microcap_alpha_zfe.yaml`
  - 聚宽微盘 Alpha（庄股过滤增强卖点）
- `joinquant_microcap_alpha_zr.yaml`
  - 聚宽微盘 Alpha（庄股最终替换）
- `joinquant_microcap_alpha_zro.yaml`
  - 聚宽微盘 Alpha（优化版）

### 微盘扩展实验策略

- `industry_weighted_microcap_alpha.yaml`
  - 行业增强微盘 Alpha
- `microcap_100b_layer_rotation.yaml`
  - 0-100 亿分层轮动微盘 Alpha
- `microcap_50b_layer_rotation.yaml`
  - 0-50 亿分层轮动微盘 Alpha
- `monster_prelude_alpha.yaml`
  - 妖股前奏 Alpha（实验策略）

### 通用策略 / 展示策略

- `first_alpha_v1.yaml`
  - 通用 Alpha 排序策略
- `stock_ranking.yaml`
  - 股票排序策略
- `etf_rotation.yaml`
  - ETF 轮动策略
- `joinquant_style.yaml`
  - 聚宽风格策略模板

## 推荐使用方式

如果你主要关注当前项目的主线策略，建议优先使用：

- 回测：`joinquant_microcap_alpha_zhuang_filter.yaml`
- 仿真盘：`joinquant_microcap_alpha_zhuang_filter.yaml`
- 实盘探活或计划执行：`joinquant_microcap_alpha_zhuang_filter.yaml`

## 回测执行

默认回测入口：

```powershell
.\.venv\Scripts\python.exe scripts\run_backtest.py --strategy configs\strategy\joinquant_microcap_alpha_zhuang_filter.yaml
```

回测完成后会输出 JSON 指标，包含：

- `total_return`
- `annualized_return`
- `max_drawdown`
- `turnover`
- `report_path`

回测报告通常落在：

- `data/reports/`
- `data/reports/saved_backtests/`

## 仿真盘执行

### 1. 预览明日交易计划

```powershell
.\.venv\Scripts\python.exe scripts\run_paper.py --mode preview --capital 100000 --strategy configs\strategy\joinquant_microcap_alpha_zhuang_filter.yaml
```

预览输出会包含：

- `plan_path`
- `signal_trade_date`
- `planned_execution_date`
- `strategy_total_asset`
- `preview_order_count`

当前 `run_paper.py` 已支持整组微盘策略正确走 `preview / execute` 分支，不再只支持纯微盘。

### 2. 明日定时执行仿真盘计划

```powershell
.\.venv\Scripts\python.exe scripts\run_paper_timed.py --capital 100000 --execute-at 09:35 --strategy configs\strategy\joinquant_microcap_alpha_zhuang_filter.yaml
```

可选参数：

- `--force-refresh-plan`
  - 强制重新生成计划，不复用已有 `latest` 计划文件

### 3. 计划文件位置

仿真盘计划文件默认写入：

- [D:\project\quant\data\reports\paper\trade_plans](/D:/project/quant/data/reports/paper/trade_plans)

关键文件：

- `microcap_t1_plan_latest.json`
- `microcap_t1_plan_YYYYMMDD_for_YYYYMMDD.json`
- `microcap_t1_execution_YYYYMMDD_for_YYYYMMDD.json`

### 4. 仿真盘测试建议

如果想验证自动交易程序是否正确，建议先在仿真盘做“有买有卖”的测试，而不是直接上实盘。

推荐流程：

1. 先跑一次 `preview`
2. 如果 `preview_order_count = 0`，可以手工制造一个“错仓”和一个“缺仓”
3. 再跑一次 `preview`，确认出现买单和卖单
4. 第二天用 `run_paper_timed.py` 自动执行

## QMT 实盘执行

### 1. 先做 QMT 探活

```powershell
.\.venv\Scripts\python.exe scripts\run_live.py --config configs\live.yaml --mode probe
```

该命令会输出：

- QMT 健康状态
- 当前账号资产
- 当前持仓
- 当前可读到的账户快照

### 2. 运行实盘策略

```powershell
.\.venv\Scripts\python.exe scripts\run_live.py --config configs\live.yaml --mode strategy --strategy configs\strategy\joinquant_microcap_alpha_zhuang_filter.yaml --capital 100000
```

注意事项：

- `configs/live.yaml` 中必须显式开启：
  - `qmt_trade_enabled: true`
- 当前 `run_live.py` 已支持 `--capital`
  - 可用于将实盘策略资金上限限制为指定金额
  - 例如 `100000`
- 如果不传或传 `0`，则按账户真实资产口径运行

### 3. 实盘与仿真盘的区别

- `run_paper.py`
  - 用于仿真盘计划生成与执行
  - 支持 `preview`
  - 支持 `--capital`
- `run_live.py`
  - 用于 QMT 实盘探活与真实策略执行
  - `--mode probe` 不下单，只检查
  - `--mode strategy` 才会真正执行策略
  - 需要 `qmt_trade_enabled: true`

## 当前策略执行特点

以 `joinquant_microcap_alpha_zhuang_filter` 为例：

- 会基于交易当日的历史 `ST / *ST / 退市整理` 状态做过滤
- 当前正式默认规则下：
  - 不买 `ST`
  - 不买北交所
  - 不买 `*ST`
  - 不买退市整理
- 如果持仓中的股票在交易当日被识别为 `ST`
  - 不会继续进入目标池
  - 会在后续可卖出的调仓日按 `not_in_target` 逻辑处理

## 常见脚本说明

- `scripts/load_history.py`
  - 加载或刷新历史数据
- `scripts/manage_history.py`
  - 管理历史数据缓存
- `scripts/run_backtest.py`
  - 运行回测
- `scripts/run_paper.py`
  - 生成或执行仿真盘计划
- `scripts/run_paper_timed.py`
  - 定时执行仿真盘计划
- `scripts/run_live.py`
  - QMT 实盘探活与策略执行
- `scripts/run_api.py`
  - 启动本地 API
- `scripts/run_dashboard.py`
  - 启动打包后的前端
- `scripts/reconcile_eod.py`
  - 日终对账
- `scripts/bootstrap_qmt.py`
  - QMT 客户端安装引导

## 已知现状

- `joinquant_microcap_engine.py` 中仍有少量 `pandas FutureWarning`
  - 不影响当前回测、仿真盘和实盘预览
- 首次跑微盘策略计划时，历史特征准备可能需要几十秒到 1 分钟
  - 这通常不是卡死，而是首次特征计算耗时
- README 历史版本存在乱码，本次已整体重写为中文可读版本

## 建议的日常使用顺序

1. `run_live.py --mode probe`
2. `run_paper.py --mode preview`
3. 确认计划文件与目标仓
4. `run_paper_timed.py` 做仿真盘自动执行验证
5. 确认无误后，再考虑 `run_live.py --mode strategy`
