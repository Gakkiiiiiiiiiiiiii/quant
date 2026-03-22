# QMT 事件驱动量化交易 Demo

日期：2026-03-21  
执行者：Codex

本项目依据 `QMT_事件驱动量化交易_Demo_技术设计文档_v1.0.docx` 落地一套可运行的量化交易 Demo，覆盖研究、回测、仿真、实盘接入骨架与展示层。系统遵循“研究层 / 交易核心层 / 接入层 / 存储层”分层，QMT 仅作为行情与交易接入层，交易内核完全由 Python 控制。

## 目录说明

- `src/quant_demo/`：核心系统代码。
- `scripts/`：运行脚本，包括历史数据加载、回测、仿真、实盘、对账与 QMT 安装引导。
- `configs/`：环境和策略配置。
- `sql/schema.sql`：设计文档对应的 PostgreSQL 初始化 DDL。
- `data/parquet/`：研究数据目录。
- `runtime/qmt_client/`：QMT 客户端安装和运行目录，与源码隔离。
- `.codex/`：本次实现过程留痕、上下文扫描、测试记录与审查报告。

## Python 虚拟环境

当前工作区已创建 `.venv`。由于沙箱网络和 `ensurepip` 权限限制，`.venv` 通过启用系统站点包复用了本机 Anaconda 环境中已有的 `pandas`、`SQLAlchemy`、`PyYAML`、`pytest`、`streamlit`、`pyarrow` 等依赖。

常用命令：

```powershell
.\.venv\Scripts\python.exe scripts\load_history.py
.\.venv\Scripts\python.exe scripts\run_backtest.py
.\.venv\Scripts\python.exe scripts\run_paper.py
.\.venv\Scripts\python.exe scripts\reconcile_eod.py
.\.venv\Scripts\python.exe -m pytest
```

## 默认实现说明

- 数据库默认使用 `SQLite` 文件库，方便本地 Demo 直接运行；设计文档要求的 PostgreSQL DDL 已同步到 `sql/schema.sql`。
- 历史数据默认落到 `data/parquet/history.parquet`。若本机未安装 QMT/xtquant，则自动生成示例 ETF 数据，保障回测链路可运行。
- QMT 实盘接入通过动态导入 `xtdata` / `xttrader`。如果 QMT 客户端尚未安装，系统仍可执行回测和仿真盘流程。
- `api/app.py` 提供基于标准库 `http.server` 的演示接口，避免无网环境下额外安装 FastAPI；`ui/streamlit_app.py` 保持 Streamlit 展示入口。
