from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from quant_demo.core.config import load_app_settings, load_strategy_settings
from quant_demo.db.session import create_session_factory
from quant_demo.experiment.evaluator import Evaluator
from quant_demo.experiment.qlib_engine import QlibBacktestEngine


def load_user_module(module_path: Path):
    spec = importlib.util.spec_from_file_location("user_pattern_strategy", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load strategy module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def configure_user_module(user_module, app_settings) -> None:
    import qlib

    def _init_qlib(provider_uri: str, region: str = app_settings.qlib_region) -> None:
        qlib.init(
            provider_uri=provider_uri,
            region=region,
            kernels=1,
            joblib_backend="threading",
            maxtasksperchild=1,
        )

    def _generate_target_weight_position(self, score, current, trade_start_time, trade_end_time):
        if score is None or len(score) == 0:
            return {}

        if isinstance(score, pd.Series):
            score = score.to_frame().T

        day_df = score.copy()
        if "instrument" in day_df.columns:
            day_df = day_df.set_index("instrument")
        day_df.index = day_df.index.astype(str)

        current_codes = [str(x) for x in current.get_stock_list()]
        keep = []

        for code in current_codes:
            row = day_df.loc[code] if code in day_df.index else None
            meta = self.entry_meta.get(code, {})
            pattern = str(meta.get("pattern", self.cfg.mode or ""))
            if row is None:
                exit_now = True
            elif pattern == "B1":
                exit_now = bool(row.get("b1_exit_flag", row.get("exit_flag", 0)))
            else:
                exit_now = bool(row.get("exit_flag", 0))

            if row is not None and not exit_now and pattern != "B1":
                stop_price = meta.get("stop_price")
                if stop_price is not None and float(row.get("close", float("inf"))) < float(stop_price):
                    exit_now = True

                get_stock_count = getattr(current, "get_stock_count", None)
                if callable(get_stock_count):
                    try:
                        hold_days = int(get_stock_count(code, "day"))
                    except TypeError:
                        hold_days = int(get_stock_count(code))
                    if hold_days >= self.cfg.max_holding_days and float(row.get("close", 0.0)) < float(row.get("st", 0.0)):
                        exit_now = True

            if not exit_now:
                keep.append(code)
            else:
                self.entry_meta.pop(code, None)

        candidate_df = day_df.loc[day_df.index.difference(keep)].copy()

        if self.cfg.mode == "B1":
            candidate_df = candidate_df[candidate_df["b1"] == 1]
        elif self.cfg.mode == "B2":
            candidate_df = candidate_df[candidate_df["b2"] == 1]
        elif self.cfg.mode == "B3":
            candidate_df = candidate_df[candidate_df["b3"] == 1]
        else:
            candidate_df = candidate_df[candidate_df["entry_flag"] == 1]

        candidate_df = candidate_df.sort_values("priority_score", ascending=False)
        if self.cfg.mode == "B1":
            slots = max(int(self.cfg.max_holdings) - len(keep), 0)
            new_codes = [str(code) for code in candidate_df.head(slots).index.tolist()]
            target_codes = keep + new_codes
        else:
            target_codes, keep, new_codes = _select_target_codes(
                candidate_df=candidate_df,
                score_df=day_df,
                keep_codes=keep,
                max_holdings=int(self.cfg.max_holdings),
                min_swap_score_gap=float(getattr(self.cfg, "min_swap_score_gap", 15.0)),
                holdings=getattr(self, "entry_meta", None),
            )

        for code in new_codes:
            row = candidate_df.loc[code]
            self.entry_meta[code] = {
                "stop_price": None if self.cfg.mode == "B1" else float(row.get("stop_price", user_module.np.nan)),
                "pattern": row.get("pattern", ""),
                "entry_dt": trade_start_time,
            }
        if not target_codes:
            return {}

        weight = 1.0 / len(target_codes)
        return {code: weight for code in target_codes}

    def _run_pattern_backtest(
        provider_uri: str,
        region: str = "cn",
        instruments: str = "all",
        start_time: str = "2018-01-01",
        end_time: str = "2024-12-31",
        benchmark: str = "SH000300",
        deal_price: str = "open",
        mode: str = "COMBINED",
        max_holdings: int = 10,
        risk_degree: float = 0.95,
        max_holding_days: int = 15,
        account: float = 10_000_000,
    ):
        from qlib.contrib.evaluate import backtest_daily, risk_analysis

        user_module.init_qlib(provider_uri=provider_uri, region=region)
        ohlcv = user_module.fetch_ohlcv(
            instruments=instruments,
            start_time=start_time,
            end_time=end_time,
            freq="day",
        )
        feats = user_module.build_indicators(ohlcv)
        signal = user_module.build_pattern_signals(feats)
        strategy = user_module.PatternSignalStrategy(
            config=user_module.StrategyConfig(
                mode=mode,
                max_holdings=max_holdings,
                risk_degree=risk_degree,
                max_holding_days=max_holding_days,
            ),
            signal=signal,
        )
        signal_dates = pd.Index(sorted(pd.to_datetime(signal.index.get_level_values("datetime")).unique()))
        effective_start = max(pd.Timestamp(start_time), signal_dates[0])
        effective_end = signal_dates[-2] if len(signal_dates) > 1 else signal_dates[-1]
        report_df, positions = backtest_daily(
            start_time=effective_start,
            end_time=effective_end,
            strategy=strategy,
            account=account,
            benchmark=benchmark,
            exchange_kwargs={
                "freq": "day",
                "limit_threshold": 0.095,
                "deal_price": deal_price,
                "open_cost": 0.0005,
                "close_cost": 0.0015,
                "min_cost": 5,
            },
        )
        excess = report_df["return"] - report_df["bench"]
        risk = risk_analysis(excess, freq="day")
        return {
            "ohlcv": ohlcv,
            "features": feats,
            "signal": signal,
            "report": report_df,
            "positions": positions,
            "risk": risk,
        }

    user_module.init_qlib = _init_qlib
    user_module.PatternSignalStrategy.generate_target_weight_position = _generate_target_weight_position
    user_module.run_pattern_backtest = _run_pattern_backtest


def ensure_provider(config_path: str, provider_strategy_path: str) -> tuple[Path, object]:
    app_settings = load_app_settings(config_path)
    strategy_settings = load_strategy_settings(provider_strategy_path)
    qlib_root = (ROOT / app_settings.qlib_source_dir).resolve()
    if str(qlib_root) not in sys.path:
        sys.path.insert(0, str(qlib_root))
    session_factory = create_session_factory(app_settings.database_url)
    engine = QlibBacktestEngine(session_factory, app_settings, strategy_settings)
    history = engine._load_history_with_benchmark()
    engine._rebuild_provider(history)
    return engine.qlib_provider_dir, app_settings


def build_equity_curve(report_df: pd.DataFrame, account: float) -> pd.DataFrame:
    ordered = report_df.sort_index().copy()
    ordered.index = pd.to_datetime(ordered.index)
    return_series = ordered["return"].astype(float) if "return" in ordered else pd.Series(0.0, index=ordered.index)
    cost_series = ordered["cost"].astype(float) if "cost" in ordered else pd.Series(0.0, index=ordered.index)
    turnover_series = ordered["turnover"].astype(float) if "turnover" in ordered else pd.Series(0.0, index=ordered.index)
    equity = float(account) * (1.0 + return_series).cumprod()
    return pd.DataFrame(
        {
            "trading_date": ordered.index.date,
            "equity": equity.astype(float),
            "cost": cost_series.cumsum().astype(float),
            "turnover": turnover_series.fillna(0.0).cumsum().astype(float),
        }
    )


def summarize_result(mode: str, report_df: pd.DataFrame, account: float) -> dict[str, object]:
    equity_curve = build_equity_curve(report_df, account)
    metrics = Evaluator().evaluate(equity_curve)
    return {
        "mode": mode,
        "total_return": metrics.total_return,
        "annualized_return": metrics.annualized_return,
        "max_drawdown": metrics.max_drawdown,
        "turnover": metrics.turnover,
        "start_date": str(equity_curve["trading_date"].min()) if not equity_curve.empty else "",
        "end_date": str(equity_curve["trading_date"].max()) if not equity_curve.empty else "",
        "trading_days": int(len(equity_curve)),
        "ending_equity": float(equity_curve["equity"].iloc[-1]) if not equity_curve.empty else float(account),
    }


def build_comparison_frame(report_map: dict[str, pd.DataFrame], account: float) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    benchmark_added = False
    for mode, report_df in report_map.items():
        ordered = report_df.sort_index().copy()
        ordered.index = pd.to_datetime(ordered.index)
        strategy_equity = float(account) * (1.0 + ordered["return"].astype(float)).cumprod()
        frames.append(pd.DataFrame({"datetime": ordered.index, "series": mode, "equity": strategy_equity.values}))
        if not benchmark_added and "bench" in ordered:
            benchmark_equity = float(account) * (1.0 + ordered["bench"].astype(float)).cumprod()
            frames.append(pd.DataFrame({"datetime": ordered.index, "series": "Benchmark", "equity": benchmark_equity.values}))
            benchmark_added = True
    if not frames:
        return pd.DataFrame(columns=["datetime", "series", "equity"])
    return pd.concat(frames, ignore_index=True)


def _normalize_stop_price(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _join_symbols(symbols: list[str]) -> str:
    return ",".join(str(symbol) for symbol in symbols)


def _to_qmt_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if "." in raw:
        return raw
    if len(raw) > 2 and raw[:2] in {"SH", "SZ", "BJ"}:
        return f"{raw[2:]}.{raw[:2]}"
    return raw


def _resolve_runtime_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (ROOT / path).resolve()


def _decode_payload(payload: bytes) -> str:
    for encoding in ("utf-8", "gb18030"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode("utf-8", errors="ignore")


def resolve_instrument_names(app_settings, symbols: list[str]) -> dict[str, str]:
    unique_symbols = sorted({_to_qmt_symbol(symbol) for symbol in symbols if symbol})
    if not unique_symbols:
        return {}

    python_path = _resolve_runtime_path(app_settings.qmt_bridge_python)
    script_path = (ROOT / "scripts" / "qmt_bridge.py").resolve()
    install_dir = _resolve_runtime_path(app_settings.qmt_install_dir)
    userdata_dir = _resolve_runtime_path(app_settings.qmt_userdata_dir)
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    details: dict[str, str] = {}

    for offset in range(0, len(unique_symbols), 200):
        batch = unique_symbols[offset : offset + 200]
        cmd = [
            str(python_path),
            str(script_path),
            "instrument-detail",
            "--install-dir",
            str(install_dir),
            "--userdata-dir",
            str(userdata_dir),
            "--symbols",
            ",".join(batch),
        ]
        if app_settings.qmt_account_id:
            cmd.extend(["--account-id", app_settings.qmt_account_id])
        completed = subprocess.run(cmd, cwd=ROOT, capture_output=True, check=False, env=env)
        if completed.returncode != 0:
            continue
        try:
            payload = json.loads(_decode_payload(completed.stdout).strip())
        except json.JSONDecodeError:
            continue
        for code, item in (payload.get("data", {}).get("details", {}) or {}).items():
            details[code] = (
                item.get("InstrumentName")
                or item.get("instrument_name")
                or item.get("name")
                or code
            )
    return details


def resolve_st_symbols(app_settings, symbols: list[str]) -> set[str]:
    name_map = resolve_instrument_names(app_settings, symbols)
    return {
        str(symbol)
        for symbol, name in name_map.items()
        if "ST" in str(name or "").upper()
    }


def build_daily_decision_frame(
    signal: pd.DataFrame,
    report_df: pd.DataFrame,
    mode: str,
    max_holdings: int,
    max_holding_days: int,
) -> tuple[pd.DataFrame, list[dict[str, object]], list[str]]:
    signal_frame = signal.reset_index().copy()
    signal_frame["instrument"] = signal_frame["instrument"].astype(str)
    signal_frame["datetime"] = pd.to_datetime(signal_frame["datetime"]).dt.normalize()
    signal_frame = signal_frame.sort_values(["datetime", "priority_score", "instrument"], ascending=[True, False, True])
    trade_dates = list(pd.to_datetime(report_df.index).normalize())
    grouped_frames = {
        trading_date: frame.set_index("instrument", drop=False)
        for trading_date, frame in signal_frame.groupby("datetime", sort=True)
    }
    account_frame = report_df.copy()
    account_frame.index = pd.to_datetime(account_frame.index).normalize()
    account_lookup = account_frame["account"].astype(float).to_dict() if "account" in account_frame else {}
    holdings: dict[str, dict[str, object]] = {}
    decision_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    used_symbols: set[str] = set()

    for trading_date in trade_dates:
        day_df = grouped_frames.get(trading_date)
        if day_df is None:
            day_df = pd.DataFrame(columns=signal_frame.columns).set_index(pd.Index([], name="instrument"))
        current_codes = list(holdings.keys())
        keep_codes: list[str] = []
        sell_codes: list[str] = []

        for code in current_codes:
            row = day_df.loc[code] if code in day_df.index else None
            exit_now = row is None or bool(row.get("exit_flag", 0))
            meta = holdings.get(code, {})
            if row is not None and not exit_now:
                stop_price = meta.get("stop_price")
                if stop_price is not None and float(row.get("close", float("inf"))) < float(stop_price):
                    exit_now = True
                hold_days = int(meta.get("hold_days", 0))
                if hold_days >= max_holding_days and float(row.get("close", 0.0)) < float(row.get("st", 0.0)):
                    exit_now = True
            if exit_now:
                sell_codes.append(code)
                sell_price = float(row.get("close")) if row is not None else float(meta.get("last_close", meta.get("buy_price", 0.0)))
                buy_price = float(meta.get("buy_price", 0.0))
                buy_amount = float(meta.get("buy_amount", 0.0))
                pnl = buy_amount * (sell_price / buy_price - 1.0) if buy_price > 0 else 0.0
                trade_rows.append(
                    {
                        "日期": pd.Timestamp(meta.get("buy_date")).date().isoformat() if meta.get("buy_date") is not None else "",
                        "策略": mode,
                        "操作": "BUY",
                        "标的": code,
                        "股票代码": _to_qmt_symbol(code),
                        "标的名称": "",
                        "BUY金额": round(buy_amount, 2),
                        "SELL日期": trading_date.date().isoformat(),
                        "盈亏金额": round(pnl, 2),
                        "收益率": round((sell_price / buy_price - 1.0) if buy_price > 0 else 0.0, 6),
                    }
                )
                used_symbols.add(code)
            else:
                keep_codes.append(code)

        candidate_df = day_df.loc[day_df.index.difference(keep_codes)].copy()
        if mode == "B1":
            candidate_df = candidate_df[candidate_df["b1"] == 1]
        elif mode == "B2":
            candidate_df = candidate_df[candidate_df["b2"] == 1]
        elif mode == "B3":
            candidate_df = candidate_df[candidate_df["b3"] == 1]
        else:
            candidate_df = candidate_df[candidate_df["entry_flag"] == 1]
        candidate_df = candidate_df.assign(_instrument_order=candidate_df.index.astype(str))
        candidate_df = candidate_df.sort_values(["priority_score", "_instrument_order"], ascending=[False, True])

        candidate_codes = [str(symbol) for symbol in candidate_df.index.tolist()]
        slots = max(max_holdings - len(keep_codes), 0)
        buy_codes = candidate_codes[:slots]
        target_codes = keep_codes + buy_codes
        account_value = float(account_lookup.get(trading_date, 0.0))
        buy_amount = (account_value / len(target_codes)) if target_codes else 0.0

        next_holdings: dict[str, dict[str, object]] = {}
        for code in keep_codes:
            meta = dict(holdings[code])
            meta["hold_days"] = int(meta.get("hold_days", 0)) + 1
            if code in day_df.index:
                meta["last_close"] = float(day_df.loc[code].get("close", meta.get("last_close", 0.0)))
            next_holdings[code] = meta
        for code in buy_codes:
            row = candidate_df.loc[code]
            next_holdings[code] = {
                "stop_price": _normalize_stop_price(row.get("stop_price")),
                "pattern": row.get("pattern", ""),
                "entry_dt": trading_date,
                "buy_date": trading_date,
                "buy_price": float(row.get("close", 0.0)),
                "buy_amount": buy_amount,
                "last_close": float(row.get("close", 0.0)),
                "hold_days": 1,
            }
            used_symbols.add(code)
        holdings = next_holdings
        hold_codes = list(holdings.keys())

        decision_rows.append(
            {
                "trading_date": trading_date.date().isoformat(),
                "mode": mode,
                "signal_count": int(len(candidate_codes)),
                "buy_count": int(len(buy_codes)),
                "sell_count": int(len(sell_codes)),
                "hold_count": int(len(hold_codes)),
                "candidate_symbols": _join_symbols([_to_qmt_symbol(symbol) for symbol in candidate_codes]),
                "buy_symbols": _join_symbols([_to_qmt_symbol(symbol) for symbol in buy_codes]),
                "sell_symbols": _join_symbols([_to_qmt_symbol(symbol) for symbol in sell_codes]),
                "hold_symbols": _join_symbols([_to_qmt_symbol(symbol) for symbol in hold_codes]),
            }
        )

    for code, meta in holdings.items():
        trade_rows.append(
            {
                "日期": pd.Timestamp(meta.get("buy_date")).date().isoformat() if meta.get("buy_date") is not None else "",
                "策略": mode,
                "操作": "BUY",
                "标的": code,
                "股票代码": _to_qmt_symbol(code),
                "标的名称": "",
                "BUY金额": round(float(meta.get("buy_amount", 0.0)), 2),
                "SELL日期": "",
                "盈亏金额": None,
                "收益率": None,
            }
        )
        used_symbols.add(code)

    return pd.DataFrame(decision_rows), trade_rows, sorted(used_symbols)


def build_trade_ledger_frame(
    trade_rows: list[dict[str, object]],
    name_map: dict[str, str],
) -> pd.DataFrame:
    normalized_rows: list[dict[str, object]] = []
    for row in trade_rows:
        stock_code = row["股票代码"]
        row["标的名称"] = name_map.get(stock_code, stock_code)
        normalized_rows.append(row)
    ledger = pd.DataFrame(
        normalized_rows,
        columns=["日期", "策略", "操作", "标的", "股票代码", "标的名称", "BUY金额", "SELL日期", "盈亏金额", "收益率"],
    )
    if not ledger.empty:
        ledger = ledger.sort_values(["日期", "策略", "股票代码"], ascending=[True, True, True]).reset_index(drop=True)
    return ledger


def write_daily_detail_outputs(output_dir: Path, app_settings, mode: str, signal: pd.DataFrame, report_df: pd.DataFrame, max_holdings: int, max_holding_days: int) -> dict[str, str]:
    decision_df, trade_rows, used_symbols = build_daily_decision_frame(signal, report_df, mode, max_holdings, max_holding_days)
    name_map = resolve_instrument_names(app_settings, used_symbols)
    action_df = build_trade_ledger_frame(trade_rows, name_map)
    decision_path = output_dir / f"{mode.lower()}_daily_decisions.csv"
    action_path = output_dir / f"{mode.lower()}_daily_actions.csv"
    markdown_path = output_dir / f"{mode.lower()}_daily_summary.md"
    decision_df.to_csv(decision_path, index=False, encoding="utf-8-sig")
    action_df.to_csv(action_path, index=False, encoding="utf-8-sig")

    lines = [
        f"# {mode} Trade Ledger",
        "",
        "| 日期 | 策略 | 操作 | 股票代码 | 标的名称 | BUY金额 | SELL日期 | 盈亏金额 | 收益率 |",
        "| --- | --- | --- | --- | --- | ---: | --- | ---: | ---: |",
    ]
    for item in action_df.to_dict(orient="records"):
        lines.append(
            f"| {item['日期'] or '-'} | {item['策略']} | {item['操作']} | {item['股票代码']} | {item['标的名称']} | {item['BUY金额'] if pd.notna(item['BUY金额']) else '-'} | {item['SELL日期'] or '-'} | {item['盈亏金额'] if pd.notna(item['盈亏金额']) else '-'} | {item['收益率'] if pd.notna(item['收益率']) else '-'} |"
        )
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {
        "daily_decision_path": str(decision_path),
        "daily_action_path": str(action_path),
        "daily_markdown_path": str(markdown_path),
    }


def _empty_signal_frame(signal_frame: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(columns=signal_frame.columns).set_index(pd.Index([], name="instrument"))


def _select_candidates(day_df: pd.DataFrame, mode: str, excluded_codes: set[str], blocked_codes: set[str] | None = None) -> pd.DataFrame:
    if day_df.empty:
        return day_df.copy()
    blocked = set(blocked_codes or set())
    candidate_df = day_df.loc[day_df.index.difference(sorted(set(excluded_codes) | blocked))].copy()
    if "volume" in candidate_df.columns:
        candidate_df = candidate_df[candidate_df["volume"].fillna(0) > 0]
    if mode == "B1":
        candidate_df = candidate_df[candidate_df["b1"] == 1]
    elif mode == "B2":
        candidate_df = candidate_df[candidate_df["b2"] == 1]
    elif mode == "B3":
        candidate_df = candidate_df[candidate_df["b3"] == 1]
    else:
        candidate_df = candidate_df[candidate_df["entry_flag"] == 1]
    if candidate_df.empty:
        return candidate_df
    candidate_df = candidate_df.assign(_instrument_order=candidate_df.index.astype(str))
    return candidate_df.sort_values(["priority_score", "_instrument_order"], ascending=[False, True])


def _read_priority_score(day_df: pd.DataFrame, code: str, fallback: float = float("-inf")) -> float:
    if day_df is None or day_df.empty or code not in day_df.index:
        return float(fallback)
    try:
        value = float(day_df.loc[code].get("priority_score", fallback))
    except (TypeError, ValueError):
        return float(fallback)
    return value if pd.notna(value) else float(fallback)


def _effective_holding_score(
    score_df: pd.DataFrame,
    code: str,
    holdings: dict[str, dict[str, object]] | None,
) -> float:
    base_score = _read_priority_score(score_df, code)
    if not holdings or code not in holdings:
        return base_score
    meta = holdings.get(code, {})
    entry_score = float(meta.get("entry_score", base_score))
    hold_days = int(meta.get("hold_days", 1))
    if hold_days <= 5:
        buffered_score = entry_score * max(0.82, 1.0 - 0.04 * (hold_days - 1))
    else:
        buffered_score = entry_score * max(0.60, 0.82 - 0.03 * (hold_days - 5))
    return max(base_score, buffered_score)


def _select_target_codes(
    *,
    candidate_df: pd.DataFrame,
    score_df: pd.DataFrame,
    keep_codes: list[str],
    max_holdings: int,
    min_swap_score_gap: float,
    holdings: dict[str, dict[str, object]] | None = None,
) -> tuple[list[str], list[str], list[str]]:
    if max_holdings <= 0:
        return [], keep_codes, []

    ranked_keep = sorted(
        [str(code) for code in keep_codes],
        key=lambda code: (_effective_holding_score(score_df, code, holdings), code),
        reverse=True,
    )
    ranked_candidates = [str(code) for code in candidate_df.index.tolist()]

    if not ranked_candidates:
        return ranked_keep[:max_holdings], ranked_keep[:max_holdings], []

    if len(ranked_keep) >= max_holdings:
        strongest_candidate_score = _read_priority_score(score_df, ranked_candidates[0])
        weakest_keep_score = min((_effective_holding_score(score_df, code, holdings) for code in ranked_keep), default=float("inf"))
        if strongest_candidate_score - weakest_keep_score < float(min_swap_score_gap):
            return ranked_keep[:max_holdings], ranked_keep[:max_holdings], []

    pool: list[str] = ranked_keep[:]
    for code in ranked_candidates:
        if code not in pool:
            pool.append(code)
        pool = sorted(
            pool,
            key=lambda symbol: (
                _effective_holding_score(score_df, symbol, holdings) if symbol in ranked_keep else _read_priority_score(score_df, symbol),
                symbol,
            ),
            reverse=True,
        )[:max_holdings]

    final_codes = pool
    final_keep = [code for code in ranked_keep if code in final_codes]
    buy_codes = [code for code in final_codes if code not in ranked_keep]
    return final_codes, final_keep, buy_codes


def _limit_new_buys(
    *,
    buy_codes: list[str],
    total_equity_before_buy: float,
    current_value: float,
    risk_degree: float,
    min_position_ratio: float,
) -> list[str]:
    selected = list(buy_codes)
    if not selected:
        return selected
    min_ratio = max(float(min_position_ratio), 0.0)
    if min_ratio <= 0:
        return selected
    min_position_value = float(total_equity_before_buy) * min_ratio
    available_for_new = max(float(total_equity_before_buy) * float(risk_degree) - float(current_value), 0.0)
    while selected:
        per_position_budget = available_for_new / len(selected)
        if per_position_budget >= min_position_value:
            break
        selected.pop()
    return selected


def _trade_cost(amount: float, rate: float, min_cost: float) -> float:
    if amount <= 0:
        return 0.0
    return max(float(amount) * float(rate), float(min_cost))


def _apply_slippage(price: float, *, side: str, slippage_rate: float) -> float:
    base_price = float(price)
    if base_price <= 0:
        return 0.0
    slip = max(float(slippage_rate), 0.0)
    if side == "buy":
        return base_price * (1.0 + slip)
    if side == "sell":
        return base_price * (1.0 - slip)
    return base_price


def _resolve_trade_price(row: pd.Series | None, price_field: str, fallback: float) -> float:
    if row is None:
        return float(fallback)
    try:
        price = float(row.get(price_field, row.get("open", fallback)))
    except (TypeError, ValueError):
        price = float(fallback)
    if pd.isna(price) or price <= 0:
        try:
            price = float(row.get("open", fallback))
        except (TypeError, ValueError):
            price = float(fallback)
    return float(price)


def _decide_exit_from_signal(
    signal_row: pd.Series | None,
    meta: dict[str, object],
    *,
    max_holding_days: int,
) -> tuple[bool, str]:
    if signal_row is None:
        return True, "signal_missing"
    pattern = str(meta.get("pattern", ""))
    if pattern == "B1":
        signal_close = float(signal_row.get("close", float("inf")))
        signal_st = float(signal_row.get("st", 0.0))
        if bool(signal_row.get("b1_lt_hard_stop_flag", 0)):
            return True, "b1_lt_hard_stop"
        if bool(signal_row.get("b1_st_stop_flag", 0)):
            return True, "b1_st_stop"
        if bool(meta.get("entry_platform_established", False)):
            platform_low = float(meta.get("entry_platform_low", float("nan")))
            if pd.notna(platform_low) and signal_close < platform_low * 0.99:
                return True, "b1_platform_stop"
        signal_low = float(meta.get("entry_signal_low", float("nan")))
        if pd.notna(signal_low) and signal_close < signal_low * 0.99:
            return True, "b1_signal_low_stop"
        hold_days = int(meta.get("hold_days", 0))
        entry_signal_close = float(meta.get("entry_signal_close", float("nan")))
        max_high_since_entry = float(meta.get("max_high_since_entry", float("nan")))
        min_low_since_entry = float(meta.get("min_low_since_entry", float("nan")))
        if (
            hold_days >= 8
            and pd.notna(entry_signal_close)
            and pd.notna(max_high_since_entry)
            and max_high_since_entry < entry_signal_close * 1.05
            and signal_close < signal_st
        ):
            return True, "b1_time_stop_a"
        if (
            hold_days >= 12
            and pd.notna(entry_signal_close)
            and pd.notna(max_high_since_entry)
            and pd.notna(min_low_since_entry)
            and max_high_since_entry < entry_signal_close * 1.08
            and min_low_since_entry > entry_signal_close * 0.95
        ):
            return True, "b1_time_stop_b"
        if bool(signal_row.get("b1_exit_flag", signal_row.get("exit_flag", 0))):
            return True, "b1_distribution"
        return False, ""
    if bool(signal_row.get("exit_flag", 0)):
        return True, "signal_exit"
    stop_price = meta.get("stop_price")
    signal_close = float(signal_row.get("close", float("inf")))
    if stop_price is not None and signal_close < float(stop_price):
        return True, "stop_loss"
    hold_days = int(meta.get("hold_days", 0))
    signal_st = float(signal_row.get("st", 0.0))
    if hold_days >= max_holding_days and signal_close < signal_st:
        return True, "time_stop"
    return False, ""


def _load_local_ohlcv(app_settings, instruments, start_time: str, end_time: str) -> pd.DataFrame:
    history_path = _resolve_runtime_path(app_settings.history_parquet)
    frame = pd.read_parquet(history_path, columns=["trading_date", "symbol", "open", "high", "low", "close", "volume", "amount"])
    frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.normalize()
    start_dt = pd.Timestamp(start_time).normalize()
    end_dt = pd.Timestamp(end_time).normalize()
    frame = frame[(frame["trading_date"] >= start_dt) & (frame["trading_date"] <= end_dt)].copy()
    if instruments != "all":
        allowed = {_to_qmt_symbol(symbol) for symbol in instruments}
        frame = frame[frame["symbol"].isin(allowed)].copy()
    if frame.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume", "amount"])
    frame = frame.rename(columns={"symbol": "instrument", "trading_date": "datetime"})
    frame = frame.sort_values(["instrument", "datetime"]).set_index(["instrument", "datetime"])
    return frame[["open", "high", "low", "close", "volume", "amount"]]


def _shift_start_by_trading_days(trading_dates: pd.Index, start_time: str, warmup_bars: int) -> pd.Timestamp:
    normalized_dates = pd.Index(pd.to_datetime(trading_dates)).dropna().sort_values().unique()
    if len(normalized_dates) == 0:
        return pd.Timestamp(start_time).normalize()
    normalized_dates = pd.DatetimeIndex(normalized_dates).normalize()
    start_dt = pd.Timestamp(start_time).normalize()
    start_pos = int(normalized_dates.searchsorted(start_dt, side="left"))
    if start_pos >= len(normalized_dates):
        return normalized_dates[0]
    warmup_pos = max(start_pos - int(max(warmup_bars, 0)), 0)
    return normalized_dates[warmup_pos]


def _resolve_warmup_start(app_settings, start_time: str, warmup_bars: int) -> str:
    history_path = _resolve_runtime_path(app_settings.history_parquet)
    frame = pd.read_parquet(history_path, columns=["trading_date"])
    warmup_start = _shift_start_by_trading_days(frame["trading_date"], start_time, warmup_bars)
    return warmup_start.date().isoformat()


def _benchmark_returns(user_module, app_settings, start_time: str, end_time: str) -> pd.Series:
    benchmark_symbol = _to_qmt_symbol(app_settings.qlib_benchmark_symbol or app_settings.qlib_benchmark)
    if not benchmark_symbol:
        return pd.Series(dtype=float)
    bench = _load_local_ohlcv(app_settings, [benchmark_symbol], start_time, end_time)
    if bench.empty:
        return pd.Series(dtype=float)
    bench = bench.reset_index().sort_values("datetime")
    close_series = bench.set_index(pd.to_datetime(bench["datetime"]).dt.normalize())["close"].astype(float)
    return close_series.pct_change().fillna(0.0)


def simulate_pattern_backtest(
    *,
    user_module,
    app_settings,
    provider_uri: Path,
    mode: str,
    start_time: str,
    end_time: str,
    account: float,
    max_holdings: int,
    risk_degree: float,
    max_holding_days: int,
    buy_price_field: str,
    sell_price_field: str,
    open_cost: float,
    close_cost: float,
    min_cost: float,
    slippage_rate: float,
    min_position_ratio: float,
    min_swap_score_gap: float,
    lot_size: int = 100,
) -> dict[str, object]:
    from qlib.contrib.evaluate import risk_analysis

    start_dt = pd.Timestamp(start_time).normalize()
    end_dt = pd.Timestamp(end_time).normalize()
    warmup_start = _resolve_warmup_start(app_settings, start_time, warmup_bars=114)

    ohlcv = _load_local_ohlcv(app_settings, "all", warmup_start, end_time)
    features = user_module.build_indicators(ohlcv)
    signal = user_module.build_pattern_signals(features)

    signal_frame = signal.reset_index().copy()
    signal_frame["instrument"] = signal_frame["instrument"].astype(str)
    signal_frame["datetime"] = pd.to_datetime(signal_frame["datetime"]).dt.normalize()
    signal_frame = signal_frame.sort_values(["datetime", "priority_score", "instrument"], ascending=[True, False, True])
    grouped_frames = {
        trading_date: frame.set_index("instrument", drop=False)
        for trading_date, frame in signal_frame.groupby("datetime", sort=True)
    }
    all_trade_dates = sorted(grouped_frames.keys())
    trade_dates = [trade_date for trade_date in all_trade_dates if start_dt <= trade_date <= end_dt]
    trade_date_positions = {trade_date: index for index, trade_date in enumerate(all_trade_dates)}
    benchmark_returns = _benchmark_returns(user_module, app_settings, start_time, end_time)
    blocked_symbols = resolve_st_symbols(app_settings, signal_frame["instrument"].dropna().astype(str).unique().tolist())

    cash = float(account)
    holdings: dict[str, dict[str, object]] = {}
    report_rows: list[dict[str, object]] = []
    decision_rows: list[dict[str, object]] = []
    ledger_rows: list[dict[str, object]] = []
    cumulative_turnover = 0.0
    cumulative_cost = 0.0
    previous_account = float(account)

    for trade_date in trade_dates:
        execution_df = grouped_frames.get(trade_date, _empty_signal_frame(signal_frame))
        date_position = trade_date_positions.get(trade_date)
        previous_signal_date = all_trade_dates[date_position - 1] if date_position is not None and date_position > 0 else None
        previous_signal_df = grouped_frames.get(previous_signal_date, _empty_signal_frame(signal_frame))

        sell_codes: list[str] = []
        keep_codes: list[str] = []
        sell_reasons: dict[str, str] = {}
        daily_turnover = 0.0
        daily_cost = 0.0

        for code, meta in list(holdings.items()):
            signal_row = previous_signal_df.loc[code] if code in previous_signal_df.index else None
            exit_now, sell_reason = _decide_exit_from_signal(
                signal_row,
                meta,
                max_holding_days=max_holding_days,
            )
            if exit_now:
                sell_codes.append(code)
                sell_reasons[code] = sell_reason or "signal_exit"
            else:
                keep_codes.append(code)

        candidate_df = _select_candidates(previous_signal_df, mode, set(keep_codes), blocked_codes=blocked_symbols)
        if mode == "B1":
            target_keep_codes = list(keep_codes)
            remaining_slots = max(int(max_holdings) - len(target_keep_codes), 0)
            planned_buy_codes = [str(code) for code in candidate_df.head(remaining_slots).index.tolist()]
            target_codes = target_keep_codes + planned_buy_codes
        else:
            target_codes, target_keep_codes, planned_buy_codes = _select_target_codes(
                candidate_df=candidate_df,
                score_df=previous_signal_df,
                keep_codes=keep_codes,
                max_holdings=max_holdings,
                min_swap_score_gap=min_swap_score_gap,
                holdings=holdings,
            )
            rotation_sell_codes = [code for code in keep_codes if code not in target_keep_codes]
            for code in rotation_sell_codes:
                if code not in sell_reasons:
                    sell_reasons[code] = "score_swap"
                sell_codes.append(code)
            keep_codes = target_keep_codes

        for code in sell_codes:
            meta = holdings.pop(code)
            row = execution_df.loc[code] if code in execution_df.index else None
            raw_sell_price = _resolve_trade_price(row, sell_price_field, float(meta.get("last_close", meta.get("buy_price", 0.0))))
            sell_price = _apply_slippage(raw_sell_price, side="sell", slippage_rate=slippage_rate)
            shares = int(meta.get("shares", 0))
            sell_amount = shares * sell_price
            sell_fee = _trade_cost(sell_amount, close_cost, min_cost)
            cash += sell_amount - sell_fee
            daily_turnover += sell_amount
            daily_cost += sell_fee
            cumulative_turnover += sell_amount
            cumulative_cost += sell_fee
            total_buy_cash = float(meta.get("buy_amount", 0.0)) + float(meta.get("buy_fee", 0.0))
            pnl = sell_amount - sell_fee - total_buy_cash
            ledger_rows.append(
                {
                    "日期": pd.Timestamp(meta.get("buy_date")).date().isoformat() if meta.get("buy_date") is not None else "",
                    "策略(B1 B2 B3)": mode,
                    "BUY": "BUY",
                    "标的": code,
                    "股票代码": _to_qmt_symbol(code),
                    "标的名称": "",
                    "买入信号日期": pd.Timestamp(meta.get("signal_date")).date().isoformat() if meta.get("signal_date") is not None else "",
                    "买入价格": round(float(meta.get("buy_price", 0.0)), 4),
                    "卖出价格": round(float(sell_price), 4),
                    "买入评分": round(float(meta.get("entry_score", 0.0)), 4),
                    "卖出评分": round(_read_priority_score(previous_signal_df, code, float(meta.get("last_score", 0.0))), 4),
                    "BUY金额": round(float(meta.get("buy_amount", 0.0)), 2),
                    "BUY股数": int(meta.get("shares", 0)),
                    "SELL日期": trade_date.date().isoformat(),
                    "卖出原因": sell_reasons.get(code, "signal_exit"),
                    "这个标的这次操作的盈亏金额": round(pnl, 2),
                    "收益率": round(pnl / float(meta.get("buy_amount", 0.0)), 6) if float(meta.get("buy_amount", 0.0)) > 0 else None,
                }
            )

        current_value = 0.0
        for code in keep_codes:
            row = execution_df.loc[code] if code in execution_df.index else None
            if row is not None:
                holdings[code]["last_close"] = float(row.get("close", holdings[code].get("last_close", 0.0)))
                holdings[code]["last_score"] = _read_priority_score(previous_signal_df, code, float(holdings[code].get("last_score", 0.0)))
                holdings[code]["max_high_since_entry"] = max(float(holdings[code].get("max_high_since_entry", float("-inf"))), float(row.get("high", holdings[code].get("last_close", 0.0))))
                holdings[code]["min_low_since_entry"] = min(float(holdings[code].get("min_low_since_entry", float("inf"))), float(row.get("low", holdings[code].get("last_close", 0.0))))
            holdings[code]["hold_days"] = int(holdings[code].get("hold_days", 0)) + 1
            current_value += int(holdings[code].get("shares", 0)) * float(holdings[code].get("last_close", 0.0))

        total_equity_before_buy = cash + current_value
        candidate_codes = _limit_new_buys(
            buy_codes=planned_buy_codes,
            total_equity_before_buy=total_equity_before_buy,
            current_value=current_value,
            risk_degree=risk_degree,
            min_position_ratio=min_position_ratio,
        )

        target_invested_value = total_equity_before_buy * float(risk_degree)
        available_for_new = max(target_invested_value - current_value, 0.0)
        per_position_budget = available_for_new / len(candidate_codes) if candidate_codes else 0.0
        buy_codes: list[str] = []

        for code in candidate_codes:
            if code not in execution_df.index:
                continue
            row = execution_df.loc[code]
            raw_buy_price = _resolve_trade_price(row, buy_price_field, float(row.get("open", 0.0)))
            buy_price = _apply_slippage(raw_buy_price, side="buy", slippage_rate=slippage_rate)
            if buy_price <= 0:
                continue
            tentative_shares = int(per_position_budget // (buy_price * lot_size)) * lot_size
            if tentative_shares <= 0:
                continue
            buy_amount = tentative_shares * buy_price
            buy_fee = _trade_cost(buy_amount, open_cost, min_cost)
            while tentative_shares > 0 and cash < buy_amount + buy_fee:
                tentative_shares -= lot_size
                buy_amount = tentative_shares * buy_price
                buy_fee = _trade_cost(buy_amount, open_cost, min_cost)
            if tentative_shares <= 0:
                continue
            cash -= buy_amount + buy_fee
            daily_turnover += buy_amount
            daily_cost += buy_fee
            cumulative_turnover += buy_amount
            cumulative_cost += buy_fee
            holdings[code] = {
                "shares": tentative_shares,
                "buy_date": trade_date,
                "signal_date": previous_signal_date,
                "buy_price": buy_price,
                "buy_amount": buy_amount,
                "buy_fee": buy_fee,
                "pattern": str(previous_signal_df.loc[code].get("pattern", mode)) if code in previous_signal_df.index else mode,
                "stop_price": _normalize_stop_price(previous_signal_df.loc[code].get("stop_price")) if code in previous_signal_df.index else None,
                "entry_signal_low": float(previous_signal_df.loc[code].get("low", float("nan"))) if code in previous_signal_df.index else float("nan"),
                "entry_signal_close": float(previous_signal_df.loc[code].get("close", float("nan"))) if code in previous_signal_df.index else float("nan"),
                "entry_platform_established": bool(previous_signal_df.loc[code].get("b1_platform_established", 0)) if code in previous_signal_df.index else False,
                "entry_platform_low": float(previous_signal_df.loc[code].get("b1_platform_low", float("nan"))) if code in previous_signal_df.index else float("nan"),
                "entry_score": _read_priority_score(previous_signal_df, code, 0.0),
                "hold_days": 1,
                "last_close": float(row.get("close", buy_price)),
                "max_high_since_entry": float(row.get("high", buy_price)),
                "min_low_since_entry": float(row.get("low", buy_price)),
                "last_score": _read_priority_score(previous_signal_df, code, 0.0),
            }
            buy_codes.append(code)

        hold_codes = sorted(holdings.keys())
        end_value = sum(int(meta.get("shares", 0)) * float(meta.get("last_close", 0.0)) for meta in holdings.values())
        end_account = cash + end_value
        daily_return = (end_account / previous_account - 1.0) if previous_account > 0 else 0.0
        turnover_rate = (daily_turnover / previous_account) if previous_account > 0 else 0.0
        cost_rate = (daily_cost / previous_account) if previous_account > 0 else 0.0
        bench = float(benchmark_returns.get(trade_date, 0.0))

        report_rows.append(
            {
                "datetime": trade_date.date().isoformat(),
                "account": end_account,
                "return": daily_return,
                "total_turnover": cumulative_turnover,
                "turnover": turnover_rate,
                "total_cost": cumulative_cost,
                "cost": cost_rate,
                "value": end_value,
                "cash": cash,
                "bench": bench,
            }
        )
        decision_rows.append(
            {
                "trading_date": trade_date.date().isoformat(),
                "mode": mode,
                "signal_count": int(len(_select_candidates(previous_signal_df, mode, set(), blocked_codes=blocked_symbols).index)),
                "buy_count": int(len(buy_codes)),
                "sell_count": int(len(sell_codes)),
                "hold_count": int(len(hold_codes)),
                "candidate_symbols": _join_symbols([_to_qmt_symbol(symbol) for symbol in candidate_df.index.tolist()]),
                "buy_symbols": _join_symbols([_to_qmt_symbol(symbol) for symbol in buy_codes]),
                "sell_symbols": _join_symbols([_to_qmt_symbol(symbol) for symbol in sell_codes]),
                "hold_symbols": _join_symbols([_to_qmt_symbol(symbol) for symbol in hold_codes]),
            }
        )
        previous_account = end_account

    for code, meta in holdings.items():
        ledger_rows.append(
            {
                "日期": pd.Timestamp(meta.get("buy_date")).date().isoformat() if meta.get("buy_date") is not None else "",
                "策略(B1 B2 B3)": mode,
                "BUY": "BUY",
                "标的": code,
                "股票代码": _to_qmt_symbol(code),
                "标的名称": "",
                "买入信号日期": pd.Timestamp(meta.get("signal_date")).date().isoformat() if meta.get("signal_date") is not None else "",
                "买入价格": round(float(meta.get("buy_price", 0.0)), 4),
                "卖出价格": None,
                "买入评分": round(float(meta.get("entry_score", 0.0)), 4),
                "卖出评分": None,
                "BUY金额": round(float(meta.get("buy_amount", 0.0)), 2),
                "BUY股数": int(meta.get("shares", 0)),
                "SELL日期": "",
                "卖出原因": "",
                "这个标的这次操作的盈亏金额": None,
                "收益率": None,
            }
        )

    report_df = pd.DataFrame(report_rows).set_index(pd.to_datetime(pd.DataFrame(report_rows)["datetime"])) if report_rows else pd.DataFrame(columns=["account", "return", "total_turnover", "turnover", "total_cost", "cost", "value", "cash", "bench"])
    if not report_df.empty:
        report_df.index.name = "datetime"
        report_df = report_df[["account", "return", "total_turnover", "turnover", "total_cost", "cost", "value", "cash", "bench"]]
    excess = report_df["return"] - report_df["bench"] if not report_df.empty else pd.Series(dtype=float)
    risk = risk_analysis(excess, freq="day") if not excess.empty else pd.DataFrame()
    decision_df = pd.DataFrame(decision_rows)
    name_map = resolve_instrument_names(app_settings, [row["股票代码"] for row in ledger_rows])
    ledger_df = pd.DataFrame(ledger_rows)
    if not ledger_df.empty:
        ledger_df["标的名称"] = ledger_df["股票代码"].map(name_map).fillna(ledger_df["股票代码"])
        ledger_df = ledger_df[
            [
                "日期",
                "买入信号日期",
                "策略(B1 B2 B3)",
                "BUY",
                "标的",
                "股票代码",
                "标的名称",
                "买入价格",
                "卖出价格",
                "买入评分",
                "卖出评分",
                "BUY股数",
                "BUY金额",
                "SELL日期",
                "卖出原因",
                "这个标的这次操作的盈亏金额",
                "收益率",
            ]
        ]
        ledger_df = ledger_df.sort_values(["日期", "策略(B1 B2 B3)", "股票代码"], ascending=[True, True, True]).reset_index(drop=True)

    return {
        "ohlcv": ohlcv,
        "features": features,
        "signal": signal,
        "report": report_df,
        "risk": risk,
        "decisions": decision_df,
        "ledger": ledger_df,
    }


def write_trade_outputs(output_dir: Path, mode: str, decision_df: pd.DataFrame, ledger_df: pd.DataFrame) -> dict[str, str]:
    decision_path = output_dir / f"{mode.lower()}_daily_decisions.csv"
    action_path = output_dir / f"{mode.lower()}_daily_actions.csv"
    markdown_path = output_dir / f"{mode.lower()}_daily_summary.md"
    decision_df.to_csv(decision_path, index=False, encoding="utf-8-sig")
    ledger_df.to_csv(action_path, index=False, encoding="utf-8-sig")

    lines = [
        f"# {mode} Trade Ledger",
        "",
        "| 日期 | 买入信号日期 | 策略(B1 B2 B3) | BUY | 标的 | 股票代码 | 标的名称 | 买入价格 | 卖出价格 | 买入评分 | 卖出评分 | BUY股数 | BUY金额 | SELL日期 | 卖出原因 | 这个标的这次操作的盈亏金额 | 收益率 |",
        "| --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: |",
    ]
    for item in ledger_df.to_dict(orient="records"):
        lines.append(
            f"| {item.get('日期') or '-'} | {item.get('买入信号日期') or '-'} | {item.get('策略(B1 B2 B3)') or '-'} | {item.get('BUY') or '-'} | {item.get('标的') or '-'} | {item.get('股票代码') or '-'} | {item.get('标的名称') or '-'} | {item.get('买入价格') if pd.notna(item.get('买入价格')) else '-'} | {item.get('卖出价格') if pd.notna(item.get('卖出价格')) else '-'} | {item.get('买入评分') if pd.notna(item.get('买入评分')) else '-'} | {item.get('卖出评分') if pd.notna(item.get('卖出评分')) else '-'} | {item.get('BUY股数') if pd.notna(item.get('BUY股数')) else '-'} | {item.get('BUY金额') if pd.notna(item.get('BUY金额')) else '-'} | {item.get('SELL日期') or '-'} | {item.get('卖出原因') or '-'} | {item.get('这个标的这次操作的盈亏金额') if pd.notna(item.get('这个标的这次操作的盈亏金额')) else '-'} | {item.get('收益率') if pd.notna(item.get('收益率')) else '-'} |"
        )
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {
        "daily_decision_path": str(decision_path),
        "daily_action_path": str(action_path),
        "daily_markdown_path": str(markdown_path),
    }


def write_comparison_outputs(report_map: dict[str, pd.DataFrame], account: float, output_dir: Path) -> dict[str, str]:
    comparison = build_comparison_frame(report_map, account)
    csv_path = output_dir / "equity_comparison.csv"
    png_path = output_dir / "equity_comparison.png"
    html_path = output_dir / "equity_comparison.html"
    comparison.to_csv(csv_path, index=False, encoding="utf-8-sig")

    import os

    mpl_dir = ROOT / 'runtime' / 'matplotlib'
    mpl_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault('MPLCONFIGDIR', str(mpl_dir))

    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(12, 6), dpi=150)
    for series_name, frame in comparison.groupby("series", sort=False):
        ax.plot(pd.to_datetime(frame["datetime"]), frame["equity"], linewidth=2.0, label=series_name)
    ax.set_title("User Pattern Strategy Equity Comparison")
    ax.set_xlabel("Date")
    ax.set_ylabel("Equity")
    ax.grid(alpha=0.2)
    ax.legend()
    fig.tight_layout()
    fig.savefig(png_path)
    plt.close(fig)

    try:
        import plotly.express as px

        chart = px.line(comparison, x="datetime", y="equity", color="series", title="User Pattern Strategy Equity Comparison")
        chart.update_layout(template="plotly_white", legend_title_text="Series")
        chart.write_html(html_path)
    except Exception:
        html_path = Path("")

    return {
        "comparison_csv": str(csv_path),
        "comparison_png": str(png_path),
        "comparison_html": str(html_path) if str(html_path) else "",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run user pattern strategy backtests")
    parser.add_argument("--config", required=True)
    parser.add_argument("--strategy-file", default=str(ROOT / "strategy" / "strategy.py"))
    parser.add_argument("--provider-strategy", default=str(ROOT / "configs" / "strategy" / "first_alpha_v1.yaml"))
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--benchmark", default="SH000300")
    parser.add_argument("--account", type=float, default=500000)
    parser.add_argument("--deal-price", default="open")
    parser.add_argument("--buy-price-field", default="")
    parser.add_argument("--sell-price-field", default="")
    parser.add_argument("--slippage-rate", type=float, default=0.005)
    parser.add_argument("--max-holdings", type=int, default=10)
    parser.add_argument("--risk-degree", type=float, default=0.95)
    parser.add_argument("--max-holding-days", type=int, default=15)
    parser.add_argument("--min-position-ratio", type=float, default=0.06)
    parser.add_argument("--swap-score-gap", type=float, default=15.0)
    parser.add_argument("--modes", nargs="+", default=["B1", "B2", "B3"])
    parser.add_argument("--output-dir", default=str(ROOT / "data" / "reports" / "user_pattern_backtests"))
    args = parser.parse_args()

    provider_uri, app_settings = ensure_provider(args.config, args.provider_strategy)
    user_module = load_user_module(Path(args.strategy_file))
    configure_user_module(user_module, app_settings)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    summaries: list[dict[str, object]] = []
    report_map: dict[str, pd.DataFrame] = {}
    buy_price_field = args.buy_price_field or args.deal_price
    sell_price_field = args.sell_price_field or args.deal_price
    for mode in args.modes:
        result = simulate_pattern_backtest(
            user_module=user_module,
            app_settings=app_settings,
            provider_uri=provider_uri,
            mode=mode,
            start_time=args.start,
            end_time=args.end,
            account=args.account,
            max_holdings=args.max_holdings,
            risk_degree=args.risk_degree,
            max_holding_days=args.max_holding_days,
            buy_price_field=buy_price_field,
            sell_price_field=sell_price_field,
            open_cost=0.0005,
            close_cost=0.0015,
            min_cost=5,
            slippage_rate=args.slippage_rate,
            min_position_ratio=args.min_position_ratio,
            min_swap_score_gap=args.swap_score_gap,
        )
        report_df = result["report"].sort_index().copy()
        report_path = output_dir / f"{mode.lower()}_report.csv"
        risk_path = output_dir / f"{mode.lower()}_risk.csv"
        report_df.to_csv(report_path, encoding="utf-8-sig")
        risk_payload = result["risk"]
        if hasattr(risk_payload, "to_csv"):
            risk_payload.to_csv(risk_path, encoding="utf-8-sig")
        report_map[mode] = report_df
        summary = summarize_result(mode, report_df, args.account)
        summary["report_path"] = str(report_path)
        summary["risk_path"] = str(risk_path)
        summary.update(
            write_trade_outputs(
                output_dir=output_dir,
                mode=mode,
                decision_df=result["decisions"],
                ledger_df=result["ledger"],
            )
        )
        summaries.append(summary)

    comparison_paths = write_comparison_outputs(report_map, args.account, output_dir)
    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summaries, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# User Pattern Strategy Backtests",
        "",
        f"- account: {args.account}",
        f"- start: {args.start}",
        f"- end: {args.end}",
        f"- benchmark: {args.benchmark}",
        "",
        "| mode | total_return | annualized_return | max_drawdown | turnover | ending_equity | start_date | end_date | trading_days |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: |",
    ]
    for item in summaries:
        lines.append(
            f"| {item['mode']} | {item['total_return']:.6f} | {item['annualized_return']:.6f} | {item['max_drawdown']:.6f} | {item['turnover']:.6f} | {item['ending_equity']:.2f} | {item['start_date']} | {item['end_date']} | {item['trading_days']} |"
        )
    markdown_path = output_dir / "summary.md"
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(json.dumps({"provider_uri": str(provider_uri), "summary_path": str(summary_path), "markdown_path": str(markdown_path), "results": summaries, **comparison_paths}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
