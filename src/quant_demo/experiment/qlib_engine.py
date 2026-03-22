from __future__ import annotations

import hashlib
import importlib.util
import json
import shutil
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import delete
from sqlalchemy.orm import sessionmaker

from quant_demo.adapters.qmt.gateway import create_gateway
from quant_demo.audit.report_service import AuditReportService
from quant_demo.core.config import AppSettings, StrategySettings
from quant_demo.db.models import (
    AssetSnapshotModel,
    AuditLogModel,
    OrderEventModel,
    OrderIntentModel,
    OrderModel,
    PositionSnapshotModel,
    RiskDecisionModel,
    TradeModel,
)
from quant_demo.experiment.evaluator import EvaluationResult, Evaluator

PROJECT_ROOT = Path(__file__).resolve().parents[3]


class QlibBacktestEngine:
    def __init__(self, session_factory: sessionmaker, app_settings: AppSettings, strategy_settings: StrategySettings) -> None:
        self.session_factory = session_factory
        self.app_settings = app_settings
        self.strategy_settings = strategy_settings
        self.account_id = "qlib-backtest"
        self.qlib_source_dir = self._resolve_path(app_settings.qlib_source_dir)
        self.qlib_provider_dir = self._resolve_path(app_settings.qlib_provider_dir)
        self.qlib_dataset_dir = self._resolve_path(app_settings.qlib_dataset_dir)
        self._ensure_qlib_importable()

    def run(self, initial_cash: Decimal) -> tuple[Path, EvaluationResult, pd.DataFrame]:
        history = self._load_history_with_benchmark()
        self._rebuild_provider(history)
        signal = self._build_signal_frame(history)
        report_normal, positions_normal = self._run_backtest(signal, float(initial_cash))
        return self._persist_results(report_normal, positions_normal, initial_cash)

    def _load_history_with_benchmark(self) -> pd.DataFrame:
        gateway = create_gateway(self.app_settings)
        quote_client = gateway.quote_client
        resolved_symbols = self._resolve_symbols(quote_client)
        benchmark_symbol = self.app_settings.qlib_benchmark_symbol.strip()
        history_symbols = resolved_symbols.copy()
        if benchmark_symbol and benchmark_symbol not in history_symbols:
            history_symbols.append(benchmark_symbol)
        history = quote_client.load_history(history_symbols, self.app_settings.history_parquet)
        allowed = set(history_symbols)
        frame = history[history["symbol"].isin(allowed)].copy()
        frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.date
        frame = frame.sort_values(["trading_date", "symbol"]).drop_duplicates(["trading_date", "symbol"], keep="last")
        return frame.reset_index(drop=True)

    def _resolve_symbols(self, quote_client: Any) -> list[str]:
        resolver = getattr(quote_client, "resolve_symbols", None)
        if callable(resolver):
            symbols = resolver(self.app_settings.symbols)
        else:
            symbols = [item for item in self.app_settings.symbols if item]
        if not symbols:
            raise ValueError("No symbols resolved for Qlib backtest")
        return sorted(dict.fromkeys(symbols))

    def _rebuild_provider(self, history: pd.DataFrame) -> None:
        source_file = self.qlib_dataset_dir / "qmt_history.parquet"
        symbol_dir = self.qlib_dataset_dir / "by_symbol"
        provider_meta = self.qlib_dataset_dir / "provider_meta.json"
        signature = self._provider_signature(history)
        if self.app_settings.qlib_force_rebuild:
            shutil.rmtree(self.qlib_provider_dir, ignore_errors=True)
            shutil.rmtree(symbol_dir, ignore_errors=True)
            provider_meta.unlink(missing_ok=True)
        if self._provider_cache_valid(provider_meta, signature):
            return
        self.qlib_dataset_dir.mkdir(parents=True, exist_ok=True)
        prepared = self._prepare_qlib_history(history)
        prepared.to_parquet(source_file, index=False)
        shutil.rmtree(symbol_dir, ignore_errors=True)
        symbol_dir.mkdir(parents=True, exist_ok=True)
        for instrument, frame in prepared.groupby("symbol", sort=False):
            frame.to_parquet(symbol_dir / f"{instrument}.parquet", index=False)
        shutil.rmtree(self.qlib_provider_dir, ignore_errors=True)
        self.qlib_provider_dir.mkdir(parents=True, exist_ok=True)
        dump_module = self._load_dump_module()
        dumper = dump_module.DumpDataAll(
            data_path=str(symbol_dir),
            qlib_dir=str(self.qlib_provider_dir),
            freq="day",
            max_workers=1,
            date_field_name="date",
            file_suffix=".parquet",
            symbol_field_name="symbol",
            exclude_fields="symbol",
        )
        self._serial_dump_provider(dumper)
        provider_meta.write_text(json.dumps(signature, ensure_ascii=False, indent=2), encoding="utf-8")

    def _prepare_qlib_history(self, history: pd.DataFrame) -> pd.DataFrame:
        prepared = history.copy()
        prepared["date"] = pd.to_datetime(prepared["trading_date"])
        prepared["symbol"] = prepared["symbol"].map(self._to_qlib_symbol)
        prepared = prepared.sort_values(["symbol", "date"]).reset_index(drop=True)
        prepared["factor"] = 1.0
        prepared["change"] = prepared.groupby("symbol", sort=False)["close"].pct_change(fill_method=None).fillna(0.0)
        if "amount" not in prepared.columns:
            prepared["amount"] = prepared["close"] * prepared["volume"]
        columns = ["symbol", "date", "open", "high", "low", "close", "volume", "amount", "factor", "change"]
        return prepared.loc[:, columns]

    def _build_signal_frame(self, history: pd.DataFrame) -> pd.DataFrame:
        benchmark_symbol = self.app_settings.qlib_benchmark_symbol.strip()
        working = history.copy()
        working = working[working["symbol"] != benchmark_symbol].copy()
        working["datetime"] = pd.to_datetime(working["trading_date"])
        working["instrument"] = working["symbol"].map(self._to_qlib_symbol)
        working = working.sort_values(["symbol", "datetime"]).reset_index(drop=True)
        grouped = working.groupby("symbol", sort=False)
        implementation = self.strategy_settings.implementation
        lookback = self.strategy_settings.lookback_days

        if implementation == "etf_rotation":
            score = grouped["close"].transform(lambda series: series / series.shift(lookback) - 1)
            score = score.where(score > 0)
        elif implementation == "stock_ranking":
            momentum = grouped["close"].transform(lambda series: series / series.shift(max(lookback - 1, 1)) - 1)
            liquidity = grouped["volume"].transform(lambda series: series.rolling(lookback, min_periods=lookback).mean()) / 1_000_000
            score = momentum * 0.7 + liquidity * 0.3
        elif implementation == "first_alpha_v1":
            window_size = max(lookback, 6)
            ret_window = max(window_size - 1, 1)
            daily_return = grouped["close"].pct_change(fill_method=None)
            momentum = grouped["close"].transform(lambda series: series / series.shift(window_size - 1) - 1)
            win_rate = daily_return.groupby(working["symbol"], sort=False).transform(
                lambda series: series.gt(0).astype(float).rolling(ret_window, min_periods=ret_window).mean()
            )
            volatility = daily_return.groupby(working["symbol"], sort=False).transform(
                lambda series: series.rolling(ret_window, min_periods=ret_window).std()
            )
            liquidity = grouped["volume"].transform(
                lambda series: series.rolling(window_size, min_periods=window_size).mean()
            ) / 1_000_000
            score = momentum * 0.55 + win_rate * 0.25 - volatility.fillna(0.0) * 0.15 + liquidity * 0.05
            score = score.where((momentum > 0) & (score > 0))
        else:
            raise ValueError(f"Unsupported Qlib signal mapping for strategy: {implementation}")

        signal = working.loc[score.notna(), ["instrument", "datetime"]].copy()
        signal["score"] = score[score.notna()].astype(float)
        if signal.empty:
            raise ValueError("Qlib signal frame is empty")
        signal = signal.drop_duplicates(["instrument", "datetime"], keep="last")
        return signal.set_index(["instrument", "datetime"]).sort_index()

    def _run_backtest(self, signal: pd.DataFrame, initial_cash: float) -> tuple[pd.DataFrame, dict[Any, Any]]:
        import qlib
        from qlib.contrib.evaluate import backtest_daily
        from qlib.contrib.strategy import TopkDropoutStrategy

        qlib.init(
            provider_uri=str(self.qlib_provider_dir),
            region=self.app_settings.qlib_region,
            kernels=1,
            joblib_backend="threading",
            maxtasksperchild=1,
        )
        strategy = TopkDropoutStrategy(
            signal=signal,
            topk=self.strategy_settings.top_n,
            n_drop=max(1, self.app_settings.qlib_n_drop),
            only_tradable=True,
            forbid_all_trade_at_limit=True,
        )
        signal_dates = pd.Index(sorted(pd.to_datetime(signal.index.get_level_values("datetime")).unique()))
        end_time = signal_dates[-2] if len(signal_dates) > 1 else signal_dates[-1]
        report_normal, positions_normal = backtest_daily(
            start_time=signal_dates[0],
            end_time=end_time,
            strategy=strategy,
            account=initial_cash,
            benchmark=self.app_settings.qlib_benchmark,
            exchange_kwargs={
                "deal_price": "close",
                "limit_threshold": 0.095,
                "open_cost": 0.0005,
                "close_cost": 0.0015,
                "min_cost": 5,
            },
        )
        return report_normal, positions_normal

    def _persist_results(
        self,
        report_normal: pd.DataFrame,
        positions_normal: dict[Any, Any],
        initial_cash: Decimal,
    ) -> tuple[Path, EvaluationResult, pd.DataFrame]:
        ordered_report = report_normal.sort_index().copy()
        ordered_report.index = pd.to_datetime(ordered_report.index)
        return_series = ordered_report["return"].astype(float) if "return" in ordered_report else pd.Series(0.0, index=ordered_report.index)
        cost_series = ordered_report["cost"].astype(float) if "cost" in ordered_report else pd.Series(0.0, index=ordered_report.index)
        turnover_series = ordered_report["turnover"].astype(float) if "turnover" in ordered_report else pd.Series(0.0, index=ordered_report.index)
        net_return = return_series - cost_series
        cumulative_turnover = turnover_series.fillna(0.0).cumsum()
        equity = float(initial_cash) * (1.0 + net_return).cumprod()
        drawdown = equity / equity.cummax() - 1
        equity_curve = pd.DataFrame(
            {
                "trading_date": ordered_report.index.date,
                "equity": equity.astype(float),
                "turnover": cumulative_turnover.astype(float),
            }
        )

        with self.session_factory() as session:
            self._reset_non_live_state(session)
            for trade_time in ordered_report.index:
                snapshot_time = datetime.combine(trade_time.date(), datetime.min.time())
                total_asset = Decimal(str(equity.loc[trade_time]))
                position_payload = self._normalize_position_payload(
                    positions_normal.get(trade_time) or positions_normal.get(trade_time.to_pydatetime())
                )
                stock_value = Decimal("0")
                for symbol, payload in position_payload.items():
                    amount = int(round(float(payload.get("amount", 0.0))))
                    price = Decimal(str(payload.get("price", 0.0)))
                    if amount <= 0:
                        continue
                    stock_value += Decimal(amount) * price
                    session.add(
                        PositionSnapshotModel(
                            account_id=self.account_id,
                            symbol=self._from_qlib_symbol(symbol),
                            qty=amount,
                            available_qty=amount,
                            cost_price=price,
                            market_price=price,
                            snapshot_time=snapshot_time,
                        )
                    )
                cash_value = total_asset - stock_value
                session.add(
                    AssetSnapshotModel(
                        account_id=self.account_id,
                        cash=cash_value,
                        frozen_cash=Decimal("0"),
                        total_asset=total_asset,
                        total_pnl=total_asset - initial_cash,
                        turnover=Decimal(str(cumulative_turnover.loc[trade_time])),
                        max_drawdown=Decimal(str(drawdown.loc[trade_time])),
                        snapshot_time=snapshot_time,
                    )
                )
            session.add(
                AuditLogModel(
                    object_type="backtest_engine",
                    object_id="qlib",
                    message="Qlib backtest finished",
                    payload={
                        "strategy": self.strategy_settings.implementation,
                        "benchmark": self.app_settings.qlib_benchmark,
                        "engine": self.app_settings.backtest_engine,
                        "provider_dir": str(self.qlib_provider_dir),
                    },
                )
            )
            session.commit()
            report_path = AuditReportService().write_daily_report(session, self.app_settings.report_dir)
        metrics = Evaluator().evaluate(equity_curve)
        return report_path, metrics, equity_curve

    def _reset_non_live_state(self, session) -> None:
        for model in [
            AuditLogModel,
            TradeModel,
            OrderEventModel,
            OrderModel,
            RiskDecisionModel,
            OrderIntentModel,
            PositionSnapshotModel,
            AssetSnapshotModel,
        ]:
            session.execute(delete(model))
        session.flush()

    @staticmethod
    def _normalize_position_payload(position_obj: Any) -> dict[str, dict[str, Any]]:
        if position_obj is None:
            return {}
        payload = getattr(position_obj, "position", position_obj)
        if not isinstance(payload, dict):
            return {}
        return {key: value for key, value in payload.items() if isinstance(value, dict)}

    def _provider_signature(self, history: pd.DataFrame) -> dict[str, Any]:
        digest_source = history.loc[:, ["trading_date", "symbol", "close", "volume"]].copy()
        digest_source["trading_date"] = digest_source["trading_date"].astype(str)
        digest = hashlib.sha256(pd.util.hash_pandas_object(digest_source, index=False).values.tobytes()).hexdigest()
        return {
            "history_digest": digest,
            "row_count": len(history),
            "benchmark": self.app_settings.qlib_benchmark,
            "strategy": self.strategy_settings.implementation,
            "provider_dir": str(self.qlib_provider_dir),
        }

    def _provider_cache_valid(self, meta_path: Path, signature: dict[str, Any]) -> bool:
        if not meta_path.exists() or not self.qlib_provider_dir.exists():
            return False
        try:
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return False
        return all(payload.get(key) == value for key, value in signature.items())

    def _serial_dump_provider(self, dumper: Any) -> None:
        all_datetime = set()
        date_range_list: list[str] = []
        for file_path in dumper.df_files:
            (begin_time, end_time), calendar_set = dumper._get_date(file_path, as_set=True, is_begin_end=True)
            all_datetime |= calendar_set
            if pd.notna(begin_time) and pd.notna(end_time):
                symbol = dumper.get_symbol_from_file(file_path)
                fields = [
                    symbol.upper(),
                    dumper._format_datetime(begin_time),
                    dumper._format_datetime(end_time),
                ]
                date_range_list.append(dumper.INSTRUMENTS_SEP.join(fields))
        dumper._calendars_list = sorted(map(pd.Timestamp, all_datetime))
        dumper.save_calendars(dumper._calendars_list)
        dumper.save_instruments(date_range_list)
        for file_path in dumper.df_files:
            dumper._dump_bin(file_path, dumper._calendars_list)

    def _load_dump_module(self):
        dump_path = self.qlib_source_dir / "scripts" / "dump_bin.py"
        module_name = "quant_demo_qlib_dump_bin"
        spec = importlib.util.spec_from_file_location(module_name, dump_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Unable to load Qlib dump script: {dump_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def _ensure_qlib_importable(self) -> None:
        qlib_root = str(self.qlib_source_dir)
        if qlib_root not in sys.path:
            sys.path.insert(0, qlib_root)

    @staticmethod
    def _to_qlib_symbol(symbol: str) -> str:
        if "." not in symbol:
            return symbol.upper()
        code, market = symbol.split(".", 1)
        return f"{market.upper()}{code.upper()}"

    @staticmethod
    def _from_qlib_symbol(symbol: str) -> str:
        normalized = symbol.upper()
        if len(normalized) < 3:
            return normalized
        market = normalized[:2]
        code = normalized[2:]
        if market in {"SH", "SZ", "BJ"}:
            return f"{code}.{market}"
        return normalized

    @staticmethod
    def _resolve_path(raw_path: str) -> Path:
        path = Path(raw_path)
        if path.is_absolute():
            return path
        return (PROJECT_ROOT / path).resolve()