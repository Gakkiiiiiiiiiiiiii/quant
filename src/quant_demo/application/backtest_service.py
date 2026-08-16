"""异步回测任务服务（设计文档 §18 / §43 / §44）。

- Backtest 必须异步执行（QUEUED -> RUNNING -> COMPLETED/FAILED）
- Reproducibility Hash：相同输入直接复用已有回测
- Idempotency-Key：避免重试产生重复任务（§33）
"""
from __future__ import annotations

import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time as dtime, timedelta
from typing import Any

from sqlalchemy import select

from quant_demo.backtest.engine import BacktestExecutionConfig, EventDrivenBacktester
from quant_demo.backtest.execution_model import ExecutionModel
from quant_demo.backtest.metrics import compute_metrics
from quant_demo.backtest.target_portfolio import TargetPortfolio, TargetWeight
from quant_demo.backtest.time_contract import LookaheadViolation
from quant_demo.backtest.transaction_cost import TransactionCostModel
from quant_demo.core.error_codes import BACKTEST_FAILED, DATA_NOT_READY, LOOKAHEAD_VIOLATION, SNAPSHOT_NOT_FOUND
from quant_demo.db.models_market import BacktestJobRow, BacktestResultRow


class BacktestService:
    def __init__(self, session_factory, market_service, max_workers: int = 1) -> None:
        self._session_factory = session_factory
        self._market = market_service
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="backtest")
        self._progress: dict[str, tuple[int, str]] = {}
        self._lock = threading.Lock()

    # ---------------------------------------------------------------- create
    def create(self, payload: dict[str, Any], idempotency_key: str | None = None) -> dict[str, Any]:
        config_hash = reproducibility_hash(payload)
        with self._session_factory() as session:
            if idempotency_key:
                existing = session.scalar(
                    select(BacktestJobRow).where(BacktestJobRow.idempotency_key == idempotency_key)
                )
                if existing is not None:
                    return _job_payload(existing, reused=True, reason="IDEMPOTENT_REPLAY")
            completed = session.scalar(
                select(BacktestJobRow).where(
                    BacktestJobRow.config_hash == config_hash, BacktestJobRow.status == "COMPLETED"
                )
            )
            if completed is not None:
                return _job_payload(completed, reused=True, reason="REPRODUCIBILITY_HASH_HIT")
            strategy = payload.get("strategy") or {}
            job = BacktestJobRow(
                status="QUEUED",
                strategy_id=str(strategy.get("type", "target_portfolio")),
                strategy_version=str(strategy.get("version", "v1")),
                market_snapshot_id=payload.get("market_snapshot_id"),
                config_hash=config_hash,
                idempotency_key=idempotency_key,
                start_date=_parse_date(payload.get("start")),
                end_date=_parse_date(payload.get("end")),
                initial_cash=float(payload.get("initial_cash", 1_000_000)),
                benchmark=payload.get("benchmark"),
                request_payload=payload,
            )
            session.add(job)
            session.commit()
            backtest_id = job.backtest_id
            response = _job_payload(job)
        self._executor.submit(self._run, backtest_id)
        return response

    def get(self, backtest_id: str) -> dict[str, Any] | None:
        with self._session_factory() as session:
            job = session.get(BacktestJobRow, backtest_id)
            if job is None:
                return None
            payload = _job_payload(job)
        progress = self._progress.get(backtest_id)
        if progress is not None and payload["status"] == "RUNNING":
            payload["progress"], payload["phase"] = progress
        return payload

    def metrics(self, backtest_id: str) -> dict | None:
        return self._result_field(backtest_id, "metrics", include_flags=True)

    def equity(self, backtest_id: str) -> list | None:
        return self._result_field(backtest_id, "equity")

    def trades(self, backtest_id: str) -> list | None:
        return self._result_field(backtest_id, "trades")

    def positions(self, backtest_id: str) -> list | None:
        return self._result_field(backtest_id, "positions")

    def daily_actions(self, backtest_id: str) -> list | None:
        return self._result_field(backtest_id, "daily_actions")

    def diagnostics(self, backtest_id: str) -> dict | None:
        return self._result_field(backtest_id, "diagnostics")

    def _result_field(self, backtest_id: str, field: str, include_flags: bool = False):
        with self._session_factory() as session:
            row = session.get(BacktestResultRow, backtest_id)
            if row is None:
                return None
            if include_flags:
                return {"metrics": row.metrics, "quality_flags": row.quality_flags}
            return getattr(row, field)

    # ------------------------------------------------------------------- run
    def _run(self, backtest_id: str) -> None:
        with self._session_factory() as session:
            job = session.get(BacktestJobRow, backtest_id)
            if job is None:
                return
            job.status = "RUNNING"
            job.started_at = datetime.utcnow()
            job.phase = "loading"
            payload = dict(job.request_payload)
            session.commit()
        try:
            result = self._execute(payload, backtest_id)
        except LookaheadViolation as exc:
            self._fail(backtest_id, LOOKAHEAD_VIOLATION, str(exc))
            return
        except KeyError as exc:
            self._fail(backtest_id, SNAPSHOT_NOT_FOUND, f"missing input: {exc}")
            return
        except Exception as exc:  # noqa: BLE001
            self._fail(backtest_id, BACKTEST_FAILED, str(exc))
            return
        with self._session_factory() as session:
            session.add(
                BacktestResultRow(
                    backtest_id=backtest_id,
                    metrics=result["metrics"],
                    quality_flags=result["quality_flags"],
                    equity=result["equity"],
                    trades=result["trades"],
                    positions=result["positions"],
                    daily_actions=result["daily_actions"],
                    diagnostics=result["diagnostics"],
                )
            )
            job = session.get(BacktestJobRow, backtest_id)
            job.status = "COMPLETED"
            job.completed_at = datetime.utcnow()
            job.progress = 100
            job.phase = "completed"
            session.commit()
        self._progress.pop(backtest_id, None)

    def _fail(self, backtest_id: str, code: str, message: str) -> None:
        with self._session_factory() as session:
            job = session.get(BacktestJobRow, backtest_id)
            if job is None:
                return
            job.status = "FAILED"
            job.completed_at = datetime.utcnow()
            job.error_code = code
            job.error_message = message[:2000]
            job.phase = "failed"
            session.commit()
        self._progress.pop(backtest_id, None)

    def _execute(self, payload: dict[str, Any], backtest_id: str) -> dict[str, Any]:
        start = payload.get("start")
        end = payload.get("end")
        if not start or not end:
            raise KeyError("start/end")
        symbols = payload.get("symbols") or self._symbols_from_snapshot(payload.get("market_snapshot_id"))
        if not symbols:
            raise KeyError("symbols or market_snapshot_id")
        self._set_progress(backtest_id, 5, "loading")
        data = self._market.bars_batch(symbols=symbols, start=start, end=end, adjust=payload.get("adjust", "qfq"))
        dates = [date.fromisoformat(day) for day in data["dates"]]
        if not dates:
            raise RuntimeError(DATA_NOT_READY)
        trading_days = self._market.trading_days(dates[0], dates[-1])
        bars_by_day = _bars_by_day(data)

        status_map = self._market.status_map(symbols, dates[0], dates[-1])
        limit_map = self._market.limit_map(symbols, dates[0], dates[-1])

        execution = payload.get("execution_config") or {}
        config = BacktestExecutionConfig(
            execution_model=ExecutionModel(execution.get("execution_model", "next_open")),
            cost=TransactionCostModel(
                commission_rate=execution.get("commission_rate", 0.0003),
                min_commission=execution.get("min_commission", 5.0),
                stamp_duty_rate=execution.get("stamp_duty_rate", 0.001),
                slippage_bps=execution.get("slippage", 0.001) * 10_000.0
                if execution.get("slippage") is not None
                else 10.0,
            ),
            t1=bool(execution.get("t1", True)),
            price_limit=bool(execution.get("price_limit", True)),
            suspension=bool(execution.get("suspension", True)),
        )
        portfolios = self._build_portfolios(payload, trading_days)
        engine = EventDrivenBacktester(
            bars_by_day=bars_by_day,
            trading_days=[day for day in trading_days if dates[0] <= day <= dates[-1]],
            initial_cash=float(payload.get("initial_cash", 1_000_000)),
            config=config,
            status_map=status_map,
            limit_map=limit_map,
            prev_close_lookup=_prev_close_lookup(bars_by_day, dates),
        )

        def progress_cb(progress: int, phase: str) -> None:
            self._set_progress(backtest_id, 5 + int(progress * 0.9), phase)

        result = engine.run(portfolios, progress_cb=progress_cb)
        benchmark_equity = _benchmark_equity(payload.get("benchmark"), bars_by_day, dates, float(payload.get("initial_cash", 1_000_000)))
        metrics = compute_metrics(result.equity, result.trades, float(payload.get("initial_cash", 1_000_000)), benchmark_equity)
        quality_flags = list(result.quality_flags)
        if benchmark_equity is None and payload.get("benchmark"):
            quality_flags.append("BENCHMARK_DATA_MISSING")
        return {
            "metrics": metrics,
            "quality_flags": quality_flags,
            "equity": result.equity,
            "trades": result.trades,
            "positions": result.positions,
            "daily_actions": result.daily_actions,
            "diagnostics": {
                **result.diagnostics,
                "data_snapshot_id": data["data_snapshot_id"],
                "data_version": data["data_version"],
                "portfolio_count": len(portfolios),
            },
        }

    def _symbols_from_snapshot(self, snapshot_id: str | None) -> list[str]:
        if not snapshot_id:
            return []
        snapshot = self._market.get_snapshot(snapshot_id)
        if snapshot is None:
            return []
        return list(snapshot.get("payload_summary", {}).get("symbols", []))

    def _build_portfolios(self, payload: dict[str, Any], trading_days: list[date]) -> list[TargetPortfolio]:
        strategy = payload.get("strategy") or {}
        strategy_type = strategy.get("type", "target_portfolio")
        symbols = payload.get("symbols") or []
        portfolios: list[TargetPortfolio] = []
        if strategy_type == "target_portfolio" and strategy.get("rebalance_targets"):
            for item in strategy["rebalance_targets"]:
                signal_day = _parse_date(item["signal_date"])
                execution_day = _next_trading_day(trading_days, signal_day)
                if execution_day is None:
                    continue
                portfolios.append(
                    TargetPortfolio(
                        strategy_id=strategy.get("type", "target_portfolio"),
                        strategy_version=strategy.get("version", "v1"),
                        signal_time=datetime.combine(signal_day, dtime(15, 0)),
                        available_at=datetime.combine(signal_day, dtime(15, 5)),
                        executable_from=datetime.combine(execution_day, dtime(9, 30)),
                        targets=[TargetWeight(**target) for target in item["targets"]],
                    )
                )
            return portfolios
        # 默认：等权持有 + 周期再平衡（deterministic）
        rebalance_every = int(strategy.get("rebalance_every_days", 20) or 20)
        if not symbols:
            return []
        weight = round(min(0.95, 1.0) / len(symbols), 6)
        for index, day in enumerate(trading_days):
            if index % rebalance_every != 0:
                continue
            signal_day = _prev_trading_day(trading_days, day) or day
            execution_day = day if day > signal_day else _next_trading_day(trading_days, day)
            if execution_day is None or execution_day <= signal_day:
                continue
            portfolios.append(
                TargetPortfolio(
                    strategy_id=strategy_type,
                    strategy_version=strategy.get("version", "v1"),
                    signal_time=datetime.combine(signal_day, dtime(15, 0)),
                    available_at=datetime.combine(signal_day, dtime(15, 5)),
                    executable_from=datetime.combine(execution_day, dtime(9, 30)),
                    targets=[TargetWeight(symbol=symbol, target_weight=weight) for symbol in symbols],
                )
            )
        return portfolios

    def _set_progress(self, backtest_id: str, progress: int, phase: str) -> None:
        with self._lock:
            self._progress[backtest_id] = (min(progress, 99), phase)


def reproducibility_hash(payload: dict[str, Any]) -> str:
    """设计文档 §44：strategy+snapshot+execution_config+benchmark+start+end+initial_cash。"""
    material = {
        "strategy": payload.get("strategy"),
        "market_snapshot_id": payload.get("market_snapshot_id"),
        "execution_config": payload.get("execution_config"),
        "benchmark": payload.get("benchmark"),
        "start": payload.get("start"),
        "end": payload.get("end"),
        "initial_cash": payload.get("initial_cash"),
        "symbols": payload.get("symbols"),
    }
    return hashlib.sha256(
        json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def _job_payload(job: BacktestJobRow, reused: bool = False, reason: str | None = None) -> dict[str, Any]:
    payload = {
        "backtest_id": job.backtest_id,
        "status": job.status,
        "progress": job.progress,
        "phase": job.phase,
        "config_hash": job.config_hash,
        "strategy": {"type": job.strategy_id, "version": job.strategy_version},
        "market_snapshot_id": job.market_snapshot_id,
        "benchmark": job.benchmark,
        "created_at": job.created_at.isoformat(timespec="seconds"),
        "started_at": job.started_at.isoformat(timespec="seconds") if job.started_at else None,
        "completed_at": job.completed_at.isoformat(timespec="seconds") if job.completed_at else None,
        "error_code": job.error_code,
        "error_message": job.error_message,
    }
    if reused:
        payload["reused"] = True
        payload["reuse_reason"] = reason
    return payload


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _bars_by_day(data: dict) -> dict[date, dict[str, dict]]:
    symbols = data["symbols"]
    dates = data["dates"]
    bars = data["bars"]
    result: dict[date, dict[str, dict]] = {}
    for symbol_index, symbol in enumerate(symbols):
        for date_index, day in enumerate(dates):
            bar = {field: bars[field][symbol_index][date_index] for field in bars}
            if bar.get("close") is None:
                continue
            result.setdefault(date.fromisoformat(day), {})[symbol] = bar
    return result


def _prev_close_lookup(bars_by_day: dict[date, dict[str, dict]], dates: list[date]):
    ordered = sorted(bars_by_day)

    def lookup(symbol: str, day: date) -> float | None:
        previous = None
        for candidate in ordered:
            if candidate >= day:
                break
            bar = bars_by_day.get(candidate, {}).get(symbol)
            if bar and bar.get("close") is not None:
                previous = float(bar["close"])
        return previous

    return lookup


def _next_trading_day(trading_days: list[date], day: date) -> date | None:
    for candidate in trading_days:
        if candidate > day:
            return candidate
    return None


def _prev_trading_day(trading_days: list[date], day: date) -> date | None:
    previous = None
    for candidate in trading_days:
        if candidate >= day:
            break
        previous = candidate
    return previous


def _benchmark_equity(benchmark: str | None, bars_by_day: dict, dates: list[date], initial_cash: float) -> list[dict] | None:
    if not benchmark:
        return None
    series: list[dict] = []
    base_price = None
    for day in sorted(bars_by_day):
        bar = bars_by_day[day].get(benchmark)
        if bar is None or bar.get("close") is None:
            continue
        if base_price is None:
            base_price = float(bar["close"])
        series.append({"date": day.isoformat(), "total_asset": round(initial_cash * float(bar["close"]) / base_price, 2)})
    return series or None


__all__ = ["BacktestService", "reproducibility_hash"]
