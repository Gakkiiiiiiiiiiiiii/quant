from __future__ import print_function

import argparse
import json
import os
import random
import sys
from pathlib import Path


def emit_ok(data):
    print(json.dumps({"ok": True, "data": data}, ensure_ascii=False))
    return 0


def emit_error(message):
    print(json.dumps({"ok": False, "error": message}, ensure_ascii=False))
    return 1


def dump_object(obj):
    data = {}
    for name in dir(obj):
        if name.startswith("_"):
            continue
        try:
            value = getattr(obj, name)
        except Exception:
            continue
        if callable(value):
            continue
        data[name] = value
    return data


def infer_userdata_dir(install_dir, explicit_userdata_dir):
    if explicit_userdata_dir:
        return Path(explicit_userdata_dir)
    mini_dir = Path(install_dir) / "userdata_mini"
    if mini_dir.exists():
        return mini_dir
    return Path(install_dir) / "userdata"


def split_symbols(raw_symbols):
    if not raw_symbols:
        return []
    return [item.strip() for item in raw_symbols.split(",") if item.strip()]


def parse_bool(raw_value):
    return str(raw_value).strip().lower() in {"1", "true", "yes", "y", "on"}


def bootstrap_runtime(install_dir):
    bin_dir = Path(install_dir) / "bin.x64"
    os.environ["PATH"] = str(bin_dir) + os.pathsep + os.environ.get("PATH", "")
    try:
        from xtquant import xtconstant, xtdata
        from xtquant.xttrader import XtQuantTrader
        from xtquant.xttype import StockAccount
    except Exception as exc:
        raise RuntimeError("xtquant 导入失败: {0}".format(exc))
    return xtconstant, xtdata, XtQuantTrader, StockAccount


def build_trader(XtQuantTrader, userdata_dir):
    trader = XtQuantTrader(str(userdata_dir), random.randint(100000, 999999))
    trader.start()
    result = trader.connect()
    return trader, result


def resolve_account_id(trader, explicit_account_id):
    infos = trader.query_account_infos() or []
    if explicit_account_id:
        return explicit_account_id, infos
    if infos:
        return getattr(infos[0], "account_id", ""), infos
    return "", infos


def command_health(args):
    xtconstant, xtdata, XtQuantTrader, StockAccount = bootstrap_runtime(args.install_dir)
    trader = None
    try:
        userdata_dir = infer_userdata_dir(args.install_dir, args.userdata_dir)
        trader, connect_result = build_trader(XtQuantTrader, userdata_dir)
        account_id, infos = resolve_account_id(trader, args.account_id)
        statuses = trader.query_account_status() or []
        sector_list = xtdata.get_sector_list() or []
        return emit_ok(
            {
                "install_dir": str(Path(args.install_dir)),
                "userdata_dir": str(userdata_dir),
                "connect_result": connect_result,
                "account_id": account_id,
                "account_infos": [dump_object(item) for item in infos],
                "account_status": [dump_object(item) for item in statuses],
                "sector_count": len(sector_list),
            }
        )
    except Exception as exc:
        return emit_error("health 检查失败: {0}".format(exc))
    finally:
        if trader is not None:
            trader.stop()


def command_quote(args):
    xtconstant, xtdata, XtQuantTrader, StockAccount = bootstrap_runtime(args.install_dir)
    try:
        quotes = xtdata.get_full_tick(split_symbols(args.symbols)) or {}
        normalized = {}
        for symbol, payload in quotes.items():
            normalized[symbol] = {
                "last_price": payload.get("lastPrice"),
                "open": payload.get("open"),
                "high": payload.get("high"),
                "low": payload.get("low"),
                "last_close": payload.get("lastClose"),
                "volume": payload.get("volume"),
                "amount": payload.get("amount"),
                "ask_price": payload.get("askPrice"),
                "bid_price": payload.get("bidPrice"),
            }
        return emit_ok({"quotes": normalized})
    except Exception as exc:
        return emit_error("quote 查询失败: {0}".format(exc))


def command_instrument_detail(args):
    xtconstant, xtdata, XtQuantTrader, StockAccount = bootstrap_runtime(args.install_dir)
    try:
        details = {}
        for symbol in split_symbols(args.symbols):
            details[symbol] = xtdata.get_instrument_detail(symbol)
        return emit_ok({"details": details})
    except Exception as exc:
        return emit_error("instrument-detail 查询失败: {0}".format(exc))


def command_sector_members(args):
    xtconstant, xtdata, XtQuantTrader, StockAccount = bootstrap_runtime(args.install_dir)
    try:
        xtdata.download_sector_data()
        symbols = xtdata.get_stock_list_in_sector(args.sector_name) or []
        if parse_bool(args.only_a_share):
            allowed_suffix = (".SH", ".SZ", ".BJ")
            symbols = [item for item in symbols if item.endswith(allowed_suffix)]
        if args.limit and args.limit > 0:
            symbols = symbols[: args.limit]
        return emit_ok(
            {
                "sector_name": args.sector_name,
                "symbol_count": len(symbols),
                "symbols": symbols,
            }
        )
    except Exception as exc:
        return emit_error("sector-members 查询失败: {0}".format(exc))


def _normalize_history_rows(pandas_module, symbol, frame):
    rows = []
    if frame is None or frame.empty:
        return rows
    data = frame.copy().reset_index(drop=True)
    if "time" in data.columns:
        trading_dates = (
            pandas_module.to_datetime(data["time"], unit="ms", utc=True)
            .dt.tz_convert("Asia/Shanghai")
            .dt.strftime("%Y-%m-%d")
        )
    else:
        trading_dates = pandas_module.to_datetime(data.index.astype(str), errors="coerce").strftime("%Y-%m-%d")
    data["trading_date"] = trading_dates
    for record in data.to_dict("records"):
        trading_date = pandas_module.to_datetime(record["trading_date"], errors="coerce")
        if pandas_module.isna(trading_date) or trading_date.dayofweek >= 5:
            continue
        rows.append(
            {
                "trading_date": trading_date.strftime("%Y-%m-%d"),
                "symbol": symbol,
                "open": float(record.get("open") or 0),
                "high": float(record.get("high") or 0),
                "low": float(record.get("low") or 0),
                "close": float(record.get("close") or 0),
                "volume": int(record.get("volume") or 0),
                "amount": float(record.get("amount") or 0),
            }
        )
    return rows


def command_history(args):
    xtconstant, xtdata, XtQuantTrader, StockAccount = bootstrap_runtime(args.install_dir)
    try:
        import pandas as pd
    except Exception as exc:
        return emit_error("history 查询失败: pandas 未安装 ({0})".format(exc))

    try:
        symbols = split_symbols(args.symbols)
        for symbol in symbols:
            xtdata.download_history_data(symbol, args.period, args.start_time, args.end_time)
        dataset = xtdata.get_market_data_ex(
            field_list=["time", "open", "high", "low", "close", "volume", "amount"],
            stock_list=symbols,
            period=args.period,
            start_time=args.start_time,
            end_time=args.end_time,
            dividend_type=args.dividend_type,
            fill_data=parse_bool(args.fill_data),
        ) or {}
        rows = []
        for symbol in symbols:
            rows.extend(_normalize_history_rows(pd, symbol, dataset.get(symbol)))
        rows.sort(key=lambda item: (item["trading_date"], item["symbol"]))
        return emit_ok(
            {
                "period": args.period,
                "dividend_type": args.dividend_type,
                "start_time": args.start_time,
                "end_time": args.end_time,
                "symbol_count": len(symbols),
                "row_count": len(rows),
                "rows": rows,
            }
        )
    except Exception as exc:
        return emit_error("history 查询失败: {0}".format(exc))


def command_financial_data(args):
    xtconstant, xtdata, XtQuantTrader, StockAccount = bootstrap_runtime(args.install_dir)
    try:
        import pandas as pd
    except Exception as exc:
        return emit_error("financial-data 查询失败: pandas 未安装 ({0})".format(exc))

    try:
        symbols = split_symbols(args.symbols)
        tables = split_symbols(args.tables)
        if not symbols or not tables:
            return emit_ok({"symbols": symbols, "tables": tables, "data": {}})

        xtdata.download_financial_data(symbols, tables, args.start_time, args.end_time)
        dataset = xtdata.get_financial_data(
            symbols,
            tables,
            args.start_time,
            args.end_time,
            args.report_type,
        ) or {}

        normalized = {}
        for symbol, table_map in dataset.items():
            normalized_tables = {}
            for table_name, frame in (table_map or {}).items():
                if frame is None or getattr(frame, "empty", True):
                    normalized_tables[table_name] = []
                    continue
                working = frame.copy()
                for column in working.columns:
                    if pd.api.types.is_datetime64_any_dtype(working[column]):
                        working[column] = working[column].dt.strftime("%Y-%m-%d")
                normalized_tables[table_name] = json.loads(working.to_json(orient="records", force_ascii=False))
            normalized[symbol] = normalized_tables

        return emit_ok(
            {
                "symbols": symbols,
                "tables": tables,
                "report_type": args.report_type,
                "data": normalized,
            }
        )
    except Exception as exc:
        return emit_error("financial-data 查询失败: {0}".format(exc))


def command_account(args):
    xtconstant, xtdata, XtQuantTrader, StockAccount = bootstrap_runtime(args.install_dir)
    trader = None
    try:
        userdata_dir = infer_userdata_dir(args.install_dir, args.userdata_dir)
        trader, connect_result = build_trader(XtQuantTrader, userdata_dir)
        account_id, infos = resolve_account_id(trader, args.account_id)
        if not account_id:
            raise RuntimeError("未发现可用证券账户")
        account = StockAccount(account_id)
        subscribe_result = trader.subscribe(account)
        asset = trader.query_stock_asset(account)
        positions = trader.query_stock_positions(account) or []
        orders = trader.query_stock_orders(account) or []
        trades = trader.query_stock_trades(account) or []
        statuses = trader.query_account_status() or []
        return emit_ok(
            {
                "connect_result": connect_result,
                "subscribe_result": subscribe_result,
                "account_id": account_id,
                "account_infos": [dump_object(item) for item in infos],
                "account_status": [dump_object(item) for item in statuses],
                "asset": dump_object(asset) if asset else None,
                "positions": [dump_object(item) for item in positions],
                "orders": [dump_object(item) for item in orders],
                "trades": [dump_object(item) for item in trades],
            }
        )
    except Exception as exc:
        return emit_error("account 查询失败: {0}".format(exc))
    finally:
        if trader is not None:
            trader.stop()


def command_order(args):
    xtconstant, xtdata, XtQuantTrader, StockAccount = bootstrap_runtime(args.install_dir)
    trader = None
    try:
        userdata_dir = infer_userdata_dir(args.install_dir, args.userdata_dir)
        trader, connect_result = build_trader(XtQuantTrader, userdata_dir)
        account_id, infos = resolve_account_id(trader, args.account_id)
        if not account_id:
            raise RuntimeError("未发现可用证券账户")
        account = StockAccount(account_id)
        subscribe_result = trader.subscribe(account)
        order_type = xtconstant.STOCK_BUY if args.side.lower() == "buy" else xtconstant.STOCK_SELL
        order_id = trader.order_stock(
            account,
            args.symbol,
            order_type,
            int(args.qty),
            xtconstant.FIX_PRICE,
            float(args.price),
            args.strategy_name,
            args.order_remark,
        )
        return emit_ok(
            {
                "connect_result": connect_result,
                "subscribe_result": subscribe_result,
                "account_id": account_id,
                "order_id": order_id,
            }
        )
    except Exception as exc:
        return emit_error("order 提交失败: {0}".format(exc))
    finally:
        if trader is not None:
            trader.stop()


def build_parser():
    parser = argparse.ArgumentParser(description="QMT Python 3.6 桥接脚本")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    def add_common(subparser):
        subparser.add_argument("--install-dir", required=True)
        subparser.add_argument("--userdata-dir", default="")
        subparser.add_argument("--account-id", default="")

    health_parser = subparsers.add_parser("health")
    add_common(health_parser)

    quote_parser = subparsers.add_parser("quote")
    add_common(quote_parser)
    quote_parser.add_argument("--symbols", required=True)

    detail_parser = subparsers.add_parser("instrument-detail")
    add_common(detail_parser)
    detail_parser.add_argument("--symbols", required=True)

    sector_parser = subparsers.add_parser("sector-members")
    add_common(sector_parser)
    sector_parser.add_argument("--sector-name", required=True)
    sector_parser.add_argument("--only-a-share", default="true")
    sector_parser.add_argument("--limit", type=int, default=0)

    history_parser = subparsers.add_parser("history")
    add_common(history_parser)
    history_parser.add_argument("--symbols", required=True)
    history_parser.add_argument("--period", default="1d")
    history_parser.add_argument("--start-time", default="")
    history_parser.add_argument("--end-time", default="")
    history_parser.add_argument("--dividend-type", default="front")
    history_parser.add_argument("--fill-data", default="true")

    financial_parser = subparsers.add_parser("financial-data")
    add_common(financial_parser)
    financial_parser.add_argument("--symbols", required=True)
    financial_parser.add_argument("--tables", required=True)
    financial_parser.add_argument("--start-time", default="")
    financial_parser.add_argument("--end-time", default="")
    financial_parser.add_argument("--report-type", default="announce_time")

    account_parser = subparsers.add_parser("account")
    add_common(account_parser)

    order_parser = subparsers.add_parser("order")
    add_common(order_parser)
    order_parser.add_argument("--symbol", required=True)
    order_parser.add_argument("--side", choices=["buy", "sell"], required=True)
    order_parser.add_argument("--qty", required=True, type=int)
    order_parser.add_argument("--price", required=True, type=float)
    order_parser.add_argument("--strategy-name", default="quant-demo-live")
    order_parser.add_argument("--order-remark", default="quant-demo")
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    command_map = {
        "health": command_health,
        "quote": command_quote,
        "instrument-detail": command_instrument_detail,
        "sector-members": command_sector_members,
        "history": command_history,
        "financial-data": command_financial_data,
        "account": command_account,
        "order": command_order,
    }
    return command_map[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
