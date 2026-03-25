from __future__ import annotations

from pathlib import Path

import pandas as pd

from quant_demo.api import dashboard_payloads as dp
from quant_demo.core.config import AppSettings


UI_LOG = "\u0055\u0049 \u65e5\u5fd7"
COL_DATE = "\u65e5\u671f"
COL_SYMBOL = "\u80a1\u7968\u4ee3\u7801"
COL_REASON = "\u5356\u51fa\u539f\u56e0"
COL_SOURCE = "\u6570\u636e\u6765\u6e90"
COL_MODE = "\u7b56\u7565(B1 B2 B3)"
LABEL_B2 = "\u5f62\u6001\u7b56\u7565 B2"


def _make_settings(tmp_path: Path) -> AppSettings:
    return AppSettings.model_validate(
        {
            'app_name': 'demo',
            'environment': 'backtest',
            'database_url': 'sqlite:///demo.db',
            'history_parquet': 'data/parquet/history.parquet',
            'report_dir': 'data/reports',
            'qmt_install_dir': 'runtime/qmt_client/installed',
            'qmt_download_url': 'https://example.com/qmt.zip',
            'default_strategy': 'first_alpha_v1',
            'symbols': [],
            'risk': {
                'max_position_ratio': 0.35,
                'daily_loss_limit': -0.04,
                'trading_start': '09:35',
                'trading_end': '14:55',
            },
        }
    )


def _make_dashboard_data() -> dp.DashboardData:
    assets = pd.DataFrame(
        [
            {
                'account_id': 'demo',
                'snapshot_time': pd.Timestamp('2026-03-24'),
                'total_asset': 100000.0,
                'cash': 40000.0,
                'frozen_cash': 0.0,
                'total_pnl': 1200.0,
                'turnover': 11.5,
                'max_drawdown': -0.08,
            }
        ]
    )
    positions = pd.DataFrame(
        [
            {
                'symbol': '000001.SZ',
                'qty': 1000,
                'available_qty': 1000,
                'cost_price': 10.0,
                'market_price': 12.0,
                'market_value': 12000.0,
                'unrealized_pnl': 2000.0,
                'snapshot_time': pd.Timestamp('2026-03-24'),
            }
        ]
    )
    orders = pd.DataFrame([{'order_id': 1, 'symbol': '000001.SZ', 'side': 'buy'}])
    trades = pd.DataFrame([{'trade_id': 1, 'symbol': '000001.SZ', 'side': 'buy'}])
    risk = pd.DataFrame([{'risk_decision_id': 1, 'status': 'approved', 'decided_at': pd.Timestamp('2026-03-24')}])
    audit = pd.DataFrame([{'audit_log_id': 1, 'message': 'ok', 'created_at': pd.Timestamp('2026-03-24')}])
    rules = pd.DataFrame([{'rule_name': 'max_position', 'passed': True}])
    benchmark = pd.DataFrame([{'trading_date': pd.Timestamp('2026-03-24'), 'benchmark_equity': 101000.0}])
    return dp.DashboardData(
        assets=assets,
        positions=positions,
        orders=orders,
        trades=trades,
        risk=risk,
        audit=audit,
        rules=rules,
        report_text='report',
        benchmark_curve=benchmark,
    )


def test_build_dashboard_payload_collects_expected_sections(monkeypatch, tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    config_path = tmp_path / 'app.yaml'
    pattern_base = tmp_path / 'mock-pattern'
    pattern_base.mkdir()
    (pattern_base / 'equity_comparison.png').write_bytes(b'png')
    (pattern_base / 'equity_comparison.html').write_text('<html></html>', encoding='utf-8')

    report_path = pattern_base / 'b1_report.csv'
    report_path.write_text(
        'datetime,account,total_turnover,value,cash,bench\n2026-03-24,123456.78,9.5,34567.89,88888.89,0.01\n',
        encoding='utf-8-sig',
    )
    risk_path = pattern_base / 'b1_risk.csv'
    risk_path.write_text(',risk\nmean,0.01\nmax_drawdown,-0.12\n', encoding='utf-8-sig')

    monkeypatch.setattr(dp, 'resolve_settings', lambda profile='backtest', config_path=None: ('backtest', config_path or tmp_path / 'app.yaml', settings))
    monkeypatch.setattr(dp, 'load_dashboard_data', lambda database_url, report_dir: _make_dashboard_data())
    monkeypatch.setattr(dp, 'load_runtime_logs', lambda: {UI_LOG: 'ready'})
    monkeypatch.setattr(dp, 'load_live_probe', lambda *_args, **_kwargs: {})
    monkeypatch.setattr(dp, 'history_status', lambda _settings: {'latest_trading_date': '2026-03-24', 'row_count': 10, 'symbol_count': 2})
    monkeypatch.setattr(dp, 'workspace_relative', lambda path: Path(path).name)
    monkeypatch.setattr(
        dp,
        'load_user_pattern_results',
        lambda _report_dir=dp.USER_PATTERN_REPORT_DIR: {
            'summary': pd.DataFrame([
                {
                    'mode': 'B1',
                    'total_return': 0.12,
                    'ending_equity': 123456.78,
                    'max_drawdown': -0.12,
                    'start_date': '2026-01-01',
                    'end_date': '2026-03-24',
                    'report_path': str(report_path),
                    'risk_path': str(risk_path),
                }
            ]),
            'comparison': pd.DataFrame(
                [
                    {'series': 'B1', 'datetime': pd.Timestamp('2026-03-24'), 'equity': 123456.78},
                    {'series': 'Benchmark', 'datetime': pd.Timestamp('2026-03-24'), 'equity': 120000.0},
                    {'series': 'B2', 'datetime': pd.Timestamp('2026-03-24'), 'equity': 110000.0},
                ]
            ),
            'daily_actions': pd.DataFrame(
                [
                    {COL_DATE: pd.Timestamp('2026-03-24'), COL_MODE: 'B1', COL_SYMBOL: '000001.SZ', COL_REASON: 'b1_st_stop'},
                    {COL_DATE: pd.Timestamp('2026-03-24'), COL_MODE: 'B2', COL_SYMBOL: '000002.SZ', COL_REASON: 'b2_timeout'},
                ]
            ),
            'daily_decisions': pd.DataFrame(
                [
                    {'trading_date': pd.Timestamp('2026-03-24'), 'mode': 'B1', 'buy_count': 1},
                    {'trading_date': pd.Timestamp('2026-03-24'), 'mode': 'B2', 'buy_count': 2},
                ]
            ),
            'png_path': str(pattern_base / 'equity_comparison.png'),
            'html_path': str(pattern_base / 'equity_comparison.html'),
            'base_dir': str(pattern_base),
        },
    )

    payload = dp.build_dashboard_payload('backtest', str(config_path))

    assert payload['profile'] == 'backtest'
    assert payload['overview']['total_asset'] == 123456.78
    assert payload['overview']['cash'] == 88888.89
    assert payload['overview']['market_value'] == 34567.89
    assert payload['overview']['trade_count'] == 1
    assert payload['data']['positions'] == []
    assert payload['data']['pattern_actions'][0][COL_SYMBOL] == '000001.SZ'
    assert len(payload['data']['pattern_actions']) == 1
    assert payload['pattern']['daily_decisions'][0]['mode'] == 'B1'
    assert payload['pattern']['summary'][0]['mode'] == 'B1'
    assert [item['series'] for item in payload['pattern']['comparison']] == ['B1', 'Benchmark']
    assert payload['data']['report_text'].startswith('# \u5f62\u6001\u7b56\u7565\u56de\u6d4b\u62a5\u544a')
    assert payload['connection']['label'] == 'B1 \u56de\u6d4b\u89c6\u56fe'
    assert payload['qlib']['status']['latest_trading_date'] == '2026-03-24'
    assert payload['logs'][UI_LOG] == 'ready'


def test_load_user_pattern_results_reads_daily_action_files(tmp_path: Path) -> None:
    summary_path = tmp_path / 'summary.json'
    action_path = tmp_path / 'b1_daily_actions.csv'
    decision_path = tmp_path / 'b1_daily_decisions.csv'
    summary_path.write_text(
        '[{"mode": "B1", "daily_action_path": "b1_daily_actions.csv", "daily_decision_path": "b1_daily_decisions.csv"}]',
        encoding='utf-8',
    )
    action_path.write_text(
        f'{COL_DATE},{COL_SYMBOL},{COL_REASON}\n2026-03-25,000002.SZ,b1_profit_take_10\n2026-03-24,000001.SZ,b1_st_stop\n',
        encoding='utf-8-sig',
    )
    decision_path.write_text(
        'trading_date,mode,buy_count\n2026-03-24,B1,1\n',
        encoding='utf-8-sig',
    )

    payload = dp.load_user_pattern_results(tmp_path)

    assert len(payload['daily_actions']) == 2
    assert payload['daily_actions'].iloc[0][COL_SYMBOL] == '000001.SZ'
    assert payload['daily_actions'].iloc[1][COL_SYMBOL] == '000002.SZ'
    assert payload['daily_actions'].iloc[0][COL_SOURCE] == 'b1_daily_actions.csv'
    assert len(payload['daily_decisions']) == 1
    assert payload['daily_decisions'].iloc[0]['mode'] == 'B1'


def test_run_pattern_action_builds_expected_modes(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(dp, '_read_yaml', lambda _path: {'app_name': 'demo', 'environment': 'backtest'})

    def fake_write_yaml(path: Path, payload: dict[str, object]) -> None:
        captured['config_path'] = path
        captured['config_payload'] = payload

    def fake_run_cmd(script_name: str, args: list[str]) -> dict[str, object]:
        captured['script_name'] = script_name
        captured['args'] = args
        return {'ok': True, 'stdout': 'done', 'stderr': '', 'returncode': 0}

    monkeypatch.setattr(dp, '_write_yaml', fake_write_yaml)
    monkeypatch.setattr(dp, 'run_cmd', fake_run_cmd)
    monkeypatch.setattr(dp, 'UI_RUNTIME', tmp_path)

    result = dp.run_pattern_action(
        {
            'selection_label': LABEL_B2,
            'start_date': '2026-01-01',
            'end_date': '2026-03-24',
            'account': 700000,
            'max_holdings': 7,
            'risk_degree': 0.9,
            'max_holding_days': 11,
        }
    )

    assert result['selection_label'] == LABEL_B2
    assert captured['script_name'] == 'scripts/run_user_pattern_backtests.py'
    assert '--modes' in captured['args']
    assert 'B2' in captured['args']
    assert captured['config_payload']['history_start'] == '20260101'
    assert captured['config_payload']['history_end'] == '20260324'
