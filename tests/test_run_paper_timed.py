from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from run_paper_timed import _resolve_receipt_path, _seconds_until


def test_seconds_until_returns_positive_delta() -> None:
    now = datetime(2026, 4, 15, 9, 0, 0)
    assert _seconds_until("09:35", now) == 35 * 60


def test_resolve_receipt_path_uses_signal_and_execution_dates() -> None:
    path = _resolve_receipt_path("data/reports/paper", "2026-04-14", "2026-04-15")
    assert path == Path("data/reports/paper/trade_plans/microcap_t1_execution_20260414_for_20260415.json")
