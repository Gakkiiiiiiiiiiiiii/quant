"""Contract Governance（详细修改方案 §21）：checksum 生成与漂移检测。"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_DIR = ROOT / "contracts"
CHECKSUM_DIR = CONTRACTS_DIR / "checksums"


def _run_checksum_script(flag: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "contract_checksums.py"), flag],
        capture_output=True, text=True, check=False,
    )


def test_contract_checksums_verify_passes():
    result = _run_checksum_script("--verify")
    assert result.returncode == 0, result.stdout + result.stderr


def test_contract_checksum_files_cover_all_contracts():
    for name in ("market-data.v1", "backtest.v1", "trading.v1"):
        record = CHECKSUM_DIR / f"{name}.sha256"
        assert record.exists(), f"缺少 {record}"
        content = record.read_text(encoding="utf-8").split()
        assert len(content[0]) == 64  # sha256 hex


def test_verify_fails_when_contract_drifts():
    """临时修改 contract 内容 → verify 必须失败（Q-01）。"""
    contract = CONTRACTS_DIR / "market-data.v1.yaml"
    original = contract.read_bytes()
    try:
        contract.write_bytes(original + b"\n# drift-probe\n")
        result = _run_checksum_script("--verify")
        assert result.returncode != 0
        assert "market-data.v1" in result.stderr
    finally:
        contract.write_bytes(original)
    # 还原后必须恢复绿色
    assert _run_checksum_script("--verify").returncode == 0


def test_verify_fails_when_checksum_file_missing():
    """缺少 checksum 文件 → verify 必须失败（Q-01）。"""
    record = CHECKSUM_DIR / "trading.v1.sha256"
    original = record.read_bytes()
    try:
        record.unlink()
        result = _run_checksum_script("--verify")
        assert result.returncode != 0
        assert "trading.v1" in result.stderr
    finally:
        record.write_bytes(original)
    assert _run_checksum_script("--verify").returncode == 0
