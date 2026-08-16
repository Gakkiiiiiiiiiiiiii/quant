"""Contract Governance checksum（详细修改方案 §21）。

Quant 是 market-data.v1 / backtest.v1 / trading.v1 的唯一 Producer；
consumer 仓库不得自行修改同名契约，只能校验 checksum。

用法：
    python scripts/contract_checksums.py --write    # 生成 contracts/checksums/*.sha256
    python scripts/contract_checksums.py --verify   # 校验（consumer / CI 使用）
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_DIR = ROOT / "contracts"
CHECKSUM_DIR = CONTRACTS_DIR / "checksums"
CONTRACT_NAMES = ("market-data.v1", "backtest.v1", "trading.v1")


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_checksums() -> int:
    CHECKSUM_DIR.mkdir(parents=True, exist_ok=True)
    for name in CONTRACT_NAMES:
        source = CONTRACTS_DIR / f"{name}.yaml"
        if not source.exists():
            print(f"missing contract: {source}", file=sys.stderr)
            return 1
        (CHECKSUM_DIR / f"{name}.sha256").write_text(f"{sha256_of(source)}  {name}.yaml\n", encoding="utf-8")
        print(f"wrote contracts/checksums/{name}.sha256")
    return 0


def verify_checksums() -> int:
    failures = 0
    for name in CONTRACT_NAMES:
        source = CONTRACTS_DIR / f"{name}.yaml"
        record = CHECKSUM_DIR / f"{name}.sha256"
        if not record.exists():
            print(f"FAIL {name}: checksum file missing (run --write)", file=sys.stderr)
            failures += 1
            continue
        expected = record.read_text(encoding="utf-8").split()[0]
        actual = sha256_of(source) if source.exists() else None
        if actual != expected:
            print(f"FAIL {name}: contract drift (expected {expected[:12]}, got {(actual or 'missing')[:12]})", file=sys.stderr)
            failures += 1
        else:
            print(f"OK   {name}")
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="quant contract checksum governance")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--write", action="store_true", help="生成 checksum 文件")
    group.add_argument("--verify", action="store_true", help="校验 checksum")
    args = parser.parse_args()
    return write_checksums() if args.write else verify_checksums()


if __name__ == "__main__":
    raise SystemExit(main())
