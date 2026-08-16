"""架构依赖规则测试（设计文档 §6.4）。

quant 必须保持基础设施独立性：禁止依赖 stock_agent / stock_factor / stock_content。
"""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "quant_demo"
FORBIDDEN_PREFIXES = ("stock_agent", "stock_factor", "stock_content")


def _iter_imports(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name
        elif isinstance(node, ast.ImportFrom) and node.module:
            yield node.module


def test_quant_does_not_import_other_repositories():
    violations: list[str] = []
    for path in SRC.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
        for module in _iter_imports(tree):
            if module.startswith(FORBIDDEN_PREFIXES):
                violations.append(f"{path.relative_to(ROOT)}: {module}")
    assert violations == [], f"quant 违反依赖规则（§6.4）: {violations}"


def test_no_cross_repo_imports_in_scripts_and_tests():
    for folder in ("scripts",):
        base = ROOT / folder
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
            for module in _iter_imports(tree):
                assert not module.startswith(FORBIDDEN_PREFIXES), f"{path} 违反依赖规则: {module}"
