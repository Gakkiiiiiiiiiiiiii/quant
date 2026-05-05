from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from sqlalchemy import create_engine, text

from quant_demo.core.config import load_app_settings


def _path_status(path: Path) -> dict:
    return {
        "path": str(path),
        "exists": path.exists(),
        "is_dir": path.is_dir() if path.exists() else False,
        "is_file": path.is_file() if path.exists() else False,
    }


def _check_database(url: str) -> dict:
    try:
        engine = create_engine(url, future=True)
        with engine.connect() as conn:
            value = conn.execute(text("select 1")).scalar_one()
        return {"ok": value == 1, "error": ""}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _check_frontend(root: Path) -> dict:
    frontend_dir = root / "frontend" / "joinquant-vue"
    return {
        "frontend_dir": _path_status(frontend_dir),
        "package_json": _path_status(frontend_dir / "package.json"),
        "dist_dir": _path_status(frontend_dir / "dist"),
    }


def _check_qmt_settings(config_path: str) -> dict:
    settings = load_app_settings(config_path)
    install_dir = (ROOT / settings.qmt_install_dir).resolve()
    userdata_dir = Path(settings.qmt_userdata_dir)
    if not userdata_dir.is_absolute():
        userdata_dir = (ROOT / userdata_dir).resolve()
    return {
        "config_path": str(Path(config_path).resolve()),
        "environment": settings.environment.value,
        "database_url": settings.database_url,
        "database": _check_database(settings.database_url),
        "qmt_client_name": getattr(settings, "qmt_client_name", ""),
        "install_dir": _path_status(install_dir),
        "install_bin_x64": _path_status(install_dir / "bin.x64"),
        "userdata_dir": _path_status(userdata_dir),
        "bridge_python": _path_status((ROOT / settings.qmt_bridge_python).resolve()) if getattr(settings, "qmt_bridge_python", "") else {},
        "bridge_script": _path_status((ROOT / settings.qmt_bridge_script).resolve()) if getattr(settings, "qmt_bridge_script", "") else {},
        "trade_enabled": bool(getattr(settings, "qmt_trade_enabled", False)),
        "protected_sell_symbols": list(getattr(settings, "qmt_protected_sell_symbols", []) or []),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="迁移后环境自检")
    parser.add_argument("--app-config", default=str(ROOT / "configs" / "app.yaml"))
    parser.add_argument("--paper-config", default=str(ROOT / "configs" / "paper.yaml"))
    parser.add_argument("--live-config", default=str(ROOT / "configs" / "live.yaml"))
    args = parser.parse_args()

    required_paths = {
        "project_root": _path_status(ROOT),
        "src": _path_status(ROOT / "src"),
        "scripts": _path_status(ROOT / "scripts"),
        "configs": _path_status(ROOT / "configs"),
        "data": _path_status(ROOT / "data"),
        "runtime_qmt_installed": _path_status(ROOT / "runtime" / "qmt_client" / "installed"),
        "runtime_qmt_live_installed": _path_status(ROOT / "runtime" / "qmt_client" / "live_installed"),
        "requirements_qmt36": _path_status(ROOT / "requirements-qmt36.txt"),
    }

    payload = {
        "prepared_by": "Codex",
        "project_root": str(ROOT),
        "required_paths": required_paths,
        "frontend": _check_frontend(ROOT),
        "configs": {
            "app": _check_qmt_settings(args.app_config),
            "paper": _check_qmt_settings(args.paper_config),
            "live": _check_qmt_settings(args.live_config),
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
