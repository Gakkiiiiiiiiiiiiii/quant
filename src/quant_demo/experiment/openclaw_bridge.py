from __future__ import annotations

import json
from pathlib import Path


class OpenClawBridge:
    def export_task(self, output_path: str | Path, payload: dict) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path
