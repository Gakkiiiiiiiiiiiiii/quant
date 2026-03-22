from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from quant_demo.core.config import load_app_settings


def main() -> None:
    parser = argparse.ArgumentParser(description="准备 QMT 独立安装目录")
    parser.add_argument("--config", default=str(ROOT / "configs" / "live.yaml"))
    parser.add_argument("--download", action="store_true", help="尝试下载 QMT 压缩包")
    args = parser.parse_args()

    settings = load_app_settings(args.config)
    install_root = ROOT / "runtime" / "qmt_client"
    download_dir = install_root / "downloads"
    client_dir = install_root / "client"
    download_dir.mkdir(parents=True, exist_ok=True)
    client_dir.mkdir(parents=True, exist_ok=True)

    archive_path = download_dir / Path(settings.qmt_download_url).name
    if args.download:
        print(f"下载 QMT 压缩包到: {archive_path}")
        urlretrieve(settings.qmt_download_url, archive_path)
        if archive_path.suffix.lower() == ".zip":
            with zipfile.ZipFile(archive_path) as zf:
                zf.extractall(client_dir)
            print(f"已解压到: {client_dir}")
        else:
            print("检测到非 zip 压缩包，请按券商提供方式手动解压安装。")
    else:
        print(f"QMT 独立安装目录已就绪: {client_dir}")
        print(f"如需下载，请执行: .\\.venv\\Scripts\\python.exe scripts\\bootstrap_qmt.py --download")


if __name__ == "__main__":
    main()
