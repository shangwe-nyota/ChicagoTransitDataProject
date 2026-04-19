from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.config import get_batch_city_config
from src.common.paths import raw_gtfs_dir


LEGACY_OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "gtfs"


def download_gtfs(city: str | None = None) -> None:
    if city is None:
        gtfs_url = "https://www.transitchicago.com/downloads/sch_data/google_transit.zip"
        output_dir = LEGACY_OUTPUT_DIR
        label = "Chicago (legacy path)"
    else:
        city_config = get_batch_city_config(city)
        gtfs_url = city_config.gtfs_static_url
        output_dir = raw_gtfs_dir(city)
        label = city_config.display_name

    output_dir.mkdir(parents=True, exist_ok=True)
    zip_path = output_dir / "gtfs.zip"

    print(f"Downloading GTFS data for {label}...")
    response = requests.get(gtfs_url, timeout=60)
    response.raise_for_status()

    zip_path.write_bytes(response.content)

    print("Download complete.")
    print("Unzipping GTFS data...")

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(output_dir)

    print(f"Unzip complete. GTFS files written to {output_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download GTFS static data.")
    parser.add_argument(
        "--city",
        choices=["chicago", "boston"],
        default=None,
        help="Use the new city-scoped batch path. Omit to preserve the legacy Chicago raw path.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    download_gtfs(args.city)
