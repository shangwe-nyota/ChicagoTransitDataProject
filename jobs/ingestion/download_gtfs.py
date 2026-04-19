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
from src.common.run_metadata import StageTracker, generate_run_id


LEGACY_OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "gtfs"


def download_gtfs(city: str | None = None, run_id: str | None = None, force: bool = False) -> None:
    tracker_city = city or "legacy_chicago"
    stage_name = "download_gtfs" if city else "download_gtfs_legacy"
    tracker = StageTracker(
        stage=stage_name,
        city=tracker_city,
        run_id=run_id or generate_run_id(tracker_city),
        force=force,
    )

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
    expected_outputs = [output_dir, zip_path]
    command = "python jobs/ingestion/download_gtfs.py" + (f" --city {city}" if city else "")

    if tracker.should_skip(expected_outputs):
        print(f"Skipping {stage_name} for {label}; checkpoint exists for run_id={tracker.run_id}")
        tracker.mark_skipped(command=command, output_paths=expected_outputs, metrics={"label": label})
        return

    tracker.mark_running(
        command=command,
        output_paths=expected_outputs,
        metrics={"label": label, "gtfs_url": gtfs_url},
    )

    try:
        print(f"Downloading GTFS data for {label}...")
        response = requests.get(gtfs_url, timeout=60)
        response.raise_for_status()

        zip_path.write_bytes(response.content)

        print("Download complete.")
        print("Unzipping GTFS data...")

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(output_dir)

        extracted_files = [path for path in output_dir.glob("*.txt")]
        tracker.mark_success(
            command=command,
            output_paths=[output_dir, zip_path, *extracted_files],
            metrics={
                "label": label,
                "gtfs_url": gtfs_url,
                "zip_bytes": zip_path.stat().st_size,
                "extracted_txt_count": len(extracted_files),
            },
        )
        print(f"Unzip complete. GTFS files written to {output_dir}")
    except Exception as error:
        tracker.mark_failed(
            command=command,
            error=error,
            output_paths=expected_outputs,
            metrics={"label": label, "gtfs_url": gtfs_url},
        )
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download GTFS static data.")
    parser.add_argument(
        "--city",
        choices=["chicago", "boston"],
        default=None,
        help="Use the new city-scoped batch path. Omit to preserve the legacy Chicago raw path.",
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    download_gtfs(args.city, run_id=args.run_id, force=args.force)
