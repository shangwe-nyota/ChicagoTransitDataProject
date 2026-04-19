from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.run_metadata import StageTracker, generate_run_id


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full city-aware GTFS + OSM batch pipeline.")
    parser.add_argument("--city", required=True, choices=["chicago", "boston"])
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--load-snowflake", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    city = args.city.lower()
    run_id = args.run_id or generate_run_id(city)

    pipeline_tracker = StageTracker(
        stage="city_batch_pipeline",
        city=city,
        run_id=run_id,
        force=args.force,
    )
    command = " ".join(
        [
            sys.executable,
            "jobs/pipeline/run_city_batch_pipeline.py",
            "--city",
            city,
            "--run-id",
            run_id,
        ]
        + (["--force"] if args.force else [])
        + (["--load-snowflake"] if args.load_snowflake else [])
    )
    pipeline_tracker.mark_running(command=command)

    commands = [
        [sys.executable, "jobs/ingestion/download_gtfs.py", "--city", city, "--run-id", run_id],
        [sys.executable, "jobs/ingestion/download_osm.py", "--city", city, "--run-id", run_id],
        [sys.executable, "jobs/spark/clean_gtfs_city.py", "--city", city, "--run-id", run_id],
        [sys.executable, "jobs/spark/clean_osm_city.py", "--city", city, "--run-id", run_id],
        [sys.executable, "jobs/spark/build_city_batch_analytics.py", "--city", city, "--run-id", run_id],
    ]

    if args.force:
        for command_parts in commands:
            command_parts.append("--force")

    if args.load_snowflake:
        snowflake_command = [sys.executable, "-m", "jobs.load.load_to_snowflake", "--run-id", run_id]
        if args.force:
            snowflake_command.append("--force")
        commands.append(snowflake_command)

    try:
        for command_parts in commands:
            print(f"\n>>> Running: {' '.join(command_parts)}", flush=True)
            subprocess.run(command_parts, check=True, cwd=PROJECT_ROOT)

        pipeline_tracker.mark_success(
            command=command,
            metrics={
                "city": city,
                "stages_run": len(commands),
            },
        )
        print(f"\nCity batch pipeline completed successfully. run_id={run_id}", flush=True)
    except Exception as error:
        pipeline_tracker.mark_failed(command=command, error=error)
        raise


if __name__ == "__main__":
    main()
