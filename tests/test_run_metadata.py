from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.common.run_metadata import StageTracker, collect_path_stats, generate_run_id


class RunMetadataTests(unittest.TestCase):
    def test_generate_run_id_includes_city_prefix(self) -> None:
        run_id = generate_run_id("boston")
        self.assertTrue(run_id.startswith("boston-"))

    def test_collect_path_stats_handles_files_dirs_and_missing_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            nested_dir = tmp_dir / "nested"
            nested_dir.mkdir()
            file_path = nested_dir / "data.txt"
            file_path.write_text("hello")
            missing_path = tmp_dir / "missing.txt"

            stats = collect_path_stats([nested_dir, file_path, missing_path])

            self.assertEqual(stats[str(nested_dir)]["file_count"], 1)
            self.assertEqual(stats[str(file_path)]["total_bytes"], 5)
            self.assertFalse(stats[str(missing_path)]["exists"])

    def test_stage_tracker_success_creates_manifest_summary_and_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            base_dir = Path(tmp_dir_name)
            output_dir = base_dir / "outputs"
            output_dir.mkdir()
            (output_dir / "result.parquet").write_text("done")

            with (
                patch("src.common.run_metadata.run_dir", return_value=base_dir / "runs" / "run-1" / "boston"),
                patch("src.common.run_metadata.checkpoint_dir", return_value=base_dir / "checkpoints" / "boston"),
            ):
                tracker = StageTracker(stage="clean_gtfs_city", city="boston", run_id="run-1")
                tracker.mark_running(command="demo-command", output_paths=[output_dir])
                tracker.mark_success(
                    command="demo-command",
                    input_paths=[base_dir / "inputs"],
                    output_paths=[output_dir],
                    metrics={"row_counts": {"stops": 10}},
                )

                manifest = json.loads(tracker.manifest_path.read_text())
                summary = json.loads(tracker.summary_path.read_text())
                checkpoint = json.loads(tracker.latest_checkpoint_path.read_text())

                self.assertEqual(manifest["status"], "success")
                self.assertEqual(summary["stages"]["clean_gtfs_city"]["status"], "success")
                self.assertEqual(checkpoint["metrics"]["row_counts"]["stops"], 10)
                self.assertTrue(tracker.should_skip([output_dir]))

    def test_stage_tracker_does_not_skip_when_outputs_missing_or_force_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            base_dir = Path(tmp_dir_name)
            missing_output = base_dir / "missing_output"

            with (
                patch("src.common.run_metadata.run_dir", return_value=base_dir / "runs" / "run-2" / "chicago"),
                patch("src.common.run_metadata.checkpoint_dir", return_value=base_dir / "checkpoints" / "chicago"),
            ):
                tracker = StageTracker(stage="download_osm", city="chicago", run_id="run-2")
                tracker.mark_success(command="demo-command", output_paths=[missing_output])
                self.assertFalse(tracker.should_skip([missing_output]))

                forced_tracker = StageTracker(stage="download_osm", city="chicago", run_id="run-2", force=True)
                self.assertFalse(forced_tracker.should_skip([missing_output]))

    def test_mark_skipped_preserves_checkpoint_skip_eligibility_and_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            base_dir = Path(tmp_dir_name)
            output_dir = base_dir / "outputs"
            output_dir.mkdir()
            (output_dir / "result.txt").write_text("ok")

            with (
                patch("src.common.run_metadata.run_dir", return_value=base_dir / "runs" / "run-3" / "boston"),
                patch("src.common.run_metadata.checkpoint_dir", return_value=base_dir / "checkpoints" / "boston"),
            ):
                tracker = StageTracker(stage="download_gtfs", city="boston", run_id="run-3")
                tracker.mark_success(
                    command="demo-command",
                    output_paths=[output_dir],
                    metrics={"zip_bytes": 1234},
                )
                tracker.mark_skipped(command="demo-command", output_paths=[output_dir])

                manifest = json.loads(tracker.manifest_path.read_text())
                self.assertEqual(manifest["status"], "skipped")
                self.assertTrue(manifest["checkpoint_reused"])
                self.assertEqual(manifest["metrics"]["zip_bytes"], 1234)
                self.assertTrue(tracker.should_skip([output_dir]))


if __name__ == "__main__":
    unittest.main()
