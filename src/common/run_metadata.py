from __future__ import annotations

import json
import socket
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from src.common.paths import checkpoint_dir, run_dir


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_run_id(city: str | None = None) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    city_prefix = f"{city.lower()}-" if city else ""
    return f"{city_prefix}{timestamp}-{uuid4().hex[:8]}"


def _coerce_path_strings(paths: list[Path | str] | None) -> list[str]:
    if not paths:
        return []
    return [str(Path(path)) for path in paths]


def collect_path_stats(paths: list[Path | str] | None) -> dict[str, dict[str, int | bool | str]]:
    stats: dict[str, dict[str, int | bool | str]] = {}
    for raw_path in paths or []:
        path = Path(raw_path)
        if path.is_dir():
            files = [child for child in path.rglob("*") if child.is_file()]
            stats[str(path)] = {
                "exists": True,
                "is_dir": True,
                "file_count": len(files),
                "total_bytes": sum(child.stat().st_size for child in files),
            }
        elif path.is_file():
            stats[str(path)] = {
                "exists": True,
                "is_dir": False,
                "file_count": 1,
                "total_bytes": path.stat().st_size,
            }
        else:
            stats[str(path)] = {
                "exists": False,
                "is_dir": False,
                "file_count": 0,
                "total_bytes": 0,
            }
    return stats


@dataclass
class StageTracker:
    stage: str
    city: str
    run_id: str
    force: bool = False

    def __post_init__(self) -> None:
        self.started_at: str | None = None
        self.stage_dir = run_dir(self.run_id, self.city)
        self.stage_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path = self.stage_dir / f"{self.stage}.json"
        self.summary_path = self.stage_dir / "summary.json"
        self.latest_checkpoint_path = checkpoint_dir(self.city) / f"{self.stage}.json"
        self.latest_checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    def should_skip(self, expected_outputs: list[Path | str] | None = None) -> bool:
        if self.force or not self.manifest_path.exists():
            return False

        payload = json.loads(self.manifest_path.read_text())
        outputs_ok = all(Path(path).exists() for path in _coerce_path_strings(expected_outputs))
        return payload.get("status") in {"success", "skipped"} and outputs_ok

    def mark_running(
        self,
        *,
        command: str,
        input_paths: list[Path | str] | None = None,
        output_paths: list[Path | str] | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        self.started_at = utc_now_iso()
        payload = {
            "run_id": self.run_id,
            "city": self.city,
            "stage": self.stage,
            "status": "running",
            "started_at": self.started_at,
            "finished_at": None,
            "duration_seconds": None,
            "command": command,
            "host": socket.gethostname(),
            "input_paths": _coerce_path_strings(input_paths),
            "output_paths": _coerce_path_strings(output_paths),
            "input_path_stats": collect_path_stats(input_paths),
            "output_path_stats": collect_path_stats(output_paths),
            "metrics": metrics or {},
            "error_message": None,
        }
        self._write_manifest(payload)

    def mark_success(
        self,
        *,
        command: str,
        input_paths: list[Path | str] | None = None,
        output_paths: list[Path | str] | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        started_at = self.started_at or utc_now_iso()
        finished_at = utc_now_iso()
        payload = {
            "run_id": self.run_id,
            "city": self.city,
            "stage": self.stage,
            "status": "success",
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_seconds": _duration_seconds(started_at, finished_at),
            "command": command,
            "host": socket.gethostname(),
            "input_paths": _coerce_path_strings(input_paths),
            "output_paths": _coerce_path_strings(output_paths),
            "input_path_stats": collect_path_stats(input_paths),
            "output_path_stats": collect_path_stats(output_paths),
            "metrics": metrics or {},
            "error_message": None,
        }
        self._write_manifest(payload)
        self.latest_checkpoint_path.write_text(json.dumps(payload, indent=2, sort_keys=True))

    def mark_failed(
        self,
        *,
        command: str,
        error: Exception,
        input_paths: list[Path | str] | None = None,
        output_paths: list[Path | str] | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        started_at = self.started_at or utc_now_iso()
        finished_at = utc_now_iso()
        payload = {
            "run_id": self.run_id,
            "city": self.city,
            "stage": self.stage,
            "status": "failed",
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_seconds": _duration_seconds(started_at, finished_at),
            "command": command,
            "host": socket.gethostname(),
            "input_paths": _coerce_path_strings(input_paths),
            "output_paths": _coerce_path_strings(output_paths),
            "input_path_stats": collect_path_stats(input_paths),
            "output_path_stats": collect_path_stats(output_paths),
            "metrics": metrics or {},
            "error_message": str(error),
        }
        self._write_manifest(payload)

    def mark_skipped(
        self,
        *,
        command: str,
        input_paths: list[Path | str] | None = None,
        output_paths: list[Path | str] | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        previous_payload: dict[str, Any] = {}
        if self.manifest_path.exists():
            previous_payload = json.loads(self.manifest_path.read_text())

        payload = {
            "run_id": self.run_id,
            "city": self.city,
            "stage": self.stage,
            "status": "skipped",
            "started_at": utc_now_iso(),
            "finished_at": utc_now_iso(),
            "duration_seconds": 0.0,
            "command": command,
            "host": socket.gethostname(),
            "input_paths": _coerce_path_strings(input_paths),
            "output_paths": _coerce_path_strings(output_paths),
            "input_path_stats": collect_path_stats(input_paths),
            "output_path_stats": collect_path_stats(output_paths),
            "metrics": metrics or previous_payload.get("metrics", {}),
            "error_message": None,
            "checkpoint_reused": True,
        }
        self._write_manifest(payload)

    def _write_manifest(self, payload: dict[str, Any]) -> None:
        self.manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
        self._update_summary(payload)

    def _update_summary(self, payload: dict[str, Any]) -> None:
        if self.summary_path.exists():
            summary = json.loads(self.summary_path.read_text())
        else:
            summary = {
                "run_id": self.run_id,
                "city": self.city,
                "created_at": utc_now_iso(),
                "updated_at": utc_now_iso(),
                "stages": {},
            }

        summary["updated_at"] = utc_now_iso()
        summary["stages"][self.stage] = {
            "status": payload["status"],
            "manifest_path": str(self.manifest_path),
            "finished_at": payload["finished_at"],
            "metrics": payload.get("metrics", {}),
        }
        self.summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True))


def _duration_seconds(started_at: str, finished_at: str) -> float:
    started = datetime.fromisoformat(started_at)
    finished = datetime.fromisoformat(finished_at)
    return round((finished - started).total_seconds(), 3)
