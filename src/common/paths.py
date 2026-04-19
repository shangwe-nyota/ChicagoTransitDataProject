from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"


def raw_gtfs_dir(city: str) -> Path:
    return DATA_DIR / "raw" / "gtfs" / city


def raw_osm_dir(city: str) -> Path:
    return DATA_DIR / "raw" / "osm" / city


def city_processed_dir(city: str) -> Path:
    return DATA_DIR / "processed" / city


def staging_dir() -> Path:
    return DATA_DIR / "staging"


def run_metadata_dir() -> Path:
    return staging_dir() / "run_metadata"


def run_dir(run_id: str, city: str | None = None) -> Path:
    base = run_metadata_dir() / run_id
    if city is None:
        return base
    return base / city


def checkpoint_dir(city: str) -> Path:
    return staging_dir() / "checkpoints" / city


def clean_gtfs_dir(city: str, dataset: str) -> Path:
    return city_processed_dir(city) / "clean" / "gtfs" / dataset


def clean_osm_dir(city: str, dataset: str) -> Path:
    return city_processed_dir(city) / "clean" / "osm" / dataset


def analytics_dir(city: str, dataset: str) -> Path:
    return city_processed_dir(city) / "analytics" / dataset
