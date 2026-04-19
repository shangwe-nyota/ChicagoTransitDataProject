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


def clean_gtfs_dir(city: str, dataset: str) -> Path:
    return city_processed_dir(city) / "clean" / "gtfs" / dataset


def clean_osm_dir(city: str, dataset: str) -> Path:
    return city_processed_dir(city) / "clean" / "osm" / dataset


def analytics_dir(city: str, dataset: str) -> Path:
    return city_processed_dir(city) / "analytics" / dataset
