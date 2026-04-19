from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BatchCityConfig:
    slug: str
    display_name: str
    gtfs_static_url: str
    osm_place_name: str


BATCH_CITY_CONFIGS: dict[str, BatchCityConfig] = {
    "chicago": BatchCityConfig(
        slug="chicago",
        display_name="Chicago",
        gtfs_static_url="https://www.transitchicago.com/downloads/sch_data/google_transit.zip",
        osm_place_name="Chicago, Illinois, USA",
    ),
    "boston": BatchCityConfig(
        slug="boston",
        display_name="Boston",
        gtfs_static_url="https://cdn.mbta.com/MBTA_GTFS.zip",
        osm_place_name="Boston, Massachusetts, USA",
    ),
}


def get_batch_city_config(city: str) -> BatchCityConfig:
    normalized = city.lower()
    if normalized not in BATCH_CITY_CONFIGS:
        raise KeyError(f"Unsupported batch city: {city}")
    return BATCH_CITY_CONFIGS[normalized]
