from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class CityConfig:
    slug: str
    display_name: str
    latitude: float
    longitude: float
    zoom: float
    supports_live: bool


CITY_CONFIGS: dict[str, CityConfig] = {
    "boston": CityConfig(
        slug="boston",
        display_name="Boston",
        latitude=42.3601,
        longitude=-71.0589,
        zoom=11.2,
        supports_live=True,
    ),
    "chicago": CityConfig(
        slug="chicago",
        display_name="Chicago",
        latitude=41.8781,
        longitude=-87.6298,
        zoom=10.6,
        supports_live=True,  # enabled
    ),
}


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
LIVE_API_HOST = os.getenv("LIVE_API_HOST", "127.0.0.1")
LIVE_API_PORT = int(os.getenv("LIVE_API_PORT", "8000"))
LIVE_POLL_INTERVAL_SECONDS = float(os.getenv("LIVE_POLL_INTERVAL_SECONDS", "5"))
LIVE_POLL_INTERVAL_BOSTON_SECONDS = float(
    os.getenv("LIVE_POLL_INTERVAL_BOSTON_SECONDS", "3")
)
LIVE_POLL_INTERVAL_CHICAGO_SECONDS = float(
    os.getenv("LIVE_POLL_INTERVAL_CHICAGO_SECONDS", "4")
)
LIVE_VEHICLE_TTL_SECONDS = int(os.getenv("LIVE_VEHICLE_TTL_SECONDS", "180"))

# Boston MBTA
MBTA_API_BASE_URL = os.getenv("MBTA_API_BASE_URL", "https://api-v3.mbta.com")
MBTA_API_KEY = os.getenv("MBTA_API_KEY")

# Chicago CTA Bus Tracker
CTA_BUS_TRACKER_BASE_URL = os.getenv(
    "CTA_BUS_TRACKER_BASE_URL", "https://www.ctabustracker.com/bustime/api/v3"
)
CTA_BUS_TRACKER_API_KEY = os.getenv("CTA_BUS_TRACKER_API_KEY")

# Chicago CTA Train Tracker
CTA_TRAIN_TRACKER_BASE_URL = os.getenv(
    "CTA_TRAIN_TRACKER_BASE_URL", "https://lapi.transitchicago.com/api/1.0"
)
CTA_TRAIN_TRACKER_API_KEY = os.getenv("CTA_TRAIN_TRACKER_API_KEY")


def get_city_config(city: str) -> CityConfig:
    normalized = city.lower()
    if normalized not in CITY_CONFIGS:
        raise KeyError(f"Unsupported city: {city}")
    return CITY_CONFIGS[normalized]


def get_live_poll_interval_seconds(city: str | None = None) -> float:
    if city is None:
        return LIVE_POLL_INTERVAL_SECONDS

    normalized = city.lower()
    if normalized == "boston":
        return LIVE_POLL_INTERVAL_BOSTON_SECONDS
    if normalized == "chicago":
        return LIVE_POLL_INTERVAL_CHICAGO_SECONDS
    return LIVE_POLL_INTERVAL_SECONDS
