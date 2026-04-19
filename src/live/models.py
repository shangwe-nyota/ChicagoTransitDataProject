from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class LiveVehicleState(BaseModel):
    city: str
    vehicle_id: str
    route_id: str | None = None
    route_label: str | None = None
    trip_id: str | None = None
    stop_id: str | None = None
    label: str | None = None
    latitude: float
    longitude: float
    bearing: float | None = None
    speed: float | None = None
    current_status: str | None = None
    occupancy_status: str | None = None
    direction_id: int | None = None
    route_type: int | None = None
    updated_at: datetime | None = None
    feed_timestamp: datetime | None = None
    source: str = "unknown"


class LiveVehiclesResponse(BaseModel):
    city: str
    vehicles: list[LiveVehicleState]
    vehicle_count: int = Field(..., ge=0)
    generated_at: datetime


class LiveCityResponse(BaseModel):
    slug: str
    display_name: str
    latitude: float
    longitude: float
    zoom: float
    supports_live: bool

