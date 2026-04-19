from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

import httpx

from src.live.config import MBTA_API_BASE_URL, MBTA_API_KEY
from src.live.models import LiveVehicleState


class MbtaVehicleClient:
    def __init__(self, base_url: str = MBTA_API_BASE_URL, api_key: str | None = MBTA_API_KEY):
        headers = {"Accept": "application/json"}
        if api_key:
            headers["x-api-key"] = api_key

        self.client = httpx.AsyncClient(base_url=base_url, headers=headers, timeout=30.0)

    async def fetch_vehicle_positions(self) -> list[LiveVehicleState]:
        response = await self.client.get(
            "/vehicles",
            params={
                "include": "route,trip,stop",
                "page[limit]": 1000,
            },
        )
        response.raise_for_status()

        payload = response.json()
        included = {
            (item.get("type"), item.get("id")): item
            for item in payload.get("included", [])
        }

        vehicles: list[LiveVehicleState] = []
        for item in payload.get("data", []):
            vehicle = self._normalize_vehicle(item, included)
            if vehicle is not None:
                vehicles.append(vehicle)

        return vehicles

    async def close(self) -> None:
        await self.client.aclose()

    def _normalize_vehicle(self, item: dict, included: dict[tuple[str | None, str | None], dict]) -> LiveVehicleState | None:
        attributes = item.get("attributes", {})
        latitude = attributes.get("latitude")
        longitude = attributes.get("longitude")
        if latitude is None or longitude is None:
            return None

        relationships = item.get("relationships", {})
        route_id = self._relationship_id(relationships, "route")
        trip_id = self._relationship_id(relationships, "trip")
        stop_id = self._relationship_id(relationships, "stop")

        route = included.get(("route", route_id))
        route_label = self._route_label(route)
        route_type = None
        if route:
            route_type = route.get("attributes", {}).get("type")

        updated_at = self._parse_datetime(attributes.get("updated_at"))

        return LiveVehicleState(
            city="boston",
            vehicle_id=str(item.get("id")),
            route_id=route_id,
            route_label=route_label,
            trip_id=trip_id,
            stop_id=stop_id,
            label=attributes.get("label"),
            latitude=float(latitude),
            longitude=float(longitude),
            bearing=self._optional_float(attributes.get("bearing")),
            speed=self._optional_float(attributes.get("speed")),
            current_status=attributes.get("current_status"),
            occupancy_status=attributes.get("occupancy_status"),
            direction_id=attributes.get("direction_id"),
            route_type=route_type,
            updated_at=updated_at,
            feed_timestamp=updated_at,
            source="mbta_v3",
        )

    @staticmethod
    def _relationship_id(relationships: dict, key: str) -> str | None:
        data = relationships.get(key, {}).get("data")
        if not data:
            return None
        return data.get("id")

    @staticmethod
    def _route_label(route: dict | None) -> str | None:
        if route is None:
            return None

        attributes = route.get("attributes", {})
        short_name = attributes.get("short_name")
        long_name = attributes.get("long_name")
        if short_name and long_name:
            return f"{short_name} - {long_name}"
        return short_name or long_name

    @staticmethod
    def _optional_float(value) -> float | None:
        if value is None:
            return None
        return float(value)

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        if value is None:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

