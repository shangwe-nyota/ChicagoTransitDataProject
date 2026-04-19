from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx

from src.live.config import (
    CTA_BUS_TRACKER_API_KEY,
    CTA_BUS_TRACKER_BASE_URL,
    CTA_TRAIN_TRACKER_API_KEY,
    CTA_TRAIN_TRACKER_BASE_URL,
)
from src.live.models import LiveVehicleState

_CTA_LOCAL_TZ = ZoneInfo("America/Chicago")
_CTA_BUS_TIMESTAMP_FORMATS = ("%Y%m%d %H:%M:%S", "%Y%m%d %H:%M")
_CTA_TRAIN_TIMESTAMP_FORMATS = ("%Y-%m-%dT%H:%M:%S", "%Y%m%d %H:%M:%S")

CTA_L_ROUTES = ["red", "blue", "brn", "g", "org", "p", "pink", "y"]
CTA_MAX_BUS_ROUTES_PER_REQUEST = 10
CTA_ACTIVE_ROUTE_REFRESH_INTERVAL = timedelta(minutes=5)


class CtaBusClient:
    """
    Fetches live bus positions from the CTA Bus Tracker REST API v3.

    CTA's `getvehicles` endpoint does not support a global "all buses" query.
    It requires either a route list or vehicle IDs, so this client:

    1. fetches all route IDs via `getroutes`
    2. discovers which routes currently have active vehicles
    3. caches those active routes for a few minutes
    4. polls only active routes in batches of 10
    5. merges and deduplicates the resulting vehicles
    """

    def __init__(
        self,
        base_url: str = CTA_BUS_TRACKER_BASE_URL,
        api_key: str | None = CTA_BUS_TRACKER_API_KEY,
    ):
        if not api_key:
            raise ValueError(
                "CTA_BUS_TRACKER_API_KEY is required. "
                "Register at https://www.ctabustracker.com/home"
            )

        self._api_key = api_key
        self.client = httpx.AsyncClient(base_url=base_url, timeout=30.0)
        self._routes_by_id: dict[str, dict] | None = None
        self._active_route_ids: set[str] = set()
        self._active_routes_refreshed_at: datetime | None = None

    async def fetch_vehicle_positions(self) -> list[LiveVehicleState]:
        routes_by_id = await self._get_routes_by_id()
        route_ids = sorted(routes_by_id.keys())
        if not route_ids:
            return []

        if self._should_refresh_active_routes():
            vehicles, active_route_ids = await self._discover_active_routes(routes_by_id)
            self._active_route_ids = active_route_ids
            self._active_routes_refreshed_at = datetime.now(timezone.utc)
            return list(_dedupe_latest(vehicles).values())

        if not self._active_route_ids:
            return []

        route_batches = [
            sorted(self._active_route_ids)[index:index + CTA_MAX_BUS_ROUTES_PER_REQUEST]
            for index in range(0, len(self._active_route_ids), CTA_MAX_BUS_ROUTES_PER_REQUEST)
        ]

        batch_results = await asyncio.gather(
            *(self._fetch_vehicle_batch(batch) for batch in route_batches),
            return_exceptions=True,
        )

        vehicles: list[LiveVehicleState] = []
        for result in batch_results:
            if isinstance(result, Exception):
                print(f"[cta] bus batch fetch failed: {result}", flush=True)
                continue

            batch_vehicles, inactive_routes = result
            self._active_route_ids.difference_update(inactive_routes)
            for item in batch_vehicles:
                vehicle = self._normalize_bus(item, routes_by_id)
                if vehicle is not None:
                    vehicles.append(vehicle)

        return list(_dedupe_latest(vehicles).values())

    def _should_refresh_active_routes(self) -> bool:
        if self._active_routes_refreshed_at is None:
            return True
        return datetime.now(timezone.utc) - self._active_routes_refreshed_at >= CTA_ACTIVE_ROUTE_REFRESH_INTERVAL

    async def _discover_active_routes(
        self,
        routes_by_id: dict[str, dict],
    ) -> tuple[list[LiveVehicleState], set[str]]:
        semaphore = asyncio.Semaphore(12)

        async def probe(route_id: str):
            async with semaphore:
                vehicles, inactive_routes = await self._fetch_vehicle_batch([route_id])
                return route_id, vehicles, inactive_routes

        probe_results = await asyncio.gather(
            *(probe(route_id) for route_id in sorted(routes_by_id.keys())),
            return_exceptions=True,
        )

        vehicles: list[LiveVehicleState] = []
        active_route_ids: set[str] = set()

        for result in probe_results:
            if isinstance(result, Exception):
                print(f"[cta] bus route probe failed: {result}", flush=True)
                continue

            route_id, raw_vehicles, inactive_routes = result
            if route_id in inactive_routes or not raw_vehicles:
                continue

            active_route_ids.add(route_id)
            for item in raw_vehicles:
                vehicle = self._normalize_bus(item, routes_by_id)
                if vehicle is not None:
                    vehicles.append(vehicle)

        print(
            f"[cta] discovered {len(active_route_ids)} active bus routes",
            flush=True,
        )
        return vehicles, active_route_ids

    async def _get_routes_by_id(self) -> dict[str, dict]:
        if self._routes_by_id is not None:
            return self._routes_by_id

        response = await self.client.get(
            "/getroutes",
            params={"key": self._api_key, "format": "json"},
        )
        response.raise_for_status()

        payload = response.json()
        bustime = payload.get("bustime-response", {})
        _raise_bus_error_if_present(bustime)

        routes = bustime.get("routes", []) or []
        self._routes_by_id = {
            route.get("rt"): route
            for route in routes
            if route.get("rt")
        }
        return self._routes_by_id

    async def _fetch_vehicle_batch(self, route_batch: list[str]) -> tuple[list[dict], set[str]]:
        response = await self.client.get(
            "/getvehicles",
            params={
                "key": self._api_key,
                "format": "json",
                "tmres": "s",
                "rt": ",".join(route_batch),
            },
        )
        response.raise_for_status()

        payload = response.json()
        bustime = payload.get("bustime-response", {})
        return _parse_bus_vehicle_response(bustime)

    @staticmethod
    def _normalize_bus(item: dict, routes_by_id: dict[str, dict]) -> LiveVehicleState | None:
        try:
            latitude = float(item["lat"])
            longitude = float(item["lon"])
        except (KeyError, TypeError, ValueError):
            return None

        vehicle_id = str(item.get("vid", "")).strip()
        if not vehicle_id:
            return None

        route_id = item.get("rt") or None
        route_name = None
        if route_id:
            route_name = (routes_by_id.get(route_id) or {}).get("rtnm")

        destination = item.get("des") or None
        label_parts = [part for part in [route_id, route_name or destination] if part]
        route_label = " - ".join(label_parts) if label_parts else route_id

        current_status = None
        if str(item.get("dly", "")).lower() == "true":
            current_status = "STOPPED_AT"

        updated_at = _parse_local_timestamp(item.get("tmstmp"), _CTA_BUS_TIMESTAMP_FORMATS)

        return LiveVehicleState(
            city="chicago",
            vehicle_id=f"bus-{vehicle_id}",
            route_id=route_id,
            route_label=route_label,
            trip_id=item.get("tatripid") or None,
            stop_id=None,
            label=vehicle_id,
            latitude=latitude,
            longitude=longitude,
            bearing=_optional_float(item.get("hdg")),
            speed=_optional_float(item.get("spd")),
            current_status=current_status,
            occupancy_status=None,
            direction_id=None,
            route_type=3,
            updated_at=updated_at,
            feed_timestamp=updated_at,
            source="cta_bus_tracker",
        )

    async def close(self) -> None:
        await self.client.aclose()


class CtaTrainClient:
    """
    Fetches live L-train positions from the CTA Train Tracker locations endpoint.
    """

    def __init__(
        self,
        base_url: str = CTA_TRAIN_TRACKER_BASE_URL,
        api_key: str | None = CTA_TRAIN_TRACKER_API_KEY,
    ):
        if not api_key:
            raise ValueError(
                "CTA_TRAIN_TRACKER_API_KEY is required. "
                "Register at https://www.transitchicago.com/developers/traintracker.aspx"
            )

        self._api_key = api_key
        self.client = httpx.AsyncClient(base_url=base_url, timeout=30.0)

    async def fetch_vehicle_positions(self) -> list[LiveVehicleState]:
        response = await self.client.get(
            "/ttpositions.aspx",
            params={
                "key": self._api_key,
                "rt": ",".join(CTA_L_ROUTES),
                "outputType": "JSON",
            },
        )
        response.raise_for_status()

        payload = response.json()
        ctatt = payload.get("ctatt", {})

        err_code = ctatt.get("errCd", "0")
        if err_code != "0":
            raise RuntimeError(
                f"CTA Train Tracker error {err_code}: {ctatt.get('errNm', 'unknown')}"
            )

        vehicles: list[LiveVehicleState] = []
        for route_block in ctatt.get("route", []):
            route_name = route_block.get("@name", "")
            raw_trains = route_block.get("train", [])
            if isinstance(raw_trains, dict):
                raw_trains = [raw_trains]

            for item in raw_trains:
                vehicle = self._normalize_train(item, route_name)
                if vehicle is not None:
                    vehicles.append(vehicle)

        return vehicles

    @staticmethod
    def _normalize_train(item: dict, route_name: str) -> LiveVehicleState | None:
        try:
            latitude = float(item["lat"])
            longitude = float(item["lon"])
        except (KeyError, TypeError, ValueError):
            return None

        run_number = str(item.get("rn", "")).strip()
        if not run_number:
            return None

        destination = item.get("destNm") or None
        route_label = (
            f"{route_name.title()} Line - {destination}"
            if destination
            else f"{route_name.title()} Line"
        )

        current_status = "INCOMING_AT" if item.get("isApp", "0") == "1" else "IN_TRANSIT_TO"

        tr_dr = item.get("trDr")
        direction_id = int(tr_dr) if tr_dr and str(tr_dr).isdigit() else None

        updated_at = _parse_local_timestamp(item.get("prdt"), _CTA_TRAIN_TIMESTAMP_FORMATS)

        return LiveVehicleState(
            city="chicago",
            vehicle_id=f"train-{run_number}",
            route_id=route_name.lower(),
            route_label=route_label,
            trip_id=None,
            stop_id=item.get("nextStpId") or None,
            label=run_number,
            latitude=latitude,
            longitude=longitude,
            bearing=_optional_float(item.get("heading")),
            speed=None,
            current_status=current_status,
            occupancy_status=None,
            direction_id=direction_id,
            route_type=1,
            updated_at=updated_at,
            feed_timestamp=updated_at,
            source="cta_train_tracker",
        )

    async def close(self) -> None:
        await self.client.aclose()


class CtaVehicleClient:
    """
    Combined Chicago vehicle client.

    Buses and trains are fetched independently so one failing feed does not
    take down the other. Missing keys disable that feed at client creation time.
    Invalid keys disable that feed after the first failed poll.
    """

    def __init__(self):
        self._bus = _build_optional_client(CtaBusClient, "bus")
        self._train = _build_optional_client(CtaTrainClient, "train")
        self._disabled_feeds: set[str] = set()

    async def fetch_vehicle_positions(self) -> list[LiveVehicleState]:
        vehicles: list[LiveVehicleState] = []

        if self._bus is not None and "bus" not in self._disabled_feeds:
            try:
                buses = await self._bus.fetch_vehicle_positions()
                vehicles.extend(buses)
                print(f"[cta] fetched {len(buses)} buses", flush=True)
            except Exception as exc:
                print(f"[cta] bus fetch failed: {exc}", flush=True)
                if "invalid api key" in str(exc).lower():
                    self._disabled_feeds.add("bus")

        if self._train is not None and "train" not in self._disabled_feeds:
            try:
                trains = await self._train.fetch_vehicle_positions()
                vehicles.extend(trains)
                print(f"[cta] fetched {len(trains)} trains", flush=True)
            except Exception as exc:
                print(f"[cta] train fetch failed: {exc}", flush=True)
                if "invalid api key" in str(exc).lower():
                    self._disabled_feeds.add("train")

        return vehicles

    async def close(self) -> None:
        if self._bus is not None:
            await self._bus.close()
        if self._train is not None:
            await self._train.close()


def _build_optional_client(factory, label: str):
    try:
        return factory()
    except ValueError as exc:
        print(f"[cta] {label} client disabled: {exc}", flush=True)
        return None


def _raise_bus_error_if_present(bustime: dict) -> None:
    error = bustime.get("error")
    if not error:
        return

    messages = [
        entry.get("msg", str(entry))
        for entry in (error if isinstance(error, list) else [error])
    ]
    raise RuntimeError(f"CTA Bus Tracker error: {'; '.join(messages)}")


def _parse_bus_vehicle_response(bustime: dict) -> tuple[list[dict], set[str]]:
    errors = bustime.get("error") or []
    if not isinstance(errors, list):
        errors = [errors]

    inactive_routes: set[str] = set()
    fatal_messages: list[str] = []
    for entry in errors:
        message = entry.get("msg", str(entry))
        route_id = entry.get("rt")
        if route_id and message == "No data found for parameter":
            inactive_routes.add(route_id)
            continue
        fatal_messages.append(message)

    if fatal_messages:
        raise RuntimeError(f"CTA Bus Tracker error: {'; '.join(fatal_messages)}")

    return bustime.get("vehicle", []) or [], inactive_routes


def _parse_local_timestamp(value: str | None, formats: tuple[str, ...]) -> datetime | None:
    if not value:
        return None

    for timestamp_format in formats:
        try:
            localized = datetime.strptime(value, timestamp_format).replace(
                tzinfo=_CTA_LOCAL_TZ
            )
            return localized.astimezone(timezone.utc)
        except ValueError:
            continue

    return None


def _dedupe_latest(vehicles: list[LiveVehicleState]) -> dict[str, LiveVehicleState]:
    deduped: dict[str, LiveVehicleState] = {}

    for vehicle in vehicles:
        existing = deduped.get(vehicle.vehicle_id)
        if existing is None:
            deduped[vehicle.vehicle_id] = vehicle
            continue

        existing_ts = existing.updated_at or existing.feed_timestamp
        candidate_ts = vehicle.updated_at or vehicle.feed_timestamp
        if candidate_ts and (existing_ts is None or candidate_ts >= existing_ts):
            deduped[vehicle.vehicle_id] = vehicle

    return deduped


def _optional_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
