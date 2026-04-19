from __future__ import annotations

import json
from datetime import datetime, timezone

from redis.asyncio import Redis

from src.live.config import LIVE_VEHICLE_TTL_SECONDS, REDIS_URL
from src.live.models import LiveVehicleState


class RedisLiveStateStore:
    def __init__(self, redis_url: str = REDIS_URL, ttl_seconds: int = LIVE_VEHICLE_TTL_SECONDS):
        self.client = Redis.from_url(redis_url, decode_responses=True)
        self.ttl_seconds = ttl_seconds

    @staticmethod
    def _vehicle_key(city: str, vehicle_id: str) -> str:
        return f"transit:live:{city}:vehicle:{vehicle_id}"

    @staticmethod
    def _vehicle_index_key(city: str) -> str:
        return f"transit:live:{city}:vehicles:index"

    @staticmethod
    def _update_channel(city: str) -> str:
        return f"transit:live:{city}:updates"

    @staticmethod
    def _metadata_key(city: str) -> str:
        return f"transit:live:{city}:metadata"

    async def ping(self) -> bool:
        return bool(await self.client.ping())

    async def upsert_vehicle(self, vehicle: LiveVehicleState) -> None:
        payload = vehicle.model_dump_json()
        metadata = {
            "last_upsert_at": datetime.now(timezone.utc).isoformat(),
            "last_vehicle_id": vehicle.vehicle_id,
        }

        async with self.client.pipeline(transaction=False) as pipe:
            pipe.set(self._vehicle_key(vehicle.city, vehicle.vehicle_id), payload, ex=self.ttl_seconds)
            pipe.sadd(self._vehicle_index_key(vehicle.city), vehicle.vehicle_id)
            pipe.hset(self._metadata_key(vehicle.city), mapping=metadata)
            pipe.publish(self._update_channel(vehicle.city), payload)
            await pipe.execute()

    async def list_vehicles(self, city: str, route_id: str | None = None) -> list[LiveVehicleState]:
        vehicle_ids = sorted(await self.client.smembers(self._vehicle_index_key(city)))
        if not vehicle_ids:
            return []

        raw_payloads = await self.client.mget([self._vehicle_key(city, vehicle_id) for vehicle_id in vehicle_ids])

        stale_ids: list[str] = []
        vehicles: list[LiveVehicleState] = []

        for vehicle_id, payload in zip(vehicle_ids, raw_payloads):
            if payload is None:
                stale_ids.append(vehicle_id)
                continue

            vehicle = LiveVehicleState.model_validate_json(payload)
            if route_id and vehicle.route_id != route_id:
                continue
            vehicles.append(vehicle)

        if stale_ids:
            await self.client.srem(self._vehicle_index_key(city), *stale_ids)

        vehicles.sort(
            key=lambda vehicle: (
                vehicle.route_id or "",
                vehicle.label or vehicle.vehicle_id,
            )
        )
        return vehicles

    async def get_metadata(self, city: str) -> dict[str, str]:
        metadata = await self.client.hgetall(self._metadata_key(city))
        if "last_upsert_at" not in metadata:
            metadata["last_upsert_at"] = datetime.now(timezone.utc).isoformat()
        return metadata

    async def subscribe(self, city: str):
        pubsub = self.client.pubsub()
        await pubsub.subscribe(self._update_channel(city))
        return pubsub

    async def close(self) -> None:
        await self.client.aclose()

    @staticmethod
    def decode_message(message) -> dict | None:
        if not message or message.get("type") != "message":
            return None

        data = message.get("data")
        if not data:
            return None

        if isinstance(data, str):
            return json.loads(data)

        return None

