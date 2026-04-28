from __future__ import annotations

import unittest
from datetime import datetime

from src.live.models import LiveVehicleState, LiveVehiclesResponse
from src.live.redis_store import RedisLiveStateStore


class LiveModelsAndStoreTests(unittest.TestCase):
    def test_live_vehicle_state_parses_iso_datetimes_and_sets_default_source(self) -> None:
        vehicle = LiveVehicleState.model_validate(
            {
                "city": "boston",
                "vehicle_id": "veh-1",
                "route_id": "1",
                "latitude": 42.36,
                "longitude": -71.05,
                "updated_at": "2026-04-27T12:34:56+00:00",
                "feed_timestamp": "2026-04-27T12:35:00+00:00",
            }
        )

        self.assertEqual(vehicle.city, "boston")
        self.assertEqual(vehicle.source, "unknown")
        self.assertIsInstance(vehicle.updated_at, datetime)
        self.assertIsInstance(vehicle.feed_timestamp, datetime)

    def test_live_vehicles_response_enforces_non_negative_vehicle_count(self) -> None:
        with self.assertRaises(Exception):
            LiveVehiclesResponse.model_validate(
                {
                    "city": "boston",
                    "vehicles": [],
                    "vehicle_count": -1,
                    "generated_at": "2026-04-27T12:35:00+00:00",
                }
            )

    def test_redis_store_key_helpers_are_city_scoped(self) -> None:
        self.assertEqual(
            RedisLiveStateStore._vehicle_key("boston", "veh-1"),
            "transit:live:boston:vehicle:veh-1",
        )
        self.assertEqual(
            RedisLiveStateStore._vehicle_index_key("chicago"),
            "transit:live:chicago:vehicles:index",
        )
        self.assertEqual(
            RedisLiveStateStore._metadata_key("boston"),
            "transit:live:boston:metadata",
        )
        self.assertEqual(
            RedisLiveStateStore._update_channel("chicago"),
            "transit:live:chicago:updates",
        )

    def test_decode_message_returns_none_for_non_payload_messages(self) -> None:
        self.assertIsNone(RedisLiveStateStore.decode_message(None))
        self.assertIsNone(RedisLiveStateStore.decode_message({"type": "subscribe", "data": 1}))
        self.assertIsNone(RedisLiveStateStore.decode_message({"type": "message", "data": ""}))

    def test_decode_message_parses_json_payload_messages(self) -> None:
        payload = RedisLiveStateStore.decode_message(
            {
                "type": "message",
                "data": '{"city":"boston","vehicle_id":"veh-1","latitude":42.36,"longitude":-71.05}',
            }
        )

        self.assertEqual(payload["city"], "boston")
        self.assertEqual(payload["vehicle_id"], "veh-1")


if __name__ == "__main__":
    unittest.main()
