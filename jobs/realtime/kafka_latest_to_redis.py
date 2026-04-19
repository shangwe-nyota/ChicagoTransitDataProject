from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from kafka import KafkaConsumer

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.live.models import LiveVehicleState
from src.live.redis_store import RedisLiveStateStore
from src.live.topics import KAFKA_BOOTSTRAP_SERVERS, KAFKA_REDIS_CONSUMER_GROUP, kafka_latest_topic


def create_consumer(city: str) -> KafkaConsumer:
    return KafkaConsumer(
        kafka_latest_topic(city),
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{KAFKA_REDIS_CONSUMER_GROUP}-{city}",
        value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )


async def consume(city: str) -> None:
    consumer = create_consumer(city)
    store = RedisLiveStateStore()
    processed = 0

    try:
        print(f"[redis-updater] consuming latest vehicle events for {city}", flush=True)
        for message in consumer:
            vehicle = LiveVehicleState.model_validate(message.value)
            await store.upsert_vehicle(vehicle)
            processed += 1
            if processed % 100 == 0:
                print(f"[redis-updater] upserted {processed} latest vehicle events for {city}", flush=True)
    finally:
        consumer.close()
        await store.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume Kafka latest-state events and upsert them into Redis.")
    parser.add_argument("--city", default="boston")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    import asyncio

    asyncio.run(consume(args.city))
