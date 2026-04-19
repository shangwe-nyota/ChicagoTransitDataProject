from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

from kafka import KafkaProducer

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.live.config import LIVE_POLL_INTERVAL_SECONDS
from src.live.mbta import MbtaVehicleClient
from src.live.topics import KAFKA_BOOTSTRAP_SERVERS, kafka_raw_topic


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        linger_ms=50,
    )


async def run(city: str, interval_seconds: float, once: bool) -> None:
    if city != "boston":
        raise ValueError("Only Boston live ingestion is implemented right now.")

    client = MbtaVehicleClient()
    producer = create_producer()
    topic = kafka_raw_topic(city)

    try:
        while True:
            started = time.perf_counter()
            vehicles = await client.fetch_vehicle_positions()
            for vehicle in vehicles:
                producer.send(topic, vehicle.model_dump(mode="json"))

            producer.flush()
            elapsed = time.perf_counter() - started
            print(f"[kafka-producer] wrote {len(vehicles)} vehicles to {topic} in {elapsed:.2f}s")

            if once:
                break

            await asyncio.sleep(max(0.0, interval_seconds - elapsed))
    finally:
        producer.close()
        await client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Poll MBTA live vehicles and publish normalized events to Kafka.")
    parser.add_argument("--city", default="boston")
    parser.add_argument("--interval", type=float, default=LIVE_POLL_INTERVAL_SECONDS)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run(city=args.city, interval_seconds=args.interval, once=args.once))
