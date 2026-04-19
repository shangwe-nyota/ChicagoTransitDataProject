from __future__ import annotations

import argparse
import asyncio
import time
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.live.config import get_live_poll_interval_seconds
from src.live.mbta import MbtaVehicleClient
from src.live.redis_store import RedisLiveStateStore


async def run(city: str, interval_seconds: float | None, once: bool) -> None:
    if city != "boston":
        raise ValueError("Only Boston live ingestion is implemented right now.")

    client = MbtaVehicleClient()
    store = RedisLiveStateStore()
    effective_interval = interval_seconds if interval_seconds is not None else get_live_poll_interval_seconds(city)

    try:
        while True:
            started = time.perf_counter()
            vehicles = await client.fetch_vehicle_positions()
            for vehicle in vehicles:
                await store.upsert_vehicle(vehicle)

            elapsed = time.perf_counter() - started
            print(f"[live] wrote {len(vehicles)} Boston vehicles to Redis in {elapsed:.2f}s")

            if once:
                break

            await asyncio.sleep(max(0.0, effective_interval - elapsed))
    finally:
        await client.close()
        await store.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Poll MBTA live vehicles and upsert the latest state into Redis.")
    parser.add_argument("--city", default="boston")
    parser.add_argument("--interval", type=float, default=None)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run(city=args.city, interval_seconds=args.interval, once=args.once))
