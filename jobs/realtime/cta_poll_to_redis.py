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
from src.live.cta import CtaVehicleClient
from src.live.redis_store import RedisLiveStateStore


async def run(city: str, interval_seconds: float | None, once: bool) -> None:
    if city != "chicago":
        raise ValueError(
            f"This poller is for Chicago only. Got city={city!r}. "
            "Use mbta_poll_to_redis.py for Boston."
        )

    client = CtaVehicleClient()
    store = RedisLiveStateStore()
    effective_interval = interval_seconds if interval_seconds is not None else get_live_poll_interval_seconds(city)

    try:
        while True:
            started = time.perf_counter()
            vehicles = await client.fetch_vehicle_positions()
            for vehicle in vehicles:
                await store.upsert_vehicle(vehicle)

            elapsed = time.perf_counter() - started
            print(
                f"[live] wrote {len(vehicles)} Chicago vehicles to Redis in {elapsed:.2f}s",
                flush=True,
            )

            if once:
                break

            await asyncio.sleep(max(0.0, effective_interval - elapsed))
    finally:
        await client.close()
        await store.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Poll CTA live vehicles (buses + trains) and upsert into Redis directly."
    )
    parser.add_argument("--city", default="chicago")
    parser.add_argument("--interval", type=float, default=None)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run(city=args.city, interval_seconds=args.interval, once=args.once))
