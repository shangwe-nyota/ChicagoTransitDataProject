from __future__ import annotations

import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "transit.live")
KAFKA_REDIS_CONSUMER_GROUP = os.getenv("KAFKA_REDIS_CONSUMER_GROUP", "transit-live-redis-updater")


def kafka_raw_topic(city: str) -> str:
    return f"{KAFKA_TOPIC_PREFIX}.raw.{city}.vehicles"


def kafka_latest_topic(city: str) -> str:
    return f"{KAFKA_TOPIC_PREFIX}.latest.{city}.vehicles"
