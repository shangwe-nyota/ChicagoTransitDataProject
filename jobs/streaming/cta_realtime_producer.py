"""
CTA Realtime Kafka Producer

Polls the CTA Train Tracker API every 30 seconds and publishes
vehicle arrival events to a Kafka topic.

Prerequisites:
  - CTA API key (set CTA_API_KEY in .env)
  - Kafka running on localhost:9092 (see docker-compose.yml)

Usage:
  python3 jobs/streaming/cta_realtime_producer.py
"""
import os
import sys
import json
import time
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# CTA Train Tracker API
API_KEY = os.getenv("CTA_API_KEY")
TRAIN_TRACKER_URL = "https://lapi.transitchicago.com/api/1.0/ttarrivals.aspx"

# Kafka config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "cta-vehicle-positions"
POLL_INTERVAL = int(os.getenv("CTA_POLL_INTERVAL", "30"))

# Major CTA L stations to poll (map IDs)
# These cover all 8 L lines
STATION_IDS = [
    40380,  # Clark/Lake (all lines)
    40260,  # State/Lake
    40730,  # Washington/Wells
    41660,  # Lake (Red)
    40530,  # Chicago (Brown/Purple)
    40710,  # Chicago (Red)
    40080,  # Belmont (Red/Brown/Purple)
    40670,  # Howard (Red/Purple/Yellow)
    40390,  # Forest Park (Blue)
    40890,  # O'Hare (Blue)
    40850,  # Harold Washington Library (Brown/Orange/Purple/Pink)
    41120,  # Adams/Wabash
    40160,  # LaSalle/Van Buren
    40360,  # Fullerton (Red/Brown/Purple)
    41450,  # 95th/Dan Ryan (Red)
    40190,  # Sox-35th (Red)
    41660,  # Lake (Red)
    40900,  # Midway (Orange)
]


def create_producer():
    """Create a Kafka producer. Falls back to console output if Kafka unavailable."""
    try:
        from confluent_kafka import Producer
        producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
        print(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
        return producer
    except ImportError:
        print("WARNING: confluent-kafka not installed. Writing to stdout instead.")
        print("Install with: pip install confluent-kafka")
        return None
    except Exception as e:
        print(f"WARNING: Cannot connect to Kafka ({e}). Writing to stdout instead.")
        return None


def poll_train_tracker(station_id):
    """Poll CTA Train Tracker for a specific station."""
    params = {
        "key": API_KEY,
        "mapid": station_id,
        "outputType": "JSON",
        "max": 20,
    }
    response = requests.get(TRAIN_TRACKER_URL, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def parse_arrivals(data, poll_timestamp):
    """Parse CTA Train Tracker JSON into normalized event records."""
    events = []
    ctatt = data.get("ctatt", {})
    eta_list = ctatt.get("eta", [])

    for eta in eta_list:
        event = {
            "poll_timestamp": poll_timestamp,
            "station_id": str(eta.get("staId", "")),
            "station_name": eta.get("staNm", ""),
            "stop_id": str(eta.get("stpId", "")),
            "stop_description": eta.get("stpDe", ""),
            "run_number": str(eta.get("rn", "")),
            "route_id": eta.get("rt", ""),
            "destination_stop": str(eta.get("destSt", "")),
            "destination_name": eta.get("destNm", ""),
            "direction": eta.get("trDr", ""),
            "prediction_time": eta.get("prdt", ""),
            "arrival_time": eta.get("arrT", ""),
            "is_approaching": eta.get("isApp", "0") == "1",
            "is_scheduled": eta.get("isSch", "0") == "1",
            "is_delayed": eta.get("isDly", "0") == "1",
            "is_fault": eta.get("isFlt", "0") == "1",
            "latitude": eta.get("lat", ""),
            "longitude": eta.get("lon", ""),
            "heading": eta.get("heading", ""),
        }
        events.append(event)

    return events


def publish_events(producer, events):
    """Publish events to Kafka topic or stdout."""
    for event in events:
        msg = json.dumps(event)
        if producer is not None:
            producer.produce(TOPIC, value=msg.encode("utf-8"))
        else:
            print(f"  [event] {event['station_name']} | {event['route_id']} line | "
                  f"delayed={event['is_delayed']} | arr={event['arrival_time']}")

    if producer is not None:
        producer.flush()


def run_poller():
    """Main polling loop."""
    if not API_KEY:
        print("ERROR: CTA_API_KEY not set in .env file.")
        print("Register at: https://www.transitchicago.com/developers/traintrackerapply/")
        sys.exit(1)

    producer = create_producer()
    print(f"Polling {len(STATION_IDS)} stations every {POLL_INTERVAL}s...")
    print(f"Publishing to Kafka topic: {TOPIC}")
    print()

    cycle = 0
    while True:
        cycle += 1
        poll_timestamp = datetime.utcnow().isoformat() + "Z"
        total_events = 0

        for station_id in STATION_IDS:
            try:
                data = poll_train_tracker(station_id)
                events = parse_arrivals(data, poll_timestamp)
                publish_events(producer, events)
                total_events += len(events)
            except requests.exceptions.RequestException as e:
                print(f"  [error] Station {station_id}: {e}")
            except Exception as e:
                print(f"  [error] Station {station_id}: {e}")

        print(f"[cycle {cycle}] {poll_timestamp} | {total_events} events published")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run_poller()
