"""
Streaming Sink to Snowflake

Consumes aggregated metrics from Kafka output topics and batch-inserts
them into Snowflake realtime tables every 60 seconds.

Topics consumed:
  - cta-delay-metrics -> REALTIME_DELAY_METRICS
  - cta-vehicle-counts -> REALTIME_VEHICLE_COUNTS

Prerequisites:
  - Kafka running with Flink output topics populated
  - Snowflake credentials in .env
  - Realtime DDL tables created (sql/ddl/realtime_tables.sql)

Usage:
  python3 jobs/streaming/sink_to_snowflake.py
"""
import os
import sys
import json
import time
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from dotenv import load_dotenv
import pandas as pd

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FLUSH_INTERVAL = int(os.getenv("SINK_FLUSH_INTERVAL", "60"))

TOPIC_TABLE_MAP = {
    "cta-delay-metrics": "REALTIME_DELAY_METRICS",
    "cta-vehicle-counts": "REALTIME_VEHICLE_COUNTS",
}


def create_consumer(topics):
    """Create Kafka consumer for output topics."""
    try:
        from confluent_kafka import Consumer
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "snowflake-sink",
            "auto.offset.reset": "latest",
        })
        consumer.subscribe(topics)
        print(f"Subscribed to: {topics}")
        return consumer
    except ImportError:
        print("ERROR: confluent-kafka not installed. Install with: pip install confluent-kafka")
        sys.exit(1)


def flush_to_snowflake(table_name, records):
    """Batch-insert records into a Snowflake table."""
    if not records:
        return

    from src.snowflake.connector import get_snowflake_connection
    from snowflake.connector.pandas_tools import write_pandas

    df = pd.DataFrame(records)
    df["INSERTED_AT"] = datetime.utcnow().isoformat()
    df.columns = [c.upper() for c in df.columns]

    conn = get_snowflake_connection()
    try:
        success, _, nrows, _ = write_pandas(conn, df, table_name, auto_create_table=False)
        if success:
            print(f"  Flushed {nrows} rows to {table_name}")
        else:
            print(f"  ERROR flushing to {table_name}")
    finally:
        conn.close()


def run_sink():
    """Main consumer loop: poll Kafka, buffer records, flush to Snowflake."""
    topics = list(TOPIC_TABLE_MAP.keys())
    consumer = create_consumer(topics)

    buffers = {topic: [] for topic in topics}
    last_flush = time.time()

    print(f"Sinking to Snowflake every {FLUSH_INTERVAL}s...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is not None and msg.error() is None:
                topic = msg.topic()
                data = json.loads(msg.value().decode("utf-8"))
                buffers[topic].append(data)

            # Flush on interval
            if time.time() - last_flush >= FLUSH_INTERVAL:
                for topic, records in buffers.items():
                    if records:
                        table_name = TOPIC_TABLE_MAP[topic]
                        flush_to_snowflake(table_name, records)
                        buffers[topic] = []
                last_flush = time.time()

    except KeyboardInterrupt:
        print("\nShutting down...")
        # Final flush
        for topic, records in buffers.items():
            if records:
                flush_to_snowflake(TOPIC_TABLE_MAP[topic], records)
    finally:
        consumer.close()


if __name__ == "__main__":
    run_sink()
