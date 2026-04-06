"""
Flink Delay Metrics Job

Consumes CTA vehicle position events from Kafka, computes 5-minute
tumbling window delay metrics per route, and outputs to a Kafka sink topic.

Metrics per window:
  - route_id
  - delayed_count: number of vehicles marked as delayed
  - total_count: total vehicle events
  - delay_ratio: delayed_count / total_count

Prerequisites:
  - Kafka + Flink running (docker compose up)
  - CTA producer publishing to cta-vehicle-positions

Usage (submit to Flink cluster):
  flink run -py jobs/streaming/flink_delay_metrics.py
"""
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    FlinkKafkaConsumer,
    FlinkKafkaProducer,
)
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction


KAFKA_BOOTSTRAP = "kafka:29092"
INPUT_TOPIC = "cta-vehicle-positions"
OUTPUT_TOPIC = "cta-delay-metrics"
WINDOW_SIZE_MINUTES = 5


class DelayAggregator(AggregateFunction):
    """Accumulates (delayed_count, total_count) per route."""

    def create_accumulator(self):
        return (0, 0)

    def add(self, value, accumulator):
        is_delayed = value[1]
        return (accumulator[0] + (1 if is_delayed else 0), accumulator[1] + 1)

    def get_result(self, accumulator):
        delayed, total = accumulator
        ratio = delayed / total if total > 0 else 0.0
        return (delayed, total, ratio)

    def merge(self, a, b):
        return (a[0] + b[0], a[1] + b[1])


def parse_event(msg):
    """Parse JSON event into (route_id, is_delayed)."""
    data = json.loads(msg)
    return (data.get("route_id", "unknown"), data.get("is_delayed", False))


def format_output(result):
    """Format aggregation result as JSON string."""
    route_id, (delayed, total, ratio) = result
    output = {
        "route_id": route_id,
        "delayed_count": delayed,
        "total_count": total,
        "delay_ratio": round(ratio, 4),
        "window_minutes": WINDOW_SIZE_MINUTES,
    }
    return json.dumps(output)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Kafka source
    kafka_props = {"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "flink-delay-metrics"}
    consumer = FlinkKafkaConsumer(INPUT_TOPIC, SimpleStringSchema(), kafka_props)
    consumer.set_start_from_latest()

    stream = env.add_source(consumer)

    # Parse, key by route, window, aggregate
    result = (
        stream
        .map(parse_event, output_type=Types.TUPLE([Types.STRING(), Types.BOOLEAN()]))
        .key_by(lambda x: x[0])
        .window(TumblingProcessingTimeWindows.of(Time.minutes(WINDOW_SIZE_MINUTES)))
        .aggregate(DelayAggregator())
    )

    # Format and publish to output topic
    output_stream = result.map(format_output, output_type=Types.STRING())

    kafka_producer = FlinkKafkaProducer(OUTPUT_TOPIC, SimpleStringSchema(), {"bootstrap.servers": KAFKA_BOOTSTRAP})
    output_stream.add_sink(kafka_producer)

    env.execute("CTA Delay Metrics (5-min tumbling window)")


if __name__ == "__main__":
    main()
