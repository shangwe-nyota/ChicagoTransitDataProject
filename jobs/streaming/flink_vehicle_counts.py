"""
Flink Vehicle Counts Job

Consumes CTA vehicle position events from Kafka, computes 5-minute
tumbling window vehicle counts per route.

Metrics per window:
  - route_id
  - vehicle_count: distinct vehicles (run numbers) active on the route
  - event_count: total events in window

Prerequisites:
  - Kafka + Flink running (docker compose up)
  - CTA producer publishing to cta-vehicle-positions

Usage (submit to Flink cluster):
  flink run -py jobs/streaming/flink_vehicle_counts.py
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
from pyflink.datastream.functions import AggregateFunction


KAFKA_BOOTSTRAP = "kafka:29092"
INPUT_TOPIC = "cta-vehicle-positions"
OUTPUT_TOPIC = "cta-vehicle-counts"
WINDOW_SIZE_MINUTES = 5


class VehicleCountAggregator(AggregateFunction):
    """Accumulates (set of run_numbers, event_count) per route."""

    def create_accumulator(self):
        return (set(), 0)

    def add(self, value, accumulator):
        run_number = value[1]
        vehicles = accumulator[0]
        vehicles.add(run_number)
        return (vehicles, accumulator[1] + 1)

    def get_result(self, accumulator):
        return (len(accumulator[0]), accumulator[1])

    def merge(self, a, b):
        return (a[0] | b[0], a[1] + b[1])


def parse_event(msg):
    """Parse JSON event into (route_id, run_number)."""
    data = json.loads(msg)
    return (data.get("route_id", "unknown"), data.get("run_number", ""))


def format_output(result):
    """Format aggregation result as JSON string."""
    route_id, (vehicle_count, event_count) = result
    output = {
        "route_id": route_id,
        "vehicle_count": vehicle_count,
        "event_count": event_count,
        "window_minutes": WINDOW_SIZE_MINUTES,
    }
    return json.dumps(output)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_props = {"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "flink-vehicle-counts"}
    consumer = FlinkKafkaConsumer(INPUT_TOPIC, SimpleStringSchema(), kafka_props)
    consumer.set_start_from_latest()

    stream = env.add_source(consumer)

    result = (
        stream
        .map(parse_event, output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
        .key_by(lambda x: x[0])
        .window(TumblingProcessingTimeWindows.of(Time.minutes(WINDOW_SIZE_MINUTES)))
        .aggregate(VehicleCountAggregator())
    )

    output_stream = result.map(format_output, output_type=Types.STRING())

    kafka_producer = FlinkKafkaProducer(OUTPUT_TOPIC, SimpleStringSchema(), {"bootstrap.servers": KAFKA_BOOTSTRAP})
    output_stream.add_sink(kafka_producer)

    env.execute("CTA Vehicle Counts (5-min tumbling window)")


if __name__ == "__main__":
    main()
