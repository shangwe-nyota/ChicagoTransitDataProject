from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyflink.common import SimpleStringSchema, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from src.live.topics import KAFKA_BOOTSTRAP_SERVERS, kafka_latest_topic, kafka_raw_topic


def timestamp_to_epoch_ms(value: str | None) -> int:
    if not value:
        return 0
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


class LatestVehicleOnly(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("latest_vehicle_timestamp_ms", Types.LONG())
        self.latest_timestamp = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        payload = json.loads(value)
        candidate = max(
            timestamp_to_epoch_ms(payload.get("updated_at")),
            timestamp_to_epoch_ms(payload.get("feed_timestamp")),
        )
        current = self.latest_timestamp.value()
        if current is None or candidate >= current:
            self.latest_timestamp.update(candidate)
            yield json.dumps(payload)


def main(city: str = "boston") -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    connector_jar = os.getenv("FLINK_KAFKA_CONNECTOR_JAR")
    if connector_jar:
        connector_uri = connector_jar
        if not connector_uri.startswith("file://"):
            connector_uri = f"file://{connector_uri}"
        env.add_jars(connector_uri)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(kafka_raw_topic(city))
        .set_group_id(f"transit-live-flink-{city}")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(kafka_latest_topic(city))
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        source_name=f"{city}-raw-vehicles",
        type_info=Types.STRING(),
    )

    (
        stream
        .key_by(lambda value: f"{json.loads(value)['city']}:{json.loads(value)['vehicle_id']}", key_type=Types.STRING())
        .process(LatestVehicleOnly(), output_type=Types.STRING())
        .sink_to(sink)
    )

    env.execute(f"{city}-vehicle-latest")


if __name__ == "__main__":
    selected_city = sys.argv[1] if len(sys.argv) > 1 else "boston"
    main(city=selected_city)
