# Streaming Jobs

Real-time GTFS pipeline: CTA Train Tracker API -> Kafka -> Flink -> Snowflake.

## Prerequisites

- CTA API key (register at https://www.transitchicago.com/developers/traintrackerapply/)
- Docker Desktop (for Kafka + Flink)
- Set `CTA_API_KEY` in `.env`

## Quick Start

```bash
# 1. Start Kafka + Flink
docker compose up -d

# 2. Run the CTA poller (publishes to Kafka)
python3 jobs/streaming/cta_realtime_producer.py

# 3. Submit Flink jobs (in another terminal)
flink run -py jobs/streaming/flink_delay_metrics.py
flink run -py jobs/streaming/flink_vehicle_counts.py

# 4. Sink aggregated metrics to Snowflake (another terminal)
python3 jobs/streaming/sink_to_snowflake.py
```

## Files

| File | Description |
|------|-------------|
| `cta_realtime_producer.py` | Polls CTA Train Tracker every 30s, publishes to Kafka |
| `flink_delay_metrics.py` | 5-min tumbling window delay ratio per route |
| `flink_vehicle_counts.py` | 5-min tumbling window vehicle counts per route |
| `sink_to_snowflake.py` | Consumes Flink output, batch-inserts to Snowflake |

## Kafka Topics

- `cta-vehicle-positions` — raw arrival events from CTA API
- `cta-delay-metrics` — Flink output: delay ratios per route per window
- `cta-vehicle-counts` — Flink output: active vehicles per route per window
