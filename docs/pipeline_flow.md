# Pipeline Flow

## 1. Chicago Batch Flow

Current implemented Chicago batch flow:

1. `jobs/ingestion/download_gtfs.py`
   - downloads CTA GTFS static data
   - extracts raw files into `data/raw/gtfs/`
2. Spark cleaning jobs
   - `jobs/spark/clean_gtfs.py`
   - `jobs/spark/clean_gtfs_routes.py`
   - `jobs/spark/clean_gtfs_trips.py`
   - `jobs/spark/clean_gtfs_stop_times.py`
   - `jobs/spark/clean_gtfs_shapes.py`
3. Spark analytics jobs
   - `jobs/spark/analytics/stop_activity.py`
   - `jobs/spark/analytics/join_stop_activity_with_stops.py`
   - `jobs/spark/analytics/route_activity.py`
   - `jobs/spark/analytics/stop_activity_by_route.py`
   - `jobs/spark/analytics/route_shapes.py`
4. outputs written to `data/processed/...`
5. optional Snowflake loading through `jobs/load/load_to_snowflake.py`
6. visualization through the legacy Streamlit dashboard

## 2. Boston Live Flow

Preferred Boston live flow:

1. MBTA vehicle feed
2. `jobs/realtime/mbta_poll_to_kafka.py`
3. Kafka raw topic:
   - `transit.live.raw.boston.vehicles`
4. `jobs/realtime/flink_vehicle_latest_job.py boston`
5. Kafka latest topic:
   - `transit.live.latest.boston.vehicles`
6. `jobs/realtime/kafka_latest_to_redis.py --city boston`
7. Redis latest-state store
8. `dashboard/live_api.py`
9. `dashboard/web/`

Fallback Boston live flow:

1. MBTA vehicle feed
2. `jobs/realtime/mbta_poll_to_redis.py`
3. Redis
4. FastAPI
5. React dashboard

## 3. Chicago Live Flow

Preferred Chicago live flow:

1. CTA bus and optional train feeds
2. `jobs/realtime/cta_poll_to_kafka.py`
3. Kafka raw topic:
   - `transit.live.raw.chicago.vehicles`
4. `jobs/realtime/flink_vehicle_latest_job.py chicago`
5. Kafka latest topic:
   - `transit.live.latest.chicago.vehicles`
6. `jobs/realtime/kafka_latest_to_redis.py --city chicago`
7. Redis latest-state store
8. `dashboard/live_api.py`
9. `dashboard/web/`

Fallback Chicago live flow:

1. CTA live feed
2. `jobs/realtime/cta_poll_to_redis.py`
3. Redis
4. FastAPI
5. React dashboard

Important current caveat:

- Chicago buses work
- Chicago trains are blocked by the currently configured CTA train key

## 4. Shared Live Serving Flow

Regardless of city, the frontend flow is:

1. `GET /api/live/{city}/vehicles` for the initial snapshot
2. `WS /ws/live/{city}` for live updates
3. render markers in `deck.gl`
4. show them over a MapLibre basemap

The frontend is intentionally decoupled from Kafka and Flink.

## 5. Local Orchestration Flow

The preferred local command is:

```bash
bash scripts/live.sh all
```

This currently means:

1. start Redis
2. start Kafka
3. create Boston and Chicago topics
4. start per-city Flink latest-state jobs
5. start per-city Kafka latest-to-Redis consumers
6. start per-city upstream producers
7. start FastAPI
8. start the React dashboard

Useful URLs:

- `http://127.0.0.1:5173`
- `http://127.0.0.1:8000/api/live/boston/health`
- `http://127.0.0.1:8000/api/live/chicago/health`

## 5b. Shared Dashboard Flow

The newer dashboard now has two operating modes in one React app:

1. `Live Ops`
   - Redis-backed
   - WebSocket-updated
   - powered by the realtime stack
2. `Batch Atlas`
   - Snowflake-backed through FastAPI
   - powered by the multi-city GTFS + OSM batch tables

Batch API endpoints currently used by the frontend:

- `GET /api/batch/cities`
- `GET /api/batch/comparison`
- `GET /api/batch/{city}/dashboard`
- `GET /api/batch/{city}/routes`
- `GET /api/batch/{city}/routes/{route_id}`

This keeps the browser decoupled from direct Snowflake access while still using the warehouse as the source of truth for batch analytics.

## 6. Shared City-Aware Batch Flow

The repo now has a newer city-aware batch path alongside the legacy Chicago-only jobs.

Current implemented flow:

1. `jobs/pipeline/run_city_batch_pipeline.py --city {city}`
2. `jobs/ingestion/download_gtfs.py --city {city}`
3. `jobs/ingestion/download_osm.py --city {city}`
4. `jobs/spark/clean_gtfs_city.py --city {city}`
5. `jobs/spark/clean_osm_city.py --city {city}`
6. `jobs/spark/build_city_batch_analytics.py --city {city}`
7. optional `python -m jobs.load.load_to_snowflake`

Current supported cities:

- `chicago`
- `boston`

Current city-aware GTFS + OSM analytics outputs include:

- `stop_poi_access`
- `busiest_stops_with_poi_context`
- `route_poi_access`
- `transit_road_coverage`

This is now the preferred path for:

- Boston batch GTFS + OSM
- Chicago GTFS + OSM continuation
- future Snowflake-backed batch API work

Manual trigger command:

```bash
bash scripts/run_batch_pipeline.sh all --load-snowflake
```

Single-city examples:

```bash
bash scripts/run_batch_pipeline.sh chicago
bash scripts/run_batch_pipeline.sh boston --load-snowflake
```

## 7. Batch Run Metadata And Checkpointing

The city-aware batch path now records run metadata and supports practical resume behavior.

Key files:

- `src/common/run_metadata.py`
  - shared manifest/checkpoint helper
- `src/common/paths.py`
  - staging/run-metadata/checkpoint paths
- `jobs/pipeline/run_city_batch_pipeline.py`
  - orchestrates a full city batch run with a shared `run_id`

Run metadata is written under:

- `data/staging/run_metadata/{run_id}/{city}/`

Per-city latest stage checkpoints are written under:

- `data/staging/checkpoints/{city}/`

Important behavior:

- stages write JSON manifests with:
  - status
  - command
  - input/output path stats
  - row counts and load metrics where available
- rerunning the same city batch pipeline with the same `run_id` skips completed stages
- this was verified on a real Boston rerun after an earlier failed stage

Example:

```bash
python jobs/pipeline/run_city_batch_pipeline.py --city boston --run-id boston-20260419T215417Z-863baa76
```

On a completed run, the pipeline now skips:

- GTFS download
- OSM download
- GTFS clean
- OSM clean
- analytics build

and exits successfully without recomputing them.

## 8. Airflow Batch Orchestration

The old Chicago-only DAG has been replaced by:

- `dags/multi_city_batch_pipeline.py`

Current Airflow flow:

1. run Chicago city batch pipeline
2. run Boston city batch pipeline
3. load shared multi-city `BATCH_*` outputs to Snowflake

Schedule:

- `@daily`

Important detail:

- Chicago and Boston run as separate city batch tasks
- Snowflake load runs only after both city tasks succeed
- the DAG uses a shared Airflow run id shape:
  - `airflow-{{ ts_nodash }}`
  - that means reruns for the same Airflow execution can reuse the batch manifest/checkpoint system cleanly
