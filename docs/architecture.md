# Repository Architecture

This repository now centers on one shared multi-city transit platform rather than separate city-specific projects. The system has two primary execution paths:

- `Batch Atlas`
  - daily GTFS + OpenStreetMap processing for Boston and Chicago
  - Spark-based cleaning and analytics
  - Snowflake-backed storage and serving

- `Live Ops`
  - real-time vehicle ingestion for Boston and Chicago
  - Kafka + Flink latest-state processing
  - Redis + FastAPI serving
  - React + deck.gl + MapLibre dashboard

## High-Level Structure

### Batch path

The city-aware batch path is centered around:

- `jobs/pipeline/run_city_batch_pipeline.py`
- `jobs/ingestion/download_gtfs.py`
- `jobs/ingestion/download_osm.py`
- `jobs/spark/clean_gtfs_city.py`
- `jobs/spark/clean_osm_city.py`
- `jobs/spark/build_city_batch_analytics.py`
- `jobs/load/load_to_snowflake.py`

Batch orchestration is handled through:

- `dags/multi_city_batch_pipeline.py`
- `scripts/run_batch_pipeline.sh`

Outputs are staged under:

- `data/raw/gtfs/{city}`
- `data/raw/osm/{city}`
- `data/processed/{city}/clean/...`
- `data/processed/{city}/analytics/...`
- `data/staging/run_metadata/{run_id}/{city}/`
- `data/staging/checkpoints/{city}/`

### Live path

The shared live stack is centered around:

- `src/live/models.py`
- `src/live/mbta.py`
- `src/live/cta.py`
- `src/live/redis_store.py`
- `src/live/topics.py`
- `jobs/realtime/*.py`
- `dashboard/live_api.py`
- `dashboard/web/`

Preferred live flow:

1. city-specific poller
2. Kafka raw topic
3. Flink latest-state job
4. Kafka latest topic
5. Redis latest-state store
6. FastAPI
7. React dashboard

### Shared serving layer

The shared serving/UI layer is what turns this repository into one product rather than two unrelated pipelines:

- `dashboard/live_api.py`
  - Redis-backed live endpoints
  - Snowflake-backed batch endpoints

- `dashboard/web/src/App.jsx`
  - `Live Ops`
  - `Batch Atlas`

## Top-Level Directory Roles

- `dags/`
  - Airflow orchestration

- `dashboard/`
  - `app.py` is the legacy Streamlit dashboard
  - `live_api.py` is the shared FastAPI service
  - `web/` is the main React dashboard

- `jobs/`
  - ingestion, Spark transforms, realtime jobs, and loading

- `scripts/`
  - local launchers and runtime helpers

- `sql/ddl/`
  - Snowflake DDL for raw, clean, and analytics table families

- `src/common/`
  - shared config, paths, constants, and run metadata

- `src/batch/`
  - Snowflake-backed batch query service

- `src/live/`
  - live data contract and source-specific normalization

- `tests/`
  - targeted coverage for shared configuration, metadata, and service logic

## Key Design Decisions

- `city-awareness` is built into schemas, topics, Redis keys, and API routes
- `LiveVehicleState` is the common live contract across ingestion, streaming, serving, and UI
- `Snowflake` is the batch source of truth
- `Redis` is the live latest-state serving layer
- `FastAPI` is the stable boundary between data systems and the frontend
- `raw -> clean -> analytics` remains the central batch modeling pattern

## Recommended Starting Points

If a new contributor needs to understand the repository quickly, the most useful sequence is:

1. `README.md`
2. `docs/pipeline_flow.md`
3. `src/live/`
4. `jobs/realtime/`
5. `jobs/pipeline/run_city_batch_pipeline.py`
6. `dashboard/live_api.py`
7. `dashboard/web/`
