# Real-Time Public Transit Reliability and Mapping Platform

This repository contains our final project for a multi-city transit intelligence platform built around two complementary views of public transit:

- `Batch Atlas`: daily GTFS + OpenStreetMap analytics for Boston and Chicago
- `Live Ops`: real-time vehicle tracking for Boston and Chicago through Kafka, Flink, Redis, FastAPI, and a React map UI

The project began as a Chicago GTFS batch analytics pipeline and evolved into a shared multi-city system that now supports:

- city-aware GTFS + OSM batch processing
- Snowflake-backed batch serving
- Boston live vehicle tracking
- Chicago live bus tracking on the same live stack
- one shared dashboard with both batch and live modes

Code is the source of truth. The README is intended to give the teaching team and future developers a reliable onboarding path.

## Repository Overview

There are three main layers in the current project:

1. `Multi-city batch pipeline`
   - downloads GTFS static and OpenStreetMap data for Boston and Chicago
   - cleans raw data into city-aware Parquet entity tables
   - computes service, accessibility, route, and roadway metrics
   - loads clean and analytics outputs into Snowflake `BATCH_*` tables

2. `Realtime streaming pipeline`
   - polls agency live vehicle feeds
   - normalizes them into a shared `LiveVehicleState` contract
   - writes raw events to Kafka
   - uses Flink to compute latest per-vehicle state
   - serves latest-state records through Redis and FastAPI

3. `Shared API and dashboard`
   - FastAPI exposes both live and batch endpoints
   - React + deck.gl + MapLibre renders:
     - `Live Ops`
     - `Batch Atlas`

## Current Status

What is working today:

- Boston and Chicago batch GTFS + OSM runs
- city-aware clean Parquet outputs
- city-aware analytics Parquet outputs
- Snowflake DDL and multi-city load path
- Snowflake-backed batch API endpoints
- batch mode inside the shared React dashboard
- Boston live vehicle dashboard
- Chicago live bus dashboard
- Kafka -> Flink -> Kafka latest -> Redis -> FastAPI live path
- direct-to-Redis fallback live pollers
- daily Airflow DAG for multi-city batch orchestration
- local launcher scripts for both batch and live workflows

Known limitations:

- Chicago train live support is code-complete but blocked by the configured CTA train API key
- some legacy Chicago-only files remain for reference
- tests exist for important shared pieces, but coverage is still lighter than a production system

## Project Structure

Important directories:

- `dags/`
  - Airflow orchestration
  - `multi_city_batch_pipeline.py` is the main batch DAG

- `jobs/ingestion/`
  - GTFS and OSM download jobs

- `jobs/spark/`
  - city-aware batch cleaning and analytics jobs

- `jobs/realtime/`
  - MBTA and CTA live pollers
  - Flink-to-Redis bridge jobs

- `jobs/load/`
  - Snowflake loading

- `jobs/pipeline/`
  - city-aware batch orchestration entrypoint

- `src/live/`
  - live normalization contracts and serving helpers

- `src/batch/`
  - Snowflake-backed batch query service

- `src/common/`
  - shared configuration, paths, constants, and run metadata

- `dashboard/live_api.py`
  - FastAPI service for both live and batch endpoints

- `dashboard/web/`
  - React dashboard frontend

- `sql/ddl/`
  - Snowflake DDL for raw, clean, and analytics table families

- `tests/`
  - targeted tests for shared batch configuration, run metadata, and batch service behavior

## Core Data Flows

### Batch flow

1. Airflow or local shell entrypoint starts a batch run
2. Chicago and Boston batch jobs run independently
3. GTFS static + OpenStreetMap data are downloaded into city-scoped raw folders
4. Spark cleaning jobs create city-aware clean Parquet datasets
5. Spark analytics jobs create route, stop, access, and roadway outputs
6. Snowflake loader writes clean and analytics outputs into `BATCH_*` tables
7. FastAPI serves batch results to the shared dashboard

### Live flow

1. MBTA or CTA vehicle feed is polled
2. records are normalized into `LiveVehicleState`
3. normalized records are written to city-specific Kafka raw topics
4. Flink keeps the latest record per `city + vehicle_id`
5. latest-state records are written to Kafka latest topics
6. Redis stores the current latest state for serving
7. FastAPI exposes REST + WebSocket endpoints
8. the React dashboard renders the live map

## Key Contracts And Storage Layers

### Shared live contract

The central live contract is `LiveVehicleState` in `src/live/models.py`. It is reused across:

- source normalization
- Kafka payloads
- Flink processing
- Redis storage
- FastAPI responses
- frontend rendering

### Batch storage layers

The batch warehouse is intentionally organized into three layers:

- `Raw / source trace`
  - local source-of-truth files used for replay and debugging

- `Clean tables`
  - normalized city-aware GTFS and OSM entities such as:
    - `BATCH_GTFS_STOPS`
    - `BATCH_GTFS_ROUTES`
    - `BATCH_GTFS_TRIPS`
    - `BATCH_GTFS_STOP_TIMES`
    - `BATCH_OSM_POIS`
    - `BATCH_OSM_ROADS`

- `Analytics tables`
  - denormalized dashboard-facing outputs such as:
    - `BATCH_STOP_ACTIVITY`
    - `BATCH_STOP_ACTIVITY_ENRICHED`
    - `BATCH_ROUTE_ACTIVITY`
    - `BATCH_STOP_ACTIVITY_BY_ROUTE`
    - `BATCH_ROUTE_SHAPES`
    - `BATCH_STOP_POI_ACCESS`
    - `BATCH_BUSIEST_STOPS_WITH_POI_CONTEXT`
    - `BATCH_ROUTE_POI_ACCESS`
    - `BATCH_TRANSIT_ROAD_COVERAGE`

## Running The Project

### Batch

Preferred local batch entrypoint:

```bash
bash scripts/run_batch_pipeline.sh all --load-snowflake
```

Other examples:

```bash
bash scripts/run_batch_pipeline.sh chicago
bash scripts/run_batch_pipeline.sh boston --load-snowflake
bash scripts/run_batch_pipeline.sh snowflake
```

The batch pipeline records:

- `run_id`
- stage manifests
- checkpoints

under:

- `data/staging/run_metadata/{run_id}/{city}/`
- `data/staging/checkpoints/{city}/`

### Live

Preferred local live entrypoint:

```bash
bash scripts/live.sh all
```

This starts:

- Redis
- Kafka
- city topics
- Flink latest-state jobs
- Kafka latest-to-Redis consumers
- upstream producers
- FastAPI
- React dashboard

Useful variants:

```bash
bash scripts/live.sh all boston
bash scripts/live.sh all chicago
bash scripts/live.sh status
bash scripts/live.sh logs
bash scripts/live.sh down
```

Expected URLs:

- UI: `http://127.0.0.1:5173`
- API: `http://127.0.0.1:8000`
- Boston health: `http://127.0.0.1:8000/api/live/boston/health`
- Chicago health: `http://127.0.0.1:8000/api/live/chicago/health`

## Environment

Important environment variables include:

- `LIVE_CITIES`
- `REDIS_URL`
- `LIVE_API_HOST`
- `LIVE_API_PORT`
- `LIVE_VEHICLE_TTL_SECONDS`
- `MBTA_API_KEY`
- `CTA_BUS_TRACKER_API_KEY`
- `CTA_TRAIN_TRACKER_API_KEY`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC_PREFIX`
- `FLINK_KAFKA_CONNECTOR_JAR`

For Snowflake:

- `SNOWFLAKE_USER`
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_PRIVATE_KEY_FILE`

See `.env.example` for the expected shape.

## Tests

Current tests cover several of the most important shared components:

- `tests/test_batch_config.py`
- `tests/test_run_metadata.py`
- `tests/test_batch_service.py`

Run them with:

```bash
pytest
```

## Notes For Reviewers

- The live dashboard is strongest during daytime when upstream feeds are fuller.
- Boston is the most reliable full live demo city.
- Chicago live currently works best in bus mode because the train key is invalid.
- The newer dashboard is the main product surface; `dashboard/app.py` is the legacy Streamlit batch dashboard kept mainly as a reference.

## Additional Documentation

Supporting docs are kept under `docs/`:

- `docs/architecture.md`
- `docs/pipeline_flow.md`
- `docs/live_architecture.md`

