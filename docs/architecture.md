# Repository Architecture

## High-Level View

This repository currently has two strong implementation centers and one emerging shared platform layer.

### Chicago Batch Center

Chicago is the most complete batch/static city today.

Current Chicago batch story:

1. CTA GTFS static download
2. raw GTFS files extracted into `data/raw/gtfs/`
3. Spark cleaning jobs under `jobs/spark/`
4. Spark analytics jobs under `jobs/spark/analytics/`
5. optional Snowflake loading via `jobs/load/load_to_snowflake.py`
6. legacy Streamlit analytics dashboard in `dashboard/app.py`

### Shared Live Platform Center

The newer live stack is shared across cities and is now the most actively evolving part of the repo.

Core layers:

- normalization contracts in `src/live/`
- realtime jobs in `jobs/realtime/`
- serving API in `dashboard/live_api.py`
- modern frontend in `dashboard/web/`
- orchestration via `scripts/live.sh`

### Emerging Multi-City Direction

The intended platform direction is:

- Chicago batch GTFS + OSM
- Boston batch GTFS + OSM
- Boston live
- Chicago live where practical
- one shared dashboard experience

That direction is no longer only aspirational. The city-aware batch path now runs end to end for both Chicago and Boston, and both cities have been loaded into the new `BATCH_*` Snowflake tables.

## Top-Level Directory Roles

- `config/`
  - project configuration files such as `settings.yaml`

- `dags/`
  - Airflow orchestration for batch work
  - `multi_city_batch_pipeline.py` is now the preferred daily batch DAG

- `dashboard/`
  - `app.py` is the legacy Streamlit batch dashboard
  - `live_api.py` is the FastAPI service for the live stack
  - `web/` is the React + deck.gl + MapLibre live dashboard

- `data/`
  - local raw, staging, and processed data outputs
  - current processed folders primarily reflect the Chicago batch pipeline

- `docs/`
  - project documentation
  - should be treated as helpful context, but code remains the source of truth

- `jobs/`
  - ingestion, Spark batch transforms, live jobs, load jobs, and validation hooks

- `scripts/`
  - local developer run scripts
  - `live.sh` is the main entrypoint for the live runtime

- `sql/`
  - DDL, queries, and validation SQL

- `src/`
  - reusable Python modules
  - `src/live/` is currently the most important shared subsystem

- `tests/`
  - lightweight current test suite
  - still needs expansion

## Current Architectural Split

### Batch

Batch is still Chicago-first and Spark-first.

Important batch outputs currently exist under:

- `data/processed/clean/gtfs/`
- `data/processed/analytics/`

These power:

- local parquet inspection
- Streamlit visualizations
- optional Snowflake loading

### Live

Live is now city-aware and shared.

Shared live contract:

- `LiveVehicleState` in `src/live/models.py`

Shared live infrastructure:

- Kafka
- Flink
- Redis
- FastAPI
- React + deck.gl + MapLibre

City-specific live source adapters:

- Boston:
  - `src/live/mbta.py`
- Chicago:
  - `src/live/cta.py`

### Emerging City-Aware Batch Path

There is now a newer batch foundation for multi-city GTFS + OSM work.

That path is centered around:

- `src/common/config.py`
- `src/common/constants.py`
- `src/common/paths.py`
- `src/common/run_metadata.py`
- `jobs/pipeline/run_city_batch_pipeline.py`
- `jobs/ingestion/download_osm.py`
- `jobs/spark/clean_gtfs_city.py`
- `jobs/spark/clean_osm_city.py`
- `jobs/spark/build_city_batch_analytics.py`

The new path writes city-scoped parquet outputs under:

- `data/raw/gtfs/{city}`
- `data/raw/osm/{city}`
- `data/processed/{city}/clean/...`
- `data/processed/{city}/analytics/...`

This lets Boston and Chicago share one batch pattern without requiring a major repo reorganization.

### Batch Idempotency And Resume Model

The city-aware batch path now includes lightweight run-manifest and checkpoint behavior.

Key ideas:

- every stage records a manifest JSON
- manifests include row counts and path stats where available
- rerunning the same `run_id` skips completed stages if outputs still exist
- a full city batch run can be resumed through `jobs/pipeline/run_city_batch_pipeline.py`

This is not a fully transactional warehouse pipeline, but it is intentionally designed to be:

- rerunnable
- practically idempotent for local development
- resumable after stage-level failures

That makes it a much better fit for the current project than a brittle one-shot script chain.

### Batch Scheduling Layer

The city-aware batch path now has two orchestration entrypoints:

- Airflow:
  - `dags/multi_city_batch_pipeline.py`
- manual shell entrypoint:
  - `scripts/run_batch_pipeline.sh`

The intended pattern is:

- Airflow runs Chicago and Boston once per day
- a shared Snowflake load follows both city runs
- local/manual runs use the same underlying Python batch pipeline, not a different code path

## Design Intent

The repo is intentionally not being heavily reorganized right now.

The preferred pattern is:

- preserve the current top-level structure
- extend it with city-aware modules
- keep serving/frontend contracts stable
- avoid broad refactors during active presentation work

That is why this repo does not currently use a full top-level split like:

- `cities/chicago/...`
- `cities/boston/...`

The modularity is instead expressed through:

- city-scoped API routes
- city-scoped Redis keys
- city-scoped Kafka topics
- city-specific source clients
- a shared live schema

## Most Important Modules For New Contributors

If you need to understand the current architecture quickly, start with:

1. `src/live/`
2. `jobs/realtime/`
3. `dashboard/live_api.py`
4. `dashboard/web/`
5. `scripts/live.sh`
6. `jobs/spark/analytics/`

That sequence gives the best picture of how the repo works today.
