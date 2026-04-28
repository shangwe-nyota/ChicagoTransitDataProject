# Pipeline Flow

## 1. Multi-City Batch Flow

Current preferred batch flow:

Please use the DAGs defined with Airflow. There are manual scripts as well if needed for one-off jobs.

1. `dags/multi_city_batch_pipeline.py` or `scripts/run_batch_pipeline.sh`
2. `jobs/pipeline/run_city_batch_pipeline.py --city {city}`
3. `jobs/ingestion/download_gtfs.py --city {city}`
4. `jobs/ingestion/download_osm.py --city {city}`
5. `jobs/spark/clean_gtfs_city.py --city {city}`
6. `jobs/spark/clean_osm_city.py --city {city}`
7. `jobs/spark/build_city_batch_analytics.py --city {city}`
8.  Snowflake load through `python -m jobs.load.load_to_snowflake`
9. batch serving through FastAPI and the shared React dashboard

Current supported cities:

- `chicago`
- `boston`

## 2. Shared Live Serving Flow

Regardless of city, the frontend flow is:

1. `GET /api/live/{city}/vehicles` for the initial snapshot
2. `WS /ws/live/{city}` for live updates
3. render markers in `deck.gl`
4. show them over a MapLibre basemap

The frontend is intentionally decoupled from Kafka and Flink.

## 3. Local Orchestration Flow

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

## 4. Shared Dashboard Flow

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

## 5. Batch Analytics Outputs

Additional manual trigger commands:

```bash
bash scripts/run_batch_pipeline.sh all --load-snowflake
```

Single-city examples:

```bash
bash scripts/run_batch_pipeline.sh chicago
bash scripts/run_batch_pipeline.sh boston --load-snowflake
```

## 6. Batch Run Metadata And Checkpointing

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

## 7. Airflow Batch Orchestration

The main batch DAG is:

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
