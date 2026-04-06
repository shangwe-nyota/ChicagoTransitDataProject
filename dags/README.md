# DAGs

Apache Airflow DAG definitions for orchestrating the Chicago Transit Data Pipeline.

---

## Contents

- `chicago_static_pipeline.py` -- End-to-end batch pipeline DAG with the following stages:

### Stage 1: Ingestion

Downloads GTFS and OSM data in parallel.

### Stage 2: Cleaning

Runs PySpark cleaning jobs for GTFS (stops, routes, trips, shapes, stop_times) and OSM (roads, POIs). GTFS and OSM cleaning tasks run in parallel after their respective ingestion tasks complete.

### Stage 3: Analytics

Builds aggregated datasets: stop activity, route activity, stop activity by route, enriched stop activity, route shapes, stop-POI access, and transit road coverage. Each analytics task depends on the specific cleaning tasks it needs.

### Stage 4: Load to Snowflake

Loads all clean and analytics Parquet files into Snowflake. Runs after all analytics tasks complete.

---

## How to Run

Place this directory in your Airflow `dags_folder` or symlink it:

```bash
ln -s /path/to/ChicagoTransitDataProject/dags ~/airflow/dags/chicago_transit
```

The DAG is configured with `schedule_interval=None` (manual trigger only) and `catchup=False`.

## Dependencies

- Apache Airflow
- All pipeline jobs must be accessible at the paths defined in `PROJECT_DIR`
