# Jobs

Pipeline jobs for the Chicago Transit Data Project. Each subdirectory handles one stage of the batch pipeline.

---

## Contents

### `ingestion/`

Scripts that download raw data from external sources.

- `download_gtfs.py` -- Downloads and unzips CTA GTFS static data from transitchicago.com
- `download_osm.py` -- Downloads OpenStreetMap data for Chicago (roads, POIs)

### `spark/`

PySpark jobs that clean raw data and build analytics datasets. Output is written as partitioned Parquet files under `data/processed/`.

**Cleaning jobs:**

- `clean_gtfs.py` -- Cleans GTFS stops
- `clean_gtfs_routes.py`, `clean_gtfs_trips.py`, `clean_gtfs_shapes.py`, `clean_gtfs_stop_times.py`
- `clean_osm_roads.py`, `clean_osm_pois.py`

**Analytics jobs** (`spark/analytics/`):

- `stop_activity.py` -- Counts scheduled stop events per stop
- `route_activity.py` -- Aggregates activity per route
- `stop_activity_by_route.py` -- Stop-level activity broken down by route
- `join_stop_activity_with_stops.py` -- Enriches stop activity with location data
- `route_shapes.py` -- Joins route geometry from shapes
- `stop_poi_access.py` -- Counts nearby POIs (schools, hospitals, etc.) within 400m of each stop
- `transit_road_coverage.py` -- Measures CTA coverage of Chicago road segments

### `load/`

- `load_to_snowflake.py` -- Runs DDL from `sql/ddl/`, then loads all clean and analytics Parquet files into Snowflake

### `validation/`

- `validate_data.py` -- Data quality checks (placeholder)

---

## How to Run

Run the full pipeline in order:

```bash
python3 jobs/ingestion/download_gtfs.py
python3 jobs/ingestion/download_osm.py
python3 jobs/spark/clean_gtfs.py
# ... (see scripts/run_local_pipeline.sh for the full sequence)
python3 -m jobs.load.load_to_snowflake
```

## Dependencies

- `pyspark`, `requests`, `pandas`, `snowflake-connector-python`
- Snowflake load requires `.env` configuration (see root README)
- Raw data must exist under `data/raw/` before running Spark jobs
