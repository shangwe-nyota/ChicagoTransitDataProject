# Docs

Project documentation for the Chicago Transit Data Project.

---

## Contents

- `architecture.md` -- System architecture overview, describing the batch pipeline components and how they connect (placeholder).

- `data_dictionary.md` -- Definitions for all tables and columns across the RAW, CLEAN, and ANALYTICS layers in Snowflake (placeholder).

- `pipeline_flow.md` -- Step-by-step description of data flow through the pipeline: ingestion, cleaning, analytics, and loading (placeholder).

---

## Summary

The pipeline follows a three-layer architecture:

1. **RAW** -- Source-of-truth landing zone for GTFS and OSM data
2. **CLEAN** -- Processed and validated datasets
3. **ANALYTICS** -- Aggregated insights (stop activity, route activity, POI access, road coverage)

For a high-level overview, see the root `README.md`. For the Airflow DAG structure, see `dags/README.md`.
