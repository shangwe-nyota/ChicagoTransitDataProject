# SQL

Snowflake SQL files for table definitions, analytics queries, and data validation.

---

## Contents

### `ddl/`

Table creation statements executed during the Snowflake load step.

- `raw_tables.sql` -- Creates RAW layer tables (source-of-truth landing zone for GTFS and OSM data)
- `clean_tables.sql` -- Creates CLEAN layer tables (processed datasets)
- `analytics_tables.sql` -- Creates ANALYTICS layer tables (aggregated insights)

All tables use `CREATE TABLE IF NOT EXISTS` to avoid accidental data loss.

### `queries/`

Ad-hoc analysis queries for exploring the loaded data.

- `busiest_stops.sql` -- Finds stops with the most scheduled stop events
- `top_routes.sql` -- Ranks routes by activity
- `neighborhood_coverage.sql` -- Analyzes transit coverage across areas

### `validation/`

- `data_quality_checks.sql` -- SQL-based data quality assertions (row counts, null checks, etc.)

---

## How to Run

These SQL files are designed to run against Snowflake. The DDL files are executed automatically by `jobs/load/load_to_snowflake.py`. Query and validation files can be run manually in a Snowflake worksheet or via the Snowflake CLI.

```sql
USE ROLE TRAINING_ROLE;
USE WAREHOUSE MARMOT_WH;
USE DATABASE MARMOT_DB;
USE SCHEMA CHICAGO_TRANSIT;
```

## Dependencies

- Snowflake account with `TRAINING_ROLE`, `MARMOT_WH`, and `MARMOT_DB` configured
- Data must be loaded via the pipeline before queries return results
