# Tests

Test suite for the Chicago Transit Data Project, using pytest and PySpark.

---

## Contents

### Test files

- `test_gtfs_transformers.py` -- Tests for GTFS data cleaning and transformation logic
- `test_transformations.py` -- Tests for general data transformation functions
- `test_ddl.py` -- Tests that DDL SQL files parse and execute correctly
- `test_snowflake_loading.py` -- Tests for the Snowflake loading pipeline

### Configuration

- `conftest.py` -- Shared pytest fixtures:
  - `spark` -- Session-scoped local SparkSession (`local[1]`)
  - `tmp_dir` -- Temporary directory, cleaned up after each test
  - `fixture_dir` -- Path to the `fixtures/` directory

### Test fixtures (`fixtures/`)

Sample CSV files used by tests:

- `stops.csv`, `routes.csv`, `trips.csv`, `stop_times.csv`, `shapes.csv`
- `osm_roads.csv`, `osm_pois.csv`

---

## How to Run

```bash
# Run all tests
pytest tests/

# Run a specific test file
pytest tests/test_gtfs_transformers.py

# Run with verbose output
pytest tests/ -v
```

## Dependencies

- `pytest`, `pyspark`
- Tests run with a local Spark instance (no cluster required)
