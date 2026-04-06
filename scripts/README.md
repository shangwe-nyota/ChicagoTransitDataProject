# Scripts

Utility and pipeline scripts for local development and setup.

---

## Contents

- `run_local_pipeline.sh` -- Runs the full batch pipeline locally (ingestion, cleaning, analytics). Skips the Snowflake load step by default. Prints instructions for loading to Snowflake and launching the dashboard at the end.

- `test_snowflake_connection.py` -- Verifies that the Snowflake connection works by querying the current user, role, database, and schema.

- `setup_env.sh` -- Environment setup script (placeholder).

- `reset_data_dirs.sh` -- Resets data directories (placeholder).

---

## How to Run

### Full local pipeline

```bash
bash scripts/run_local_pipeline.sh
```

### Test Snowflake connection

```bash
python -m scripts.test_snowflake_connection
```

## Dependencies

- `run_local_pipeline.sh` requires Python 3 and PySpark
- `test_snowflake_connection.py` requires `.env` with Snowflake credentials
