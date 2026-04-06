# Source (`src/`)

Reusable Python modules shared across the pipeline. Imported by jobs, scripts, and tests.

---

## Contents

### `snowflake/`

Snowflake connection management.

- `connector.py` -- Provides `get_snowflake_connection()` using key-pair authentication. Reads credentials from environment variables via `.env`.

### `common/`

Shared utilities and configuration.

- `config.py` -- Configuration loader (placeholder)
- `paths.py` -- Centralized path definitions (placeholder)
- `constants.py` -- Project-wide constants (placeholder)

### `gtfs/`

GTFS data handling modules.

- `readers.py` -- Functions for reading GTFS CSV files (placeholder)
- `transformers.py` -- Cleaning and transformation logic for GTFS datasets (placeholder)

### `osm/`

OpenStreetMap data handling modules.

- `readers.py` -- Functions for reading OSM data (placeholder)
- `transformers.py` -- Cleaning and transformation logic for OSM datasets

### `analytics/`

- `metrics.py` -- Reusable analytics metric definitions (placeholder)

---

## Usage

Import modules from the project root:

```python
from src.snowflake.connector import get_snowflake_connection
from src.gtfs.transformers import clean_stops
```

## Dependencies

- `snowflake-connector-python`, `cryptography`, `python-dotenv`
- Requires `.env` file for Snowflake connector
