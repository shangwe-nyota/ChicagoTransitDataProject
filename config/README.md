# Config

Configuration files for the Chicago Transit Data Project.

---

## Contents

- `settings.yaml` -- Project-level settings (placeholder). Intended for centralizing configuration such as data paths, Snowflake parameters, and pipeline options.

---

## Usage

Configuration is currently handled via environment variables in a `.env` file at the project root (for Snowflake credentials) and hardcoded paths in individual scripts. The `settings.yaml` file is reserved for future use.

See the root README for the required `.env` variables:

```
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PRIVATE_KEY_FILE=...
SNOWFLAKE_ROLE=TRAINING_ROLE
SNOWFLAKE_WAREHOUSE=...
SNOWFLAKE_DATABASE=...
SNOWFLAKE_SCHEMA=...
```

## Dependencies

- `python-dotenv` (for loading `.env`)
