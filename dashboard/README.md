# Dashboard

Interactive Streamlit dashboard for visualizing CTA transit analytics.

---

## Contents

- `app.py` -- Main Streamlit application with three views:
  1. **Overall busiest stops** -- Map and table of CTA stops ranked by scheduled stop events, with color-coded activity levels
  2. **Explore a bus route** -- Select a route and view its stops on an interactive map
  3. **Transit + POI Access** -- Shows nearby amenities (schools, hospitals, etc.) within 400m of each stop, plus road coverage stats

The dashboard uses `pydeck` for interactive map rendering with scatter plot layers.

---

## How to Run

```bash
streamlit run dashboard/app.py
```

The dashboard reads Parquet files directly from `data/processed/analytics/`. No Snowflake connection is required.

---

## Data Sources

The dashboard reads from these local Parquet directories:

- `data/processed/analytics/stop_activity_enriched`
- `data/processed/analytics/stop_activity_by_route`
- `data/processed/analytics/stop_poi_access`
- `data/processed/analytics/transit_road_coverage`

Run the full pipeline (`scripts/run_local_pipeline.sh`) before launching the dashboard.

## Dependencies

- `streamlit`, `pydeck`, `pandas`
