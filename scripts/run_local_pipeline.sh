#!/usr/bin/env bash
set -euo pipefail

echo "=========================================="
echo " Chicago Transit Data Pipeline (Local)"
echo "=========================================="

# --- STEP 1: INGESTION ---
echo ""
echo "[1/4] Downloading GTFS data..."
python3 jobs/ingestion/download_gtfs.py

echo ""
echo "[1/4] Downloading OSM data..."
python3 jobs/ingestion/download_osm.py

# --- STEP 2: CLEAN ---
echo ""
echo "[2/4] Cleaning GTFS data (Spark)..."
python3 jobs/spark/clean_gtfs.py
python3 jobs/spark/clean_gtfs_routes.py
python3 jobs/spark/clean_gtfs_trips.py
python3 jobs/spark/clean_gtfs_shapes.py
python3 jobs/spark/clean_gtfs_stop_times.py

echo ""
echo "[2/4] Cleaning OSM data (Spark)..."
python3 jobs/spark/clean_osm_roads.py
python3 jobs/spark/clean_osm_pois.py

# --- STEP 3: ANALYTICS ---
echo ""
echo "[3/4] Building GTFS analytics..."
python3 jobs/spark/analytics/stop_activity.py
python3 jobs/spark/analytics/route_activity.py
python3 jobs/spark/analytics/stop_activity_by_route.py
python3 jobs/spark/analytics/join_stop_activity_with_stops.py
python3 jobs/spark/analytics/route_shapes.py

echo ""
echo "[3/4] Building GTFS + OSM analytics..."
python3 jobs/spark/analytics/stop_poi_access.py
python3 jobs/spark/analytics/transit_road_coverage.py

# --- STEP 4: LOAD TO SNOWFLAKE (optional) ---
echo ""
echo "[4/4] Skipping Snowflake load (requires .env config)."
echo "      To load, run: python3 -m jobs.load.load_to_snowflake"

echo ""
echo "=========================================="
echo " Pipeline complete!"
echo " Launch dashboard: streamlit run dashboard/app.py"
echo "=========================================="
