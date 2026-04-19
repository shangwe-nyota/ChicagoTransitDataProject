# Pipeline Flow

## 1. Chicago Batch Flow

Current implemented Chicago batch flow:

1. `jobs/ingestion/download_gtfs.py`
   - downloads CTA GTFS static data
   - extracts raw files into `data/raw/gtfs/`
2. Spark cleaning jobs
   - `jobs/spark/clean_gtfs.py`
   - `jobs/spark/clean_gtfs_routes.py`
   - `jobs/spark/clean_gtfs_trips.py`
   - `jobs/spark/clean_gtfs_stop_times.py`
   - `jobs/spark/clean_gtfs_shapes.py`
3. Spark analytics jobs
   - `jobs/spark/analytics/stop_activity.py`
   - `jobs/spark/analytics/join_stop_activity_with_stops.py`
   - `jobs/spark/analytics/route_activity.py`
   - `jobs/spark/analytics/stop_activity_by_route.py`
   - `jobs/spark/analytics/route_shapes.py`
4. outputs written to `data/processed/...`
5. optional Snowflake loading through `jobs/load/load_to_snowflake.py`
6. visualization through the legacy Streamlit dashboard

## 2. Boston Live Flow

Preferred Boston live flow:

1. MBTA vehicle feed
2. `jobs/realtime/mbta_poll_to_kafka.py`
3. Kafka raw topic:
   - `transit.live.raw.boston.vehicles`
4. `jobs/realtime/flink_vehicle_latest_job.py boston`
5. Kafka latest topic:
   - `transit.live.latest.boston.vehicles`
6. `jobs/realtime/kafka_latest_to_redis.py --city boston`
7. Redis latest-state store
8. `dashboard/live_api.py`
9. `dashboard/web/`

Fallback Boston live flow:

1. MBTA vehicle feed
2. `jobs/realtime/mbta_poll_to_redis.py`
3. Redis
4. FastAPI
5. React dashboard

## 3. Chicago Live Flow

Preferred Chicago live flow:

1. CTA bus and optional train feeds
2. `jobs/realtime/cta_poll_to_kafka.py`
3. Kafka raw topic:
   - `transit.live.raw.chicago.vehicles`
4. `jobs/realtime/flink_vehicle_latest_job.py chicago`
5. Kafka latest topic:
   - `transit.live.latest.chicago.vehicles`
6. `jobs/realtime/kafka_latest_to_redis.py --city chicago`
7. Redis latest-state store
8. `dashboard/live_api.py`
9. `dashboard/web/`

Fallback Chicago live flow:

1. CTA live feed
2. `jobs/realtime/cta_poll_to_redis.py`
3. Redis
4. FastAPI
5. React dashboard

Important current caveat:

- Chicago buses work
- Chicago trains are blocked by the currently configured CTA train key

## 4. Shared Live Serving Flow

Regardless of city, the frontend flow is:

1. `GET /api/live/{city}/vehicles` for the initial snapshot
2. `WS /ws/live/{city}` for live updates
3. render markers in `deck.gl`
4. show them over a MapLibre basemap

The frontend is intentionally decoupled from Kafka and Flink.

## 5. Local Orchestration Flow

The preferred local command is:

```bash
bash scripts/live.sh all
```

This currently means:

1. start Redis
2. start Kafka
3. create Boston and Chicago topics
4. start per-city Flink latest-state jobs
5. start per-city Kafka latest-to-Redis consumers
6. start per-city upstream producers
7. start FastAPI
8. start the React dashboard

Useful URLs:

- `http://127.0.0.1:5173`
- `http://127.0.0.1:8000/api/live/boston/health`
- `http://127.0.0.1:8000/api/live/chicago/health`

## 6. Near-Term Planned Batch Flow Expansion

The next major batch work is expected to be:

1. Boston GTFS batch support
2. Boston OSM integration
3. Chicago OSM integration completion
4. new spatial analytics that combine GTFS + OSM
5. migration of older batch visuals into the newer dashboard stack

That batch + OSM work is the highest-value next engineering area because it will create richer dashboard queries and a stronger presentation story.

## 7. New City-Aware Batch Foundation

The repo now also has a newer city-aware batch path alongside the legacy Chicago-only jobs.

Key files:

- `jobs/ingestion/download_gtfs.py`
  - preserves the legacy Chicago raw path by default
  - also supports `--city chicago` and `--city boston` for city-scoped GTFS downloads

- `jobs/ingestion/download_osm.py`
  - downloads roads and POIs into `data/raw/osm/{city}/`

- `jobs/spark/clean_gtfs_city.py`
  - writes cleaned GTFS parquet to `data/processed/{city}/clean/gtfs/...`

- `jobs/spark/clean_osm_city.py`
  - writes cleaned OSM parquet to `data/processed/{city}/clean/osm/...`

- `jobs/spark/build_city_batch_analytics.py`
  - writes city-scoped analytics parquet to `data/processed/{city}/analytics/...`

Current city-aware GTFS + OSM analytics outputs include:

- `stop_poi_access`
- `busiest_stops_with_poi_context`
- `route_poi_access`
- `transit_road_coverage`

This is the intended path for Boston batch work and for finishing Chicago GTFS + OSM in a reusable way.
