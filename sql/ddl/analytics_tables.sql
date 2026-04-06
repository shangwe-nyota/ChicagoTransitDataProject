USE ROLE TRAINING_ROLE;
USE WAREHOUSE MARMOT_WH;
USE DATABASE MARMOT_DB;
CREATE SCHEMA IF NOT EXISTS CHICAGO_TRANSIT;
USE SCHEMA CHICAGO_TRANSIT;


CREATE OR REPLACE TABLE ANALYTICS_STOP_ACTIVITY (
    stop_id STRING,
    trip_count INTEGER
);

CREATE OR REPLACE TABLE ANALYTICS_STOP_ACTIVITY_ENRICHED (
    stop_id STRING,
    stop_name STRING,
    stop_lat FLOAT,
    stop_lon FLOAT,
    trip_count INTEGER,
    location_type INTEGER,
    parent_station STRING
);

CREATE OR REPLACE TABLE ANALYTICS_ROUTE_ACTIVITY (
    route_id STRING,
    route_short_name STRING,
    route_long_name STRING,
    route_type INTEGER,
    stop_event_count INTEGER,
    distinct_trip_count INTEGER,
    distinct_stop_count INTEGER
);

CREATE OR REPLACE TABLE ANALYTICS_STOP_ACTIVITY_BY_ROUTE (
    route_id STRING,
    route_short_name STRING,
    route_long_name STRING,
    stop_id STRING,
    stop_name STRING,
    stop_lat FLOAT,
    stop_lon FLOAT,
    trip_count INTEGER
);

CREATE OR REPLACE TABLE ANALYTICS_ROUTE_SHAPES (
    route_id STRING,
    route_short_name STRING,
    route_long_name STRING,
    route_type INTEGER,
    shape_id STRING,
    shape_pt_sequence INTEGER,
    shape_pt_lat FLOAT,
    shape_pt_lon FLOAT
);

-- =========================
-- GTFS + OSM ANALYTICS TABLES
-- =========================

CREATE OR REPLACE TABLE ANALYTICS_STOP_POI_ACCESS (
    stop_id STRING,
    stop_name STRING,
    stop_lat FLOAT,
    stop_lon FLOAT,
    poi_count_within_400m INTEGER,
    nearest_school_m FLOAT,
    nearest_hospital_m FLOAT,
    amenity_types STRING
);

CREATE OR REPLACE TABLE ANALYTICS_TRANSIT_ROAD_COVERAGE (
    highway STRING,
    total_road_segments INTEGER,
    road_segments_near_transit INTEGER,
    coverage_pct FLOAT,
    total_length_km FLOAT,
    covered_length_km FLOAT
);