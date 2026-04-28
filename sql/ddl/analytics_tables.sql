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

CREATE OR REPLACE TABLE BATCH_STOP_ACTIVITY (
    city STRING,
    stop_id STRING,
    trip_count INTEGER,
    avg_daily_stop_events FLOAT
);

CREATE OR REPLACE TABLE BATCH_STOP_ACTIVITY_ENRICHED (
    city STRING,
    stop_id STRING,
    stop_name STRING,
    stop_lat FLOAT,
    stop_lon FLOAT,
    trip_count INTEGER,
    avg_daily_stop_events FLOAT,
    location_type INTEGER,
    parent_station STRING
);

CREATE OR REPLACE TABLE BATCH_ROUTE_ACTIVITY (
    city STRING,
    route_id STRING,
    route_short_name STRING,
    route_long_name STRING,
    route_type INTEGER,
    stop_event_count INTEGER,
    distinct_trip_count INTEGER,
    distinct_stop_count INTEGER,
    avg_daily_stop_events FLOAT
);

CREATE OR REPLACE TABLE BATCH_STOP_ACTIVITY_BY_ROUTE (
    city STRING,
    route_id STRING,
    route_short_name STRING,
    route_long_name STRING,
    route_type INTEGER,
    stop_id STRING,
    stop_name STRING,
    stop_lat FLOAT,
    stop_lon FLOAT,
    trip_count INTEGER,
    avg_daily_stop_events FLOAT
);

CREATE OR REPLACE TABLE BATCH_ROUTE_SHAPES (
    city STRING,
    route_id STRING,
    route_short_name STRING,
    route_long_name STRING,
    route_type INTEGER,
    shape_id STRING,
    shape_pt_sequence INTEGER,
    shape_pt_lat FLOAT,
    shape_pt_lon FLOAT
);

CREATE OR REPLACE TABLE BATCH_STOP_POI_ACCESS (
    city STRING,
    stop_id STRING,
    stop_name STRING,
    stop_lat FLOAT,
    stop_lon FLOAT,
    poi_count_within_400m INTEGER,
    food_poi_count_within_400m INTEGER,
    critical_service_poi_count_within_400m INTEGER,
    park_poi_count_within_400m INTEGER,
    nearest_school_m FLOAT,
    nearest_hospital_m FLOAT,
    nearest_grocery_m FLOAT,
    nearest_park_m FLOAT,
    poi_categories STRING
);

CREATE OR REPLACE TABLE BATCH_BUSIEST_STOPS_WITH_POI_CONTEXT (
    city STRING,
    stop_id STRING,
    stop_name STRING,
    stop_lat FLOAT,
    stop_lon FLOAT,
    trip_count INTEGER,
    avg_daily_stop_events FLOAT,
    poi_count_within_400m INTEGER,
    food_poi_count_within_400m INTEGER,
    critical_service_poi_count_within_400m INTEGER,
    park_poi_count_within_400m INTEGER,
    nearest_school_m FLOAT,
    nearest_hospital_m FLOAT,
    nearest_grocery_m FLOAT,
    nearest_park_m FLOAT,
    poi_categories STRING
);

CREATE OR REPLACE TABLE BATCH_ROUTE_POI_ACCESS (
    city STRING,
    route_id STRING,
    route_short_name STRING,
    route_long_name STRING,
    route_type INTEGER,
    stop_count INTEGER,
    total_poi_access INTEGER,
    avg_poi_access_per_stop FLOAT,
    max_poi_access_at_stop INTEGER,
    stops_near_hospital INTEGER,
    stops_near_grocery INTEGER,
    stops_near_park INTEGER
);

CREATE OR REPLACE TABLE BATCH_TRANSIT_ROAD_COVERAGE (
    city STRING,
    highway STRING,
    total_road_segments INTEGER,
    road_segments_near_transit INTEGER,
    coverage_pct FLOAT,
    total_length_km FLOAT,
    covered_length_km FLOAT
);
