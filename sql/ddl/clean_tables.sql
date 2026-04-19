USE ROLE TRAINING_ROLE;
USE WAREHOUSE MARMOT_WH;
USE DATABASE MARMOT_DB;
CREATE SCHEMA IF NOT EXISTS CHICAGO_TRANSIT;
USE SCHEMA CHICAGO_TRANSIT;

CREATE OR REPLACE TABLE CLEAN_GTFS_STOPS (
    stop_id STRING,
    stop_name STRING,
    stop_lat FLOAT,
    stop_lon FLOAT,
    location_type INTEGER,
    parent_station STRING
);

CREATE OR REPLACE TABLE CLEAN_GTFS_ROUTES (
    route_id STRING,
    route_short_name STRING,
    route_long_name STRING,
    route_type INTEGER
);

CREATE OR REPLACE TABLE CLEAN_GTFS_TRIPS (
    route_id STRING,
    service_id STRING,
    trip_id STRING,
    direction_id INTEGER,
    shape_id STRING,
    direction STRING,
    wheelchair_accessible INTEGER,
    schd_trip_id STRING
);

CREATE OR REPLACE TABLE CLEAN_GTFS_STOP_TIMES (
    trip_id STRING,
    arrival_time STRING,
    departure_time STRING,
    stop_id STRING,
    stop_sequence INTEGER
);

CREATE OR REPLACE TABLE CLEAN_GTFS_SHAPES (
    shape_id STRING,
    shape_pt_lat FLOAT,
    shape_pt_lon FLOAT,
    shape_pt_sequence INTEGER
);

CREATE OR REPLACE TABLE BATCH_GTFS_STOPS (
    city STRING,
    stop_id STRING,
    stop_name STRING,
    stop_lat FLOAT,
    stop_lon FLOAT,
    location_type INTEGER,
    parent_station STRING
);

CREATE OR REPLACE TABLE BATCH_GTFS_ROUTES (
    city STRING,
    route_id STRING,
    route_short_name STRING,
    route_long_name STRING,
    route_type INTEGER
);

CREATE OR REPLACE TABLE BATCH_GTFS_TRIPS (
    city STRING,
    route_id STRING,
    service_id STRING,
    trip_id STRING,
    direction_id INTEGER,
    shape_id STRING,
    direction STRING,
    wheelchair_accessible INTEGER,
    schd_trip_id STRING
);

CREATE OR REPLACE TABLE BATCH_GTFS_STOP_TIMES (
    city STRING,
    trip_id STRING,
    arrival_time STRING,
    departure_time STRING,
    stop_id STRING,
    stop_sequence INTEGER
);

CREATE OR REPLACE TABLE BATCH_GTFS_SHAPES (
    city STRING,
    shape_id STRING,
    shape_pt_lat FLOAT,
    shape_pt_lon FLOAT,
    shape_pt_sequence INTEGER
);

CREATE OR REPLACE TABLE BATCH_OSM_ROADS (
    city STRING,
    osm_id STRING,
    name STRING,
    highway STRING,
    start_lat FLOAT,
    start_lon FLOAT,
    end_lat FLOAT,
    end_lon FLOAT,
    mid_lat FLOAT,
    mid_lon FLOAT,
    length_m FLOAT
);

CREATE OR REPLACE TABLE BATCH_OSM_POIS (
    city STRING,
    osm_id STRING,
    name STRING,
    tag_key STRING,
    tag_value STRING,
    poi_category STRING,
    poi_group STRING,
    lat FLOAT,
    lon FLOAT
);
