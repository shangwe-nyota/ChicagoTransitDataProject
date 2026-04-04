USE ROLE TRAINING_ROLE;
USE WAREHOUSE MARMOT_WH;
USE DATABASE MARMOT_DB;

CREATE SCHEMA IF NOT EXISTS CHICAGO_TRANSIT;
USE SCHEMA CHICAGO_TRANSIT;

-- =========================
-- RAW GTFS TABLES
-- Source-of-truth landing layer for CTA GTFS text files
-- NOTE: DO NOT CHANGE IF NOT EXISTS so raw/source data is not accidentally wiped -Shangwe
-- =========================

CREATE TABLE IF NOT EXISTS RAW_GTFS_AGENCY (
    agency_id STRING,
    agency_name STRING,
    agency_url STRING,
    agency_timezone STRING,
    agency_lang STRING,
    agency_phone STRING,
    agency_fare_url STRING
);

CREATE TABLE IF NOT EXISTS RAW_GTFS_CALENDAR (
    service_id STRING,
    monday INTEGER,
    tuesday INTEGER,
    wednesday INTEGER,
    thursday INTEGER,
    friday INTEGER,
    saturday INTEGER,
    sunday INTEGER,
    start_date STRING,
    end_date STRING
);

CREATE TABLE IF NOT EXISTS RAW_GTFS_CALENDAR_DATES (
    service_id STRING,
    date STRING,
    exception_type INTEGER
);

CREATE TABLE IF NOT EXISTS RAW_GTFS_FREQUENCIES (
    trip_id STRING,
    start_time STRING,
    end_time STRING,
    headway_secs INTEGER
);

CREATE TABLE IF NOT EXISTS RAW_GTFS_ROUTES (
    agency_id STRING,
    route_id STRING,
    route_short_name STRING,
    route_long_name STRING,
    route_type INTEGER,
    route_url STRING,
    route_color STRING,
    route_text_color STRING
);

CREATE TABLE IF NOT EXISTS RAW_GTFS_SHAPES (
    shape_id STRING,
    shape_pt_lat FLOAT,
    shape_pt_lon FLOAT,
    shape_pt_sequence INTEGER,
    shape_dist_traveled FLOAT
);

CREATE TABLE IF NOT EXISTS RAW_GTFS_STOP_TIMES (
    trip_id STRING,
    arrival_time STRING,
    departure_time STRING,
    stop_id STRING,
    stop_sequence INTEGER,
    stop_headsign STRING,
    pickup_type INTEGER,
    shape_dist_traveled FLOAT
);

CREATE TABLE IF NOT EXISTS RAW_GTFS_STOPS (
    stop_id STRING,
    stop_code STRING,
    stop_name STRING,
    stop_desc STRING,
    stop_lat FLOAT,
    stop_lon FLOAT,
    location_type INTEGER,
    parent_station STRING,
    wheelchair_boarding INTEGER
);

CREATE TABLE IF NOT EXISTS RAW_GTFS_TRANSFERS (
    from_stop_id STRING,
    to_stop_id STRING,
    transfer_type INTEGER
);

CREATE TABLE IF NOT EXISTS RAW_GTFS_TRIPS (
    route_id STRING,
    service_id STRING,
    trip_id STRING,
    direction_id INTEGER,
    block_id STRING,
    shape_id STRING,
    direction STRING,
    wheelchair_accessible INTEGER,
    schd_trip_id STRING
);