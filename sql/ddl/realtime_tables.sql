USE ROLE TRAINING_ROLE;
USE WAREHOUSE MARMOT_WH;
USE DATABASE MARMOT_DB;
CREATE SCHEMA IF NOT EXISTS CHICAGO_TRANSIT;
USE SCHEMA CHICAGO_TRANSIT;

-- =========================
-- REALTIME STREAMING TABLES
-- Landing tables for Kafka/Flink output
-- =========================

CREATE TABLE IF NOT EXISTS REALTIME_VEHICLE_POSITIONS (
    poll_timestamp STRING,
    station_id STRING,
    station_name STRING,
    stop_id STRING,
    stop_description STRING,
    run_number STRING,
    route_id STRING,
    destination_stop STRING,
    destination_name STRING,
    direction STRING,
    prediction_time STRING,
    arrival_time STRING,
    is_approaching BOOLEAN,
    is_scheduled BOOLEAN,
    is_delayed BOOLEAN,
    is_fault BOOLEAN,
    latitude STRING,
    longitude STRING,
    heading STRING,
    inserted_at STRING
);

CREATE TABLE IF NOT EXISTS REALTIME_DELAY_METRICS (
    route_id STRING,
    delayed_count INTEGER,
    total_count INTEGER,
    delay_ratio FLOAT,
    window_minutes INTEGER,
    inserted_at STRING
);

CREATE TABLE IF NOT EXISTS REALTIME_VEHICLE_COUNTS (
    route_id STRING,
    vehicle_count INTEGER,
    event_count INTEGER,
    window_minutes INTEGER,
    inserted_at STRING
);
