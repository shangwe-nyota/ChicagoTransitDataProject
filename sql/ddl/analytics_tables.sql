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