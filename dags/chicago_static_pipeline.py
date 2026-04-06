from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/Users/fortunakadima/ChicagoTransitDataProject"

default_args = {
    "owner": "chicago-transit",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="chicago_static_pipeline",
    default_args=default_args,
    description="End-to-end batch pipeline: GTFS + OSM ingestion, cleaning, analytics, and Snowflake loading",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["chicago-transit", "batch"],
) as dag:

    # ========================
    # STEP 1: INGESTION
    # ========================

    download_gtfs = BashOperator(
        task_id="download_gtfs",
        bash_command=f"cd {PROJECT_DIR} && python3 jobs/ingestion/download_gtfs.py",
    )

    download_osm = BashOperator(
        task_id="download_osm",
        bash_command=f"cd {PROJECT_DIR} && python3 jobs/ingestion/download_osm.py",
    )

    # ========================
    # STEP 2: CLEANING (GTFS)
    # ========================

    clean_gtfs_stops = BashOperator(
        task_id="clean_gtfs_stops",
        bash_command=f"cd {PROJECT_DIR} && python3 jobs/spark/clean_gtfs.py",
    )

    clean_gtfs_routes = BashOperator(
        task_id="clean_gtfs_routes",
        bash_command=f"cd {PROJECT_DIR} && python3 jobs/spark/clean_gtfs_routes.py",
    )

    clean_gtfs_trips = BashOperator(
        task_id="clean_gtfs_trips",
        bash_command=f"cd {PROJECT_DIR} && python3 jobs/spark/clean_gtfs_trips.py",
    )

    clean_gtfs_shapes = BashOperator(
        task_id="clean_gtfs_shapes",
        bash_command=f"cd {PROJECT_DIR} && python3 jobs/spark/clean_gtfs_shapes.py",
    )

    clean_gtfs_stop_times = BashOperator(
        task_id="clean_gtfs_stop_times",
        bash_command=f"cd {PROJECT_DIR} && python3 jobs/spark/clean_gtfs_stop_times.py",
    )

    # ========================
    # STEP 2: CLEANING (OSM)
    # ========================

    clean_osm_roads = BashOperator(
        task_id="clean_osm_roads",
        bash_command=f"cd {PROJECT_DIR} && python3 jobs/spark/clean_osm_roads.py",
    )

    clean_osm_pois = BashOperator(
        task_id="clean_osm_pois",
        bash_command=f"cd {PROJECT_DIR} && python3 jobs/spark/clean_osm_pois.py",
    )

    # ========================
    # STEP 3: ANALYTICS
    # ========================

    stop_activity = BashOperator(
        task_id="stop_activity",
        bash_command=f"cd {PROJECT_DIR} && python3 jobs/spark/analytics/stop_activity.py",
    )

    route_activity = BashOperator(
        task_id="route_activity",
        bash_command=f"cd {PROJECT_DIR} && python3 jobs/spark/analytics/route_activity.py",
    )

    stop_activity_by_route = BashOperator(
        task_id="stop_activity_by_route",
        bash_command=f"cd {PROJECT_DIR} && python3 jobs/spark/analytics/stop_activity_by_route.py",
    )

    join_stop_activity_with_stops = BashOperator(
        task_id="join_stop_activity_with_stops",
        bash_command=f"cd {PROJECT_DIR} && python3 jobs/spark/analytics/join_stop_activity_with_stops.py",
    )

    route_shapes = BashOperator(
        task_id="route_shapes",
        bash_command=f"cd {PROJECT_DIR} && python3 jobs/spark/analytics/route_shapes.py",
    )

    stop_poi_access = BashOperator(
        task_id="stop_poi_access",
        bash_command=f"cd {PROJECT_DIR} && python3 jobs/spark/analytics/stop_poi_access.py",
    )

    transit_road_coverage = BashOperator(
        task_id="transit_road_coverage",
        bash_command=f"cd {PROJECT_DIR} && python3 jobs/spark/analytics/transit_road_coverage.py",
    )

    # ========================
    # STEP 4: LOAD TO SNOWFLAKE
    # ========================

    load_to_snowflake = BashOperator(
        task_id="load_to_snowflake",
        bash_command=f"cd {PROJECT_DIR} && python3 -m jobs.load.load_to_snowflake",
    )

    # ========================
    # TASK DEPENDENCIES
    # ========================

    # Ingestion -> Cleaning
    download_gtfs >> [clean_gtfs_stops, clean_gtfs_routes, clean_gtfs_trips, clean_gtfs_shapes, clean_gtfs_stop_times]
    download_osm >> [clean_osm_roads, clean_osm_pois]

    # Cleaning -> Analytics (based on data dependencies)
    clean_gtfs_stop_times >> stop_activity
    [clean_gtfs_stop_times, clean_gtfs_trips, clean_gtfs_routes] >> route_activity
    [clean_gtfs_stop_times, clean_gtfs_trips, clean_gtfs_routes, clean_gtfs_stops] >> stop_activity_by_route
    [stop_activity, clean_gtfs_stops] >> join_stop_activity_with_stops
    [clean_gtfs_trips, clean_gtfs_routes, clean_gtfs_shapes] >> route_shapes
    [clean_gtfs_stops, clean_osm_pois] >> stop_poi_access
    [clean_gtfs_stops, clean_osm_roads] >> transit_road_coverage

    # Analytics -> Load
    [stop_activity, route_activity, stop_activity_by_route,
     join_stop_activity_with_stops, route_shapes,
     stop_poi_access, transit_road_coverage] >> load_to_snowflake
