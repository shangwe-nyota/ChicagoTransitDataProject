from pathlib import Path
import argparse
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.snowflake.connector import get_snowflake_connection
from src.common.run_metadata import StageTracker, generate_run_id

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas


DDL_DIR = PROJECT_ROOT / "sql" / "ddl"
DATA_DIR = PROJECT_ROOT / "data" / "processed"
BATCH_CITY_DIRS = [path for path in DATA_DIR.iterdir() if path.is_dir() and path.name in {"chicago", "boston"}]


# ------------------------
# DDL EXECUTION (Create raw tables)
# ------------------------
def run_sql_file(cursor, file_path: Path) -> None:
    print(f"Running SQL file: {file_path.name}")
    sql_text = file_path.read_text()

    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]

    for statement in statements:
        print(f"\nExecuting:\n{statement}\n")
        cursor.execute(statement)


# ------------------------
# Load DATA (Processed Parquet files)
# ------------------------
def load_parquet_folder(conn, folder_path: Path, table_name: str) -> None:
    print(f"\n Loading {table_name} from {folder_path}")

    df = pd.read_parquet(folder_path)
    df.columns = [col.upper() for col in df.columns] #snowflake table names are uppercase in ddl

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name,
        auto_create_table=False
    )

    if success:
        print(f"✅ Loaded {nrows} rows into {table_name}")
    else:
        print(f"❌ Failed to load {table_name}")
    return {"table_name": table_name, "row_count": int(nrows), "folder_path": str(folder_path)}


def load_city_partitioned_table(conn, dataset_path_suffix: str, table_name: str) -> None:
    frames: list[pd.DataFrame] = []

    for city_dir in BATCH_CITY_DIRS:
        folder_path = city_dir / dataset_path_suffix
        if not folder_path.exists():
            continue

        print(f"\n Loading {table_name} partition from {folder_path}")
        frames.append(pd.read_parquet(folder_path))

    if not frames:
        print(f"\n Skipping {table_name}; no city-partitioned parquet folders found for {dataset_path_suffix}")
        return {"table_name": table_name, "row_count": 0, "folder_path": str(dataset_path_suffix), "skipped": True}

    df = pd.concat(frames, ignore_index=True)
    df.columns = [col.upper() for col in df.columns]
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name,
        auto_create_table=False
    )

    if success:
        print(f"✅ Loaded {nrows} rows into {table_name}")
    else:
        print(f"❌ Failed to load {table_name}")
    return {"table_name": table_name, "row_count": int(nrows), "folder_path": str(dataset_path_suffix)}


# ------------------------
# MAIN PIPELINE
# ------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load batch parquet outputs to Snowflake.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    tracker = StageTracker(
        stage="load_to_snowflake",
        city="shared",
        run_id=args.run_id or generate_run_id("shared"),
        force=args.force,
    )
    output_paths = [DDL_DIR / "raw_tables.sql", DDL_DIR / "clean_tables.sql", DDL_DIR / "analytics_tables.sql"]
    command = "python -m jobs.load.load_to_snowflake"

    if tracker.should_skip(output_paths):
        print(f"Skipping load_to_snowflake; checkpoint exists for run_id={tracker.run_id}")
        tracker.mark_skipped(command=command, input_paths=[DATA_DIR], output_paths=output_paths)
        return

    tracker.mark_running(command=command, input_paths=[DATA_DIR], output_paths=output_paths)
    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:
        load_results: list[dict[str, object]] = []
        # STEP 1: DDL
        run_sql_file(cur, DDL_DIR / "raw_tables.sql")
        run_sql_file(cur, DDL_DIR / "clean_tables.sql")
        run_sql_file(cur, DDL_DIR / "analytics_tables.sql")
        print("\n✅ All Snowflake DDL files executed successfully.")

        # STEP 2: LOAD CLEAN DATA
        print("\n Loading CLEAN tables...")
        load_results.append(load_parquet_folder(conn, DATA_DIR / "clean/gtfs/stops", "CLEAN_GTFS_STOPS"))
        load_results.append(load_parquet_folder(conn, DATA_DIR / "clean/gtfs/routes", "CLEAN_GTFS_ROUTES"))
        load_results.append(load_parquet_folder(conn, DATA_DIR / "clean/gtfs/trips", "CLEAN_GTFS_TRIPS"))
        load_results.append(load_parquet_folder(conn, DATA_DIR / "clean/gtfs/stop_times", "CLEAN_GTFS_STOP_TIMES"))
        load_results.append(load_parquet_folder(conn, DATA_DIR / "clean/gtfs/shapes", "CLEAN_GTFS_SHAPES"))

        # STEP 3: LOAD ANALYTICS DATA
        print("\n Loading ANALYTICS tables...")
        load_results.append(load_parquet_folder(conn, DATA_DIR / "analytics/stop_activity", "ANALYTICS_STOP_ACTIVITY"))
        load_results.append(load_parquet_folder(conn, DATA_DIR / "analytics/stop_activity_enriched", "ANALYTICS_STOP_ACTIVITY_ENRICHED"))
        load_results.append(load_parquet_folder(conn, DATA_DIR / "analytics/route_activity", "ANALYTICS_ROUTE_ACTIVITY"))
        load_results.append(load_parquet_folder(conn, DATA_DIR / "analytics/stop_activity_by_route", "ANALYTICS_STOP_ACTIVITY_BY_ROUTE"))
        load_results.append(load_parquet_folder(conn, DATA_DIR / "analytics/route_shapes", "ANALYTICS_ROUTE_SHAPES"))

        # STEP 4: LOAD MULTI-CITY BATCH TABLES
        print("\n Loading multi-city BATCH tables...")
        load_results.append(load_city_partitioned_table(conn, Path("clean/gtfs/stops"), "BATCH_GTFS_STOPS"))
        load_results.append(load_city_partitioned_table(conn, Path("clean/gtfs/routes"), "BATCH_GTFS_ROUTES"))
        load_results.append(load_city_partitioned_table(conn, Path("clean/gtfs/trips"), "BATCH_GTFS_TRIPS"))
        load_results.append(load_city_partitioned_table(conn, Path("clean/gtfs/stop_times"), "BATCH_GTFS_STOP_TIMES"))
        load_results.append(load_city_partitioned_table(conn, Path("clean/gtfs/shapes"), "BATCH_GTFS_SHAPES"))
        load_results.append(load_city_partitioned_table(conn, Path("clean/osm/roads"), "BATCH_OSM_ROADS"))
        load_results.append(load_city_partitioned_table(conn, Path("clean/osm/pois"), "BATCH_OSM_POIS"))
        load_results.append(load_city_partitioned_table(conn, Path("analytics/stop_activity"), "BATCH_STOP_ACTIVITY"))
        load_results.append(load_city_partitioned_table(conn, Path("analytics/stop_activity_enriched"), "BATCH_STOP_ACTIVITY_ENRICHED"))
        load_results.append(load_city_partitioned_table(conn, Path("analytics/route_activity"), "BATCH_ROUTE_ACTIVITY"))
        load_results.append(load_city_partitioned_table(conn, Path("analytics/stop_activity_by_route"), "BATCH_STOP_ACTIVITY_BY_ROUTE"))
        load_results.append(load_city_partitioned_table(conn, Path("analytics/route_shapes"), "BATCH_ROUTE_SHAPES"))
        load_results.append(load_city_partitioned_table(conn, Path("analytics/stop_poi_access"), "BATCH_STOP_POI_ACCESS"))
        load_results.append(load_city_partitioned_table(conn, Path("analytics/busiest_stops_with_poi_context"), "BATCH_BUSIEST_STOPS_WITH_POI_CONTEXT"))
        load_results.append(load_city_partitioned_table(conn, Path("analytics/route_poi_access"), "BATCH_ROUTE_POI_ACCESS"))
        load_results.append(load_city_partitioned_table(conn, Path("analytics/transit_road_coverage"), "BATCH_TRANSIT_ROAD_COVERAGE"))

        print("\n ALL DATA LOADED SUCCESSFULLY")
        tracker.mark_success(
            command=command,
            input_paths=[DATA_DIR],
            output_paths=output_paths,
            metrics={"load_results": load_results},
        )

    except Exception as error:
        tracker.mark_failed(command=command, error=error, input_paths=[DATA_DIR], output_paths=output_paths)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
