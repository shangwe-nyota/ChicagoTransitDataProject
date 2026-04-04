from pathlib import Path

from src.snowflake.connector import get_snowflake_connection

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DDL_DIR = PROJECT_ROOT / "sql" / "ddl"
DATA_DIR = PROJECT_ROOT / "data" / "processed"


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


# ------------------------
# MAIN PIPELINE
# ------------------------
def main() -> None:
    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:
        # STEP 1: DDL
        run_sql_file(cur, DDL_DIR / "raw_tables.sql")
        run_sql_file(cur, DDL_DIR / "clean_tables.sql")
        run_sql_file(cur, DDL_DIR / "analytics_tables.sql")
        print("\n✅ All Snowflake DDL files executed successfully.")

        # STEP 2: LOAD CLEAN DATA
        print("\n Loading CLEAN tables...")
        load_parquet_folder(conn, DATA_DIR / "clean/gtfs/stops", "CLEAN_GTFS_STOPS")
        load_parquet_folder(conn, DATA_DIR / "clean/gtfs/routes", "CLEAN_GTFS_ROUTES")
        load_parquet_folder(conn, DATA_DIR / "clean/gtfs/trips", "CLEAN_GTFS_TRIPS")
        load_parquet_folder(conn, DATA_DIR / "clean/gtfs/stop_times", "CLEAN_GTFS_STOP_TIMES")
        load_parquet_folder(conn, DATA_DIR / "clean/gtfs/shapes", "CLEAN_GTFS_SHAPES")

        # STEP 3: LOAD ANALYTICS DATA
        print("\n Loading ANALYTICS tables...")
        load_parquet_folder(conn, DATA_DIR / "analytics/stop_activity", "ANALYTICS_STOP_ACTIVITY")
        load_parquet_folder(conn, DATA_DIR / "analytics/stop_activity_enriched", "ANALYTICS_STOP_ACTIVITY_ENRICHED")
        load_parquet_folder(conn, DATA_DIR / "analytics/route_activity", "ANALYTICS_ROUTE_ACTIVITY")
        load_parquet_folder(conn, DATA_DIR / "analytics/stop_activity_by_route", "ANALYTICS_STOP_ACTIVITY_BY_ROUTE")
        load_parquet_folder(conn, DATA_DIR / "analytics/route_shapes", "ANALYTICS_ROUTE_SHAPES")

        print("\n ALL DATA LOADED SUCCESSFULLY")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()