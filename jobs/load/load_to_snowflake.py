from pathlib import Path

from src.snowflake.connector import get_snowflake_connection


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DDL_DIR = PROJECT_ROOT / "sql" / "ddl"


def run_sql_file(cursor, file_path: Path) -> None:
    print(f"Running SQL file: {file_path.name}")
    sql_text = file_path.read_text()

    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]

    for statement in statements:
        print(f"\nExecuting:\n{statement}\n")
        cursor.execute(statement)


def main() -> None:
    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:
        run_sql_file(cur, DDL_DIR / "raw_tables.sql")
        run_sql_file(cur, DDL_DIR / "clean_tables.sql")
        run_sql_file(cur, DDL_DIR / "analytics_tables.sql")
        print("\n✅ All Snowflake DDL files executed successfully.")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()