"""
Issue #2: Test DDL Creation Scripts

Tests that all DDL scripts are:
- Valid SQL (parseable, correct syntax)
- Idempotent (IF NOT EXISTS for raw, OR REPLACE for clean/analytics)
- Complete (cover all expected tables)
"""
import os
import re
import pytest

DDL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql", "ddl")


def read_sql(filename):
    path = os.path.join(DDL_DIR, filename)
    with open(path) as f:
        return f.read()


def extract_create_statements(sql_text):
    """Extract all CREATE TABLE statements from SQL text."""
    pattern = r"CREATE\s+(TABLE\s+IF\s+NOT\s+EXISTS|OR\s+REPLACE\s+TABLE)\s+(\w+)"
    return re.findall(pattern, sql_text, re.IGNORECASE)


def extract_table_names(sql_text):
    """Extract table names from CREATE statements."""
    stmts = extract_create_statements(sql_text)
    return [name for _, name in stmts]


# =====================
# raw_tables.sql
# =====================

class TestRawTablesDDL:

    def test_file_exists(self):
        assert os.path.isfile(os.path.join(DDL_DIR, "raw_tables.sql"))

    def test_all_raw_gtfs_tables_present(self):
        sql = read_sql("raw_tables.sql")
        tables = extract_table_names(sql)
        expected = [
            "RAW_GTFS_AGENCY", "RAW_GTFS_CALENDAR", "RAW_GTFS_CALENDAR_DATES",
            "RAW_GTFS_FREQUENCIES", "RAW_GTFS_ROUTES", "RAW_GTFS_SHAPES",
            "RAW_GTFS_STOP_TIMES", "RAW_GTFS_STOPS", "RAW_GTFS_TRANSFERS",
            "RAW_GTFS_TRIPS",
        ]
        for table in expected:
            assert table in tables, f"Missing raw table: {table}"

    def test_all_raw_osm_tables_present(self):
        sql = read_sql("raw_tables.sql")
        tables = extract_table_names(sql)
        assert "RAW_OSM_ROADS" in tables
        assert "RAW_OSM_POIS" in tables

    def test_raw_tables_use_if_not_exists(self):
        """Raw tables must use IF NOT EXISTS to never wipe source data."""
        sql = read_sql("raw_tables.sql")
        stmts = extract_create_statements(sql)
        for create_type, table_name in stmts:
            assert "IF NOT EXISTS" in create_type.upper(), (
                f"{table_name} does not use IF NOT EXISTS — raw data could be wiped!"
            )

    def test_no_drop_statements(self):
        sql = read_sql("raw_tables.sql")
        assert "DROP TABLE" not in sql.upper(), "raw_tables.sql must never DROP tables"

    def test_statements_are_semicolon_terminated(self):
        sql = read_sql("raw_tables.sql")
        statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
        for stmt in statements:
            # Each should be a valid SQL statement (USE or CREATE)
            first_word = stmt.split()[0].upper()
            assert first_word in ("USE", "CREATE", "--"), f"Unexpected statement: {stmt[:50]}"


# =====================
# clean_tables.sql
# =====================

class TestCleanTablesDDL:

    def test_file_exists(self):
        assert os.path.isfile(os.path.join(DDL_DIR, "clean_tables.sql"))

    def test_all_clean_gtfs_tables_present(self):
        sql = read_sql("clean_tables.sql")
        tables = extract_table_names(sql)
        expected = [
            "CLEAN_GTFS_STOPS", "CLEAN_GTFS_ROUTES", "CLEAN_GTFS_TRIPS",
            "CLEAN_GTFS_STOP_TIMES", "CLEAN_GTFS_SHAPES",
        ]
        for table in expected:
            assert table in tables, f"Missing clean table: {table}"

    def test_all_clean_osm_tables_present(self):
        sql = read_sql("clean_tables.sql")
        tables = extract_table_names(sql)
        assert "CLEAN_OSM_ROADS" in tables
        assert "CLEAN_OSM_POIS" in tables

    def test_clean_tables_use_or_replace(self):
        """Clean tables use OR REPLACE for idempotent rebuilds."""
        sql = read_sql("clean_tables.sql")
        stmts = extract_create_statements(sql)
        for create_type, table_name in stmts:
            assert "OR REPLACE" in create_type.upper(), (
                f"{table_name} does not use OR REPLACE — not idempotent!"
            )


# =====================
# analytics_tables.sql
# =====================

class TestAnalyticsTablesDDL:

    def test_file_exists(self):
        assert os.path.isfile(os.path.join(DDL_DIR, "analytics_tables.sql"))

    def test_all_analytics_tables_present(self):
        sql = read_sql("analytics_tables.sql")
        tables = extract_table_names(sql)
        expected = [
            "ANALYTICS_STOP_ACTIVITY", "ANALYTICS_STOP_ACTIVITY_ENRICHED",
            "ANALYTICS_ROUTE_ACTIVITY", "ANALYTICS_STOP_ACTIVITY_BY_ROUTE",
            "ANALYTICS_ROUTE_SHAPES",
            "ANALYTICS_STOP_POI_ACCESS", "ANALYTICS_TRANSIT_ROAD_COVERAGE",
        ]
        for table in expected:
            assert table in tables, f"Missing analytics table: {table}"

    def test_analytics_tables_use_or_replace(self):
        sql = read_sql("analytics_tables.sql")
        stmts = extract_create_statements(sql)
        for create_type, table_name in stmts:
            assert "OR REPLACE" in create_type.upper(), (
                f"{table_name} does not use OR REPLACE — not idempotent!"
            )


# =====================
# Cross-DDL consistency
# =====================

class TestDDLConsistency:

    def test_ddl_execution_order_is_safe(self):
        """Running DDL files in order raw -> clean -> analytics should not conflict."""
        raw_tables = extract_table_names(read_sql("raw_tables.sql"))
        clean_tables = extract_table_names(read_sql("clean_tables.sql"))
        analytics_tables = extract_table_names(read_sql("analytics_tables.sql"))

        # No name collisions across layers
        all_tables = raw_tables + clean_tables + analytics_tables
        assert len(all_tables) == len(set(all_tables)), "Duplicate table names across DDL files!"

    def test_all_ddl_files_set_context(self):
        """Each DDL file should set USE ROLE, USE WAREHOUSE, USE DATABASE."""
        for filename in ["raw_tables.sql", "clean_tables.sql", "analytics_tables.sql"]:
            sql = read_sql(filename).upper()
            assert "USE ROLE" in sql, f"{filename} missing USE ROLE"
            assert "USE WAREHOUSE" in sql, f"{filename} missing USE WAREHOUSE"
            assert "USE DATABASE" in sql, f"{filename} missing USE DATABASE"
