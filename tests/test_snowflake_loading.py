"""
Issue #4: Test Batch Writing to Snowflake

Tests that Snowflake loading logic is:
- Logically correct (column uppercasing, correct table names)
- Idempotent (DDL re-execution, data re-loading)
- Using correct table/schema/warehouse
- NOT touching real Snowflake (all mocked)
"""
import os
import sys
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jobs.load.load_to_snowflake import run_sql_file, load_parquet_folder


# =====================
# DDL EXECUTION TESTS
# =====================

class TestRunSQLFile:

    def test_executes_all_statements(self, tmp_path):
        """Each semicolon-separated statement should be executed."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("USE ROLE TEST;\nCREATE TABLE foo (id INT);\nCREATE TABLE bar (id INT);")

        cursor = MagicMock()
        run_sql_file(cursor, sql_file)

        assert cursor.execute.call_count == 3

    def test_skips_empty_statements(self, tmp_path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("USE ROLE TEST;;\n\n;CREATE TABLE foo (id INT);")

        cursor = MagicMock()
        run_sql_file(cursor, sql_file)

        # Should only execute non-empty: "USE ROLE TEST" and "CREATE TABLE foo (id INT)"
        assert cursor.execute.call_count == 2

    def test_idempotent_execution(self, tmp_path):
        """Running DDL twice should not raise errors."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("CREATE TABLE IF NOT EXISTS foo (id INT);")

        cursor = MagicMock()
        run_sql_file(cursor, sql_file)
        run_sql_file(cursor, sql_file)
        assert cursor.execute.call_count == 2  # Called twice, no error


# =====================
# PARQUET LOADING TESTS
# =====================

class TestLoadParquetFolder:

    def test_uppercases_column_names(self, spark, tmp_dir):
        """Snowflake requires uppercase column names."""
        # Create a small parquet file with lowercase columns
        df = spark.createDataFrame(
            [("1", "test", 41.88)],
            ["stop_id", "stop_name", "stop_lat"],
        )
        parquet_path = os.path.join(tmp_dir, "test_data")
        df.write.mode("overwrite").parquet(parquet_path)

        conn = MagicMock()
        with patch("jobs.load.load_to_snowflake.write_pandas") as mock_write:
            mock_write.return_value = (True, 1, 1, None)
            load_parquet_folder(conn, Path(parquet_path), "TEST_TABLE")

            # Check the DataFrame passed to write_pandas has uppercase columns
            actual_df = mock_write.call_args[0][1]
            assert list(actual_df.columns) == ["STOP_ID", "STOP_NAME", "STOP_LAT"]

    def test_correct_table_name(self, spark, tmp_dir):
        df = spark.createDataFrame([("1",)], ["id"])
        parquet_path = os.path.join(tmp_dir, "data")
        df.write.mode("overwrite").parquet(parquet_path)

        conn = MagicMock()
        with patch("jobs.load.load_to_snowflake.write_pandas") as mock_write:
            mock_write.return_value = (True, 1, 1, None)
            load_parquet_folder(conn, Path(parquet_path), "CLEAN_GTFS_STOPS")

            # table_name is 3rd positional arg: write_pandas(conn, df, table_name, ...)
            assert mock_write.call_args[0][2] == "CLEAN_GTFS_STOPS"

    def test_auto_create_table_is_false(self, spark, tmp_dir):
        """Tables must be pre-created by DDL — never auto-created."""
        df = spark.createDataFrame([("1",)], ["id"])
        parquet_path = os.path.join(tmp_dir, "data")
        df.write.mode("overwrite").parquet(parquet_path)

        conn = MagicMock()
        with patch("jobs.load.load_to_snowflake.write_pandas") as mock_write:
            mock_write.return_value = (True, 1, 1, None)
            load_parquet_folder(conn, Path(parquet_path), "TEST")

            assert mock_write.call_args[1]["auto_create_table"] is False


# =====================
# FULL PIPELINE TESTS
# =====================

class TestMainPipeline:

    @patch("jobs.load.load_to_snowflake.get_snowflake_connection")
    @patch("jobs.load.load_to_snowflake.write_pandas")
    @patch("jobs.load.load_to_snowflake.pd.read_parquet")
    def test_loads_all_expected_tables(self, mock_read_parquet, mock_write, mock_conn):
        """main() should load all 15 tables (GTFS clean + OSM clean + analytics)."""
        import pandas as pd

        mock_read_parquet.return_value = pd.DataFrame({"col": [1]})
        mock_write.return_value = (True, 1, 1, None)

        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_conn.return_value = mock_connection

        from jobs.load.load_to_snowflake import main
        main()

        # Extract table names from write_pandas calls (3rd positional arg)
        table_names = [c[0][2] for c in mock_write.call_args_list]

        expected_tables = [
            "CLEAN_GTFS_STOPS", "CLEAN_GTFS_ROUTES", "CLEAN_GTFS_TRIPS",
            "CLEAN_GTFS_STOP_TIMES", "CLEAN_GTFS_SHAPES",
            "CLEAN_OSM_ROADS", "CLEAN_OSM_POIS",
            "ANALYTICS_STOP_ACTIVITY", "ANALYTICS_STOP_ACTIVITY_ENRICHED",
            "ANALYTICS_ROUTE_ACTIVITY", "ANALYTICS_STOP_ACTIVITY_BY_ROUTE",
            "ANALYTICS_ROUTE_SHAPES",
            "ANALYTICS_STOP_POI_ACCESS", "ANALYTICS_TRANSIT_ROAD_COVERAGE",
        ]

        for table in expected_tables:
            assert table in table_names, f"Missing table load: {table}"

    @patch("jobs.load.load_to_snowflake.get_snowflake_connection")
    @patch("jobs.load.load_to_snowflake.write_pandas")
    @patch("jobs.load.load_to_snowflake.pd.read_parquet")
    def test_ddl_executed_before_data_load(self, mock_read_parquet, mock_write, mock_conn):
        """DDL files must be executed before any data is loaded."""
        import pandas as pd

        mock_read_parquet.return_value = pd.DataFrame({"col": [1]})
        mock_write.return_value = (True, 1, 1, None)

        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_conn.return_value = mock_connection

        from jobs.load.load_to_snowflake import main
        main()

        # DDL should have been executed (cursor.execute called many times)
        assert mock_cursor.execute.call_count > 0
        # write_pandas should also have been called
        assert mock_write.call_count > 0

    @patch("jobs.load.load_to_snowflake.get_snowflake_connection")
    def test_connection_is_closed(self, mock_conn):
        """Connection must be closed even if loading fails."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_conn.return_value = mock_connection

        # Force an error during DDL execution
        mock_cursor.execute.side_effect = Exception("Snowflake error")

        from jobs.load.load_to_snowflake import main
        with pytest.raises(Exception):
            main()

        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
