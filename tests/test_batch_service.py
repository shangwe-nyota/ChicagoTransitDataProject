from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from src.batch.service import SnowflakeBatchService


class BatchServiceTests(unittest.TestCase):
    def test_list_batch_cities_contains_boston_and_chicago(self) -> None:
        service = SnowflakeBatchService()
        cities = service.list_batch_cities()
        self.assertEqual([city["slug"] for city in cities], ["chicago", "boston"])
        self.assertTrue(all(city["supports_batch"] for city in cities))

    def test_group_paths_groups_points_by_shape_id(self) -> None:
        rows = [
            {"shape_id": "shape-a", "shape_pt_lon": -71.1, "shape_pt_lat": 42.3},
            {"shape_id": "shape-a", "shape_pt_lon": -71.2, "shape_pt_lat": 42.4},
            {"shape_id": "shape-b", "shape_pt_lon": -87.6, "shape_pt_lat": 41.8},
            {"shape_id": "shape-b", "shape_pt_lon": -87.7, "shape_pt_lat": 41.9},
        ]
        grouped = SnowflakeBatchService._group_paths(rows)

        self.assertEqual(len(grouped), 2)
        self.assertEqual(grouped[0]["shape_id"], "shape-a")
        self.assertEqual(grouped[0]["path"][0], [-71.1, 42.3])
        self.assertEqual(grouped[1]["shape_id"], "shape-b")

    def test_dataframe_to_records_normalizes_columns_and_nulls(self) -> None:
        dataframe = pd.DataFrame(
            [
                {"STOP_NAME": "Nubian", "TRIP_COUNT": 10414, "NEAREST_HOSPITAL_M": None},
                {"STOP_NAME": "Ashmont", "TRIP_COUNT": 7570, "NEAREST_HOSPITAL_M": 100.5},
            ]
        )

        records = SnowflakeBatchService._dataframe_to_records(dataframe)

        self.assertEqual(records[0]["stop_name"], "Nubian")
        self.assertEqual(records[0]["trip_count"], 10414)
        self.assertIsNone(records[0]["nearest_hospital_m"])
        self.assertEqual(records[1]["nearest_hospital_m"], 100.5)

    def test_query_records_uses_in_memory_cache_until_ttl_expires(self) -> None:
        class FakeCursor:
            def __init__(self) -> None:
                self.execute_calls = 0

            def execute(self, sql: str) -> None:
                self.execute_calls += 1

            def fetch_pandas_all(self) -> pd.DataFrame:
                return pd.DataFrame([{"CITY": "boston", "TOTAL_STOPS": 1}])

            def close(self) -> None:
                return None

        class FakeConnection:
            def __init__(self, cursor: FakeCursor) -> None:
                self._cursor = cursor

            def cursor(self) -> FakeCursor:
                return self._cursor

            def close(self) -> None:
                return None

        fake_cursor = FakeCursor()
        fake_connection = FakeConnection(fake_cursor)
        service = SnowflakeBatchService(cache_ttl_seconds=3600)

        with patch("src.batch.service.get_snowflake_connection", return_value=fake_connection):
            first = service._query_records("SELECT 1", cache_key="cache-key")
            second = service._query_records("SELECT 1", cache_key="cache-key")

        self.assertEqual(fake_cursor.execute_calls, 1)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
