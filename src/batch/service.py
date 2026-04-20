from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from src.common.config import BATCH_CITY_CONFIGS, get_batch_city_config
from src.snowflake.connector import get_snowflake_connection


@dataclass
class CacheEntry:
    created_at: float
    payload: Any


class SnowflakeBatchService:
    def __init__(self, cache_ttl_seconds: int | None = None) -> None:
        if cache_ttl_seconds is None:
            cache_ttl_seconds = int(os.getenv("BATCH_API_CACHE_TTL_SECONDS", "21600"))
        self.cache_ttl_seconds = cache_ttl_seconds
        self._cache: dict[str, CacheEntry] = {}

    def list_batch_cities(self) -> list[dict[str, Any]]:
        cities: list[dict[str, Any]] = []
        for city in BATCH_CITY_CONFIGS.values():
            cities.append(
                {
                    "slug": city.slug,
                    "display_name": city.display_name,
                    "supports_batch": True,
                }
            )
        return cities

    def get_city_dashboard(self, city: str, stop_limit: int = 200, route_limit: int = 25) -> dict[str, Any]:
        city_slug = get_batch_city_config(city).slug
        overview = self._get_overview(city_slug)
        top_stops_activity = self._query_records(
            f"""
            SELECT
                city,
                stop_id,
                stop_name,
                stop_lat,
                stop_lon,
                trip_count,
                poi_count_within_400m,
                food_poi_count_within_400m,
                critical_service_poi_count_within_400m,
                park_poi_count_within_400m,
                nearest_school_m,
                nearest_hospital_m,
                nearest_grocery_m,
                nearest_park_m,
                poi_categories
            FROM CHICAGO_TRANSIT.BATCH_BUSIEST_STOPS_WITH_POI_CONTEXT
            WHERE city = {self._sql_string(city_slug)}
            ORDER BY trip_count DESC, poi_count_within_400m DESC
            LIMIT {int(stop_limit)}
            """,
            cache_key=f"dashboard:{city_slug}:top_stops_activity:{stop_limit}",
        )
        top_stops_poi = self._query_records(
            f"""
            SELECT
                city,
                stop_id,
                stop_name,
                stop_lat,
                stop_lon,
                poi_count_within_400m,
                food_poi_count_within_400m,
                critical_service_poi_count_within_400m,
                park_poi_count_within_400m,
                nearest_school_m,
                nearest_hospital_m,
                nearest_grocery_m,
                nearest_park_m,
                poi_categories
            FROM CHICAGO_TRANSIT.BATCH_STOP_POI_ACCESS
            WHERE city = {self._sql_string(city_slug)}
            ORDER BY poi_count_within_400m DESC, food_poi_count_within_400m DESC
            LIMIT {int(stop_limit)}
            """,
            cache_key=f"dashboard:{city_slug}:top_stops_poi:{stop_limit}",
        )
        top_routes_activity = self._query_records(
            f"""
            SELECT
                city,
                route_id,
                route_short_name,
                route_long_name,
                route_type,
                stop_event_count,
                distinct_trip_count,
                distinct_stop_count
            FROM CHICAGO_TRANSIT.BATCH_ROUTE_ACTIVITY
            WHERE city = {self._sql_string(city_slug)}
            ORDER BY stop_event_count DESC, distinct_trip_count DESC
            LIMIT {int(route_limit)}
            """,
            cache_key=f"dashboard:{city_slug}:top_routes_activity:{route_limit}",
        )
        top_routes_poi = self._query_records(
            f"""
            SELECT
                city,
                route_id,
                route_short_name,
                route_long_name,
                route_type,
                stop_count,
                total_poi_access,
                avg_poi_access_per_stop,
                max_poi_access_at_stop,
                stops_near_hospital,
                stops_near_grocery,
                stops_near_park
            FROM CHICAGO_TRANSIT.BATCH_ROUTE_POI_ACCESS
            WHERE city = {self._sql_string(city_slug)}
            ORDER BY total_poi_access DESC, avg_poi_access_per_stop DESC
            LIMIT {int(route_limit)}
            """,
            cache_key=f"dashboard:{city_slug}:top_routes_poi:{route_limit}",
        )
        road_coverage = self._query_records(
            f"""
            SELECT
                city,
                highway,
                total_road_segments,
                road_segments_near_transit,
                coverage_pct,
                total_length_km,
                covered_length_km
            FROM CHICAGO_TRANSIT.BATCH_TRANSIT_ROAD_COVERAGE
            WHERE city = {self._sql_string(city_slug)}
            ORDER BY coverage_pct DESC, total_length_km DESC
            """,
            cache_key=f"dashboard:{city_slug}:road_coverage",
        )

        return {
            "city": city_slug,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "overview": overview,
            "top_stops_by_activity": top_stops_activity,
            "top_stops_by_poi": top_stops_poi,
            "top_routes_by_activity": top_routes_activity,
            "top_routes_by_poi": top_routes_poi,
            "road_coverage": road_coverage,
        }

    def list_routes(self, city: str, limit: int = 500) -> list[dict[str, Any]]:
        city_slug = get_batch_city_config(city).slug
        return self._query_records(
            f"""
            SELECT
                city,
                route_id,
                route_short_name,
                route_long_name,
                route_type,
                stop_event_count,
                distinct_trip_count,
                distinct_stop_count
            FROM CHICAGO_TRANSIT.BATCH_ROUTE_ACTIVITY
            WHERE city = {self._sql_string(city_slug)}
            ORDER BY stop_event_count DESC, route_short_name ASC, route_long_name ASC
            LIMIT {int(limit)}
            """,
            cache_key=f"routes:{city_slug}:{limit}",
        )

    def get_route_detail(self, city: str, route_id: str, stop_limit: int = 80) -> dict[str, Any]:
        city_slug = get_batch_city_config(city).slug
        route_key = self._sql_string(route_id)
        summary_rows = self._query_records(
            f"""
            SELECT
                ra.city,
                ra.route_id,
                ra.route_short_name,
                ra.route_long_name,
                ra.route_type,
                ra.stop_event_count,
                ra.distinct_trip_count,
                ra.distinct_stop_count,
                rp.stop_count,
                rp.total_poi_access,
                rp.avg_poi_access_per_stop,
                rp.max_poi_access_at_stop,
                rp.stops_near_hospital,
                rp.stops_near_grocery,
                rp.stops_near_park
            FROM CHICAGO_TRANSIT.BATCH_ROUTE_ACTIVITY ra
            LEFT JOIN CHICAGO_TRANSIT.BATCH_ROUTE_POI_ACCESS rp
              ON ra.city = rp.city AND ra.route_id = rp.route_id
            WHERE ra.city = {self._sql_string(city_slug)}
              AND ra.route_id = {route_key}
            LIMIT 1
            """,
            cache_key=f"route_detail:{city_slug}:{route_id}:summary",
        )
        stop_rows = self._query_records(
            f"""
            SELECT
                sar.city,
                sar.route_id,
                sar.route_short_name,
                sar.route_long_name,
                sar.route_type,
                sar.stop_id,
                sar.stop_name,
                sar.stop_lat,
                sar.stop_lon,
                sar.trip_count,
                spa.poi_count_within_400m,
                spa.food_poi_count_within_400m,
                spa.critical_service_poi_count_within_400m,
                spa.park_poi_count_within_400m
            FROM CHICAGO_TRANSIT.BATCH_STOP_ACTIVITY_BY_ROUTE sar
            LEFT JOIN CHICAGO_TRANSIT.BATCH_STOP_POI_ACCESS spa
              ON sar.city = spa.city AND sar.stop_id = spa.stop_id
            WHERE sar.city = {self._sql_string(city_slug)}
              AND sar.route_id = {route_key}
            ORDER BY sar.trip_count DESC, spa.poi_count_within_400m DESC
            LIMIT {int(stop_limit)}
            """,
            cache_key=f"route_detail:{city_slug}:{route_id}:stops:{stop_limit}",
        )
        shape_rows = self._query_records(
            f"""
            SELECT
                city,
                route_id,
                shape_id,
                shape_pt_sequence,
                shape_pt_lat,
                shape_pt_lon
            FROM CHICAGO_TRANSIT.BATCH_ROUTE_SHAPES
            WHERE city = {self._sql_string(city_slug)}
              AND route_id = {route_key}
            ORDER BY shape_id, shape_pt_sequence
            """,
            cache_key=f"route_detail:{city_slug}:{route_id}:shapes",
        )
        paths = self._group_paths(shape_rows)
        summary = summary_rows[0] if summary_rows else None
        if summary is None:
            raise KeyError(f"Unsupported or missing route: {route_id}")
        return {
            "city": city_slug,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": summary,
            "stops": stop_rows,
            "paths": paths,
        }

    def get_city_comparison(self) -> dict[str, Any]:
        cities = sorted(BATCH_CITY_CONFIGS.keys())
        comparison_rows = []
        for city in cities:
            overview = self._get_overview(city)
            comparison_rows.append(
                {
                    "city": city,
                    "display_name": get_batch_city_config(city).display_name,
                    **overview,
                }
            )
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "cities": comparison_rows,
        }

    def _get_overview(self, city: str) -> dict[str, Any]:
        overview_rows = self._query_records(
            f"""
            SELECT
                (SELECT COUNT(*) FROM CHICAGO_TRANSIT.BATCH_GTFS_STOPS WHERE city = {self._sql_string(city)}) AS total_stops,
                (SELECT COUNT(*) FROM CHICAGO_TRANSIT.BATCH_ROUTE_ACTIVITY WHERE city = {self._sql_string(city)}) AS total_routes,
                (SELECT COALESCE(SUM(trip_count), 0) FROM CHICAGO_TRANSIT.BATCH_STOP_ACTIVITY WHERE city = {self._sql_string(city)}) AS total_stop_events,
                (SELECT COUNT(*) FROM CHICAGO_TRANSIT.BATCH_STOP_POI_ACCESS WHERE city = {self._sql_string(city)}) AS stops_with_poi_context,
                (SELECT ROUND(AVG(poi_count_within_400m), 2) FROM CHICAGO_TRANSIT.BATCH_STOP_POI_ACCESS WHERE city = {self._sql_string(city)}) AS avg_poi_access_per_stop
            """,
            cache_key=f"overview:{city}:totals",
        )
        busiest_stop_rows = self._query_records(
            f"""
            SELECT stop_name, trip_count
            FROM CHICAGO_TRANSIT.BATCH_STOP_ACTIVITY_ENRICHED
            WHERE city = {self._sql_string(city)}
            ORDER BY trip_count DESC
            LIMIT 1
            """,
            cache_key=f"overview:{city}:busiest_stop",
        )
        busiest_route_rows = self._query_records(
            f"""
            SELECT route_id, route_short_name, route_long_name, stop_event_count
            FROM CHICAGO_TRANSIT.BATCH_ROUTE_ACTIVITY
            WHERE city = {self._sql_string(city)}
            ORDER BY stop_event_count DESC
            LIMIT 1
            """,
            cache_key=f"overview:{city}:busiest_route",
        )
        poi_leader_rows = self._query_records(
            f"""
            SELECT stop_name, poi_count_within_400m, food_poi_count_within_400m
            FROM CHICAGO_TRANSIT.BATCH_STOP_POI_ACCESS
            WHERE city = {self._sql_string(city)}
            ORDER BY poi_count_within_400m DESC, food_poi_count_within_400m DESC
            LIMIT 1
            """,
            cache_key=f"overview:{city}:poi_leader",
        )
        totals = overview_rows[0] if overview_rows else {}
        return {
            "total_stops": totals.get("total_stops", 0),
            "total_routes": totals.get("total_routes", 0),
            "total_stop_events": totals.get("total_stop_events", 0),
            "stops_with_poi_context": totals.get("stops_with_poi_context", 0),
            "avg_poi_access_per_stop": totals.get("avg_poi_access_per_stop", 0),
            "busiest_stop": busiest_stop_rows[0] if busiest_stop_rows else None,
            "busiest_route": busiest_route_rows[0] if busiest_route_rows else None,
            "poi_leader": poi_leader_rows[0] if poi_leader_rows else None,
        }

    def _query_records(self, sql: str, cache_key: str) -> list[dict[str, Any]]:
        cached = self._cache.get(cache_key)
        now = time.time()
        if cached and now - cached.created_at < self.cache_ttl_seconds:
            return cached.payload

        connection = get_snowflake_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            dataframe = cursor.fetch_pandas_all()
        finally:
            cursor.close()
            connection.close()

        records = self._dataframe_to_records(dataframe)
        self._cache[cache_key] = CacheEntry(created_at=now, payload=records)
        return records

    @staticmethod
    def _group_paths(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        paths_by_shape: dict[str, list[list[float]]] = {}
        for row in rows:
            shape_id = str(row["shape_id"])
            paths_by_shape.setdefault(shape_id, []).append(
                [float(row["shape_pt_lon"]), float(row["shape_pt_lat"])]
            )

        grouped = []
        for shape_id, path in paths_by_shape.items():
            if len(path) >= 2:
                grouped.append({"shape_id": shape_id, "path": path})
        return grouped

    @staticmethod
    def _dataframe_to_records(dataframe: pd.DataFrame) -> list[dict[str, Any]]:
        if dataframe.empty:
            return []

        normalized = dataframe.copy()
        normalized.columns = [str(column).lower() for column in normalized.columns]
        normalized = normalized.where(pd.notnull(normalized), None)
        return json.loads(normalized.to_json(orient="records", date_format="iso"))

    @staticmethod
    def _sql_string(value: str) -> str:
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
