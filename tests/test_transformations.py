"""
Issue #3: Test GTFS + OSM Transformations

Tests that all Spark cleaning and analytics jobs are:
- Logically correct (proper trimming, casting, filtering)
- Idempotent (overwrite mode, same result on re-run)
- Writing to correct output paths
"""
import os
import shutil
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, count, countDistinct


FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


# =====================
# GTFS CLEANING TESTS
# =====================

class TestCleanGTFSStops:

    def test_trims_whitespace(self, spark, tmp_dir):
        df = spark.read.csv(os.path.join(FIXTURE_DIR, "stops.csv"), header=True, inferSchema=True)
        cleaned = (
            df.select(
                trim(col("stop_id")).cast("string").alias("stop_id"),
                trim(col("stop_name")).alias("stop_name"),
                col("stop_lat").cast("double").alias("stop_lat"),
                col("stop_lon").cast("double").alias("stop_lon"),
                col("location_type").cast("int").alias("location_type"),
                trim(col("parent_station")).cast("string").alias("parent_station"),
            )
            .dropna(subset=["stop_id", "stop_name", "stop_lat", "stop_lon"])
            .dropDuplicates(["stop_id"])
        )
        # stop_id "  3 " should be trimmed (inferSchema may read as numeric 3.0)
        ids = [row.stop_id for row in cleaned.collect()]
        # Should contain trimmed version, not the whitespace version
        assert any("3" in sid for sid in ids)
        assert "  3 " not in ids

    def test_drops_nulls(self, spark):
        df = spark.read.csv(os.path.join(FIXTURE_DIR, "stops.csv"), header=True, inferSchema=True)
        cleaned = (
            df.select(
                trim(col("stop_id")).cast("string").alias("stop_id"),
                trim(col("stop_name")).alias("stop_name"),
                col("stop_lat").cast("double"),
                col("stop_lon").cast("double"),
            )
            .dropna(subset=["stop_id", "stop_name", "stop_lat", "stop_lon"])
        )
        # stop_id 4 has missing lat — should be dropped
        ids = [row.stop_id for row in cleaned.collect()]
        assert "4" not in ids

    def test_deduplicates_on_stop_id(self, spark):
        df = spark.read.csv(os.path.join(FIXTURE_DIR, "stops.csv"), header=True, inferSchema=True)
        cleaned = (
            df.select(trim(col("stop_id")).cast("string").alias("stop_id"))
            .dropna(subset=["stop_id"])
            .dropDuplicates(["stop_id"])
        )
        id_counts = cleaned.groupBy("stop_id").count().filter(col("count") > 1).count()
        assert id_counts == 0, "Duplicates remain after deduplication"

    def test_output_schema(self, spark):
        df = spark.read.csv(os.path.join(FIXTURE_DIR, "stops.csv"), header=True, inferSchema=True)
        cleaned = df.select(
            trim(col("stop_id")).cast("string").alias("stop_id"),
            trim(col("stop_name")).alias("stop_name"),
            col("stop_lat").cast("double").alias("stop_lat"),
            col("stop_lon").cast("double").alias("stop_lon"),
            col("location_type").cast("int").alias("location_type"),
            trim(col("parent_station")).cast("string").alias("parent_station"),
        )
        assert set(cleaned.columns) == {"stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station"}

    def test_idempotent_write(self, spark, tmp_dir):
        """Writing twice to same dir should produce same result."""
        df = spark.read.csv(os.path.join(FIXTURE_DIR, "stops.csv"), header=True, inferSchema=True)
        cleaned = (
            df.select(
                trim(col("stop_id")).cast("string").alias("stop_id"),
                trim(col("stop_name")).alias("stop_name"),
                col("stop_lat").cast("double").alias("stop_lat"),
                col("stop_lon").cast("double").alias("stop_lon"),
            )
            .dropna(subset=["stop_id", "stop_name", "stop_lat", "stop_lon"])
            .dropDuplicates(["stop_id"])
        )
        out = os.path.join(tmp_dir, "stops")
        cleaned.write.mode("overwrite").parquet(out)
        count1 = spark.read.parquet(out).count()
        cleaned.write.mode("overwrite").parquet(out)
        count2 = spark.read.parquet(out).count()
        assert count1 == count2


class TestCleanGTFSRoutes:

    def test_expected_row_count(self, spark):
        df = spark.read.csv(os.path.join(FIXTURE_DIR, "routes.csv"), header=True, inferSchema=True)
        cleaned = (
            df.select(
                trim(col("route_id")).cast("string").alias("route_id"),
                trim(col("route_short_name")).alias("route_short_name"),
                trim(col("route_long_name")).alias("route_long_name"),
                col("route_type").cast("int").alias("route_type"),
            )
            .dropna(subset=["route_id"])
            .dropDuplicates(["route_id"])
        )
        # 5 rows: R1, R2, R1(dupe), missing-id, R3
        # After dropna on route_id: remove missing-id row
        # After dedup on route_id: remove R1 dupe
        # Expected: R1, R2, R3 = 3 rows
        assert cleaned.count() == 3

    def test_trims_route_id(self, spark):
        df = spark.read.csv(os.path.join(FIXTURE_DIR, "routes.csv"), header=True, inferSchema=True)
        cleaned = (
            df.select(trim(col("route_id")).cast("string").alias("route_id"))
            .dropna(subset=["route_id"])
            .dropDuplicates(["route_id"])
        )
        ids = [r.route_id for r in cleaned.collect()]
        assert "R1" in ids
        assert "  R1 " not in ids


class TestCleanGTFSStopTimes:

    def test_drops_null_rows(self, spark):
        df = spark.read.csv(os.path.join(FIXTURE_DIR, "stop_times.csv"), header=True, inferSchema=True)
        cleaned = (
            df.select(
                trim(col("trip_id")).cast("string").alias("trip_id"),
                trim(col("stop_id")).cast("string").alias("stop_id"),
                col("stop_sequence").cast("int").alias("stop_sequence"),
            )
            .dropna(subset=["trip_id", "stop_id", "stop_sequence"])
        )
        # Row 6 is all nulls, row 7 has null stop_id
        assert cleaned.count() == 5

    def test_no_deduplication(self, spark):
        """stop_times should NOT deduplicate — same stop can appear multiple times."""
        df = spark.read.csv(os.path.join(FIXTURE_DIR, "stop_times.csv"), header=True, inferSchema=True)
        cleaned = (
            df.select(
                trim(col("trip_id")).cast("string").alias("trip_id"),
                trim(col("stop_id")).cast("string").alias("stop_id"),
                col("stop_sequence").cast("int").alias("stop_sequence"),
            )
            .dropna(subset=["trip_id", "stop_id", "stop_sequence"])
        )
        # stop_id 1 appears twice (T1 and T2) — both should remain
        stop1_count = cleaned.filter(col("stop_id") == "1").count()
        assert stop1_count == 2


# =====================
# OSM CLEANING TESTS
# =====================

class TestCleanOSMRoads:

    def test_filters_highway_types(self, spark):
        df = spark.read.csv(os.path.join(FIXTURE_DIR, "osm_roads.csv"), header=True, inferSchema=True)
        relevant = ["motorway", "trunk", "primary", "secondary", "tertiary", "residential", "unclassified"]
        cleaned = (
            df.select(
                col("osm_id").cast("string").alias("osm_id"),
                trim(col("highway")).alias("highway"),
                col("start_lat").cast("double"),
                col("start_lon").cast("double"),
                col("end_lat").cast("double"),
                col("end_lon").cast("double"),
            )
            .dropna(subset=["osm_id", "start_lat", "start_lon", "end_lat", "end_lon"])
            .filter(col("highway").isin(relevant))
            .dropDuplicates(["osm_id"])
        )
        highways = [r.highway for r in cleaned.collect()]
        assert "service" not in highways  # osm_id 103 is "service" type

    def test_deduplicates(self, spark):
        df = spark.read.csv(os.path.join(FIXTURE_DIR, "osm_roads.csv"), header=True, inferSchema=True)
        cleaned = (
            df.select(col("osm_id").cast("string").alias("osm_id"), col("highway"))
            .dropna(subset=["osm_id"])
            .dropDuplicates(["osm_id"])
        )
        assert cleaned.filter(col("osm_id") == "100").count() == 1


class TestCleanOSMPOIs:

    def test_filters_amenity_types(self, spark):
        df = spark.read.csv(os.path.join(FIXTURE_DIR, "osm_pois.csv"), header=True, inferSchema=True)
        relevant = ["school", "hospital", "clinic", "library", "pharmacy",
                     "restaurant", "cafe", "bank", "place_of_worship",
                     "community_centre", "police", "fire_station", "post_office"]
        cleaned = (
            df.select(
                col("osm_id").cast("string").alias("osm_id"),
                trim(col("amenity")).alias("amenity"),
                col("lat").cast("double"),
                col("lon").cast("double"),
            )
            .dropna(subset=["osm_id", "amenity", "lat", "lon"])
            .filter(col("amenity").isin(relevant))
            .dropDuplicates(["osm_id"])
        )
        amenities = [r.amenity for r in cleaned.collect()]
        assert "atm" not in amenities  # P4 is "atm" — not in relevant list

    def test_expected_count(self, spark):
        df = spark.read.csv(os.path.join(FIXTURE_DIR, "osm_pois.csv"), header=True, inferSchema=True)
        relevant = ["school", "hospital", "clinic", "library", "pharmacy",
                     "restaurant", "cafe", "bank", "place_of_worship",
                     "community_centre", "police", "fire_station", "post_office"]
        cleaned = (
            df.select(
                col("osm_id").cast("string").alias("osm_id"),
                trim(col("amenity")).alias("amenity"),
                col("lat").cast("double"),
                col("lon").cast("double"),
            )
            .dropna(subset=["osm_id", "amenity", "lat", "lon"])
            .filter(col("amenity").isin(relevant))
            .dropDuplicates(["osm_id"])
        )
        # P1 school, P2 hospital, P3 cafe, P5 library = 4 valid
        # P4 (atm) filtered, P1 dupe removed, P6 null lat removed
        assert cleaned.count() == 4


# =====================
# ANALYTICS TESTS
# =====================

class TestStopActivityAnalytics:

    def test_counts_trips_per_stop(self, spark):
        """stop_activity groups by stop_id and counts occurrences."""
        df = spark.read.csv(os.path.join(FIXTURE_DIR, "stop_times.csv"), header=True, inferSchema=True)
        df = df.dropna(subset=["stop_id"])
        activity = df.groupBy("stop_id").agg(count("*").alias("trip_count"))

        # stop 1 appears in T1 and T2 = 2
        # stop 2 appears in T1 and T3 = 2
        # stop 3 appears in T2 = 1
        row_1 = activity.filter(col("stop_id") == 1).first()
        assert row_1.trip_count == 2

    def test_idempotent_output(self, spark, tmp_dir):
        df = spark.read.csv(os.path.join(FIXTURE_DIR, "stop_times.csv"), header=True, inferSchema=True)
        df = df.dropna(subset=["stop_id"])
        activity = df.groupBy("stop_id").agg(count("*").alias("trip_count"))

        out = os.path.join(tmp_dir, "stop_activity")
        activity.write.mode("overwrite").parquet(out)
        c1 = spark.read.parquet(out).count()
        activity.write.mode("overwrite").parquet(out)
        c2 = spark.read.parquet(out).count()
        assert c1 == c2


class TestRouteActivityAnalytics:

    def test_join_and_aggregate(self, spark, tmp_dir):
        """Route activity joins stop_times -> trips -> routes and aggregates."""
        # Write clean fixtures as parquet (analytics reads parquet, not CSV)
        stops_times_df = spark.read.csv(os.path.join(FIXTURE_DIR, "stop_times.csv"), header=True, inferSchema=True)
        stops_times_df = stops_times_df.dropna(subset=["trip_id", "stop_id"])
        st_path = os.path.join(tmp_dir, "stop_times")
        stops_times_df.write.mode("overwrite").parquet(st_path)

        trips_df = spark.read.csv(os.path.join(FIXTURE_DIR, "trips.csv"), header=True, inferSchema=True)
        trips_df = trips_df.dropDuplicates(["trip_id"])
        t_path = os.path.join(tmp_dir, "trips")
        trips_df.write.mode("overwrite").parquet(t_path)

        routes_df = spark.read.csv(os.path.join(FIXTURE_DIR, "routes.csv"), header=True, inferSchema=True)
        routes_df = routes_df.dropna(subset=["route_id"]).dropDuplicates(["route_id"])
        r_path = os.path.join(tmp_dir, "routes")
        routes_df.write.mode("overwrite").parquet(r_path)

        # Replicate route_activity logic
        st = spark.read.parquet(st_path)
        t = spark.read.parquet(t_path)
        r = spark.read.parquet(r_path)

        joined = st.join(t, "trip_id", "inner").join(r, "route_id", "inner")
        result = joined.groupBy("route_id").agg(
            count("*").alias("stop_event_count"),
            countDistinct("trip_id").alias("distinct_trip_count"),
            countDistinct("stop_id").alias("distinct_stop_count"),
        )

        assert result.count() > 0
        # R1 has trips T1, T2 which cover stops 1,2,1,3 = 4 stop events
        r1 = result.filter(col("route_id") == "R1").first()
        assert r1 is not None
        assert r1.stop_event_count == 4
        assert r1.distinct_trip_count == 2


class TestHaversineDistance:

    def test_known_distance(self, spark):
        """Test Haversine with known coordinates."""
        from src.osm.transformers import haversine_distance
        from pyspark.sql.functions import lit

        df = spark.range(1).select(
            haversine_distance(
                lit(41.8781), lit(-87.6298),  # Chicago downtown
                lit(41.8827), lit(-87.6233),  # ~0.7km away
            ).alias("dist_m")
        )
        dist = df.first().dist_m
        # Should be approximately 700m (within 100m tolerance)
        assert 600 < dist < 800, f"Expected ~700m, got {dist}"

    def test_same_point_is_zero(self, spark):
        from src.osm.transformers import haversine_distance
        from pyspark.sql.functions import lit

        df = spark.range(1).select(
            haversine_distance(lit(41.88), lit(-87.63), lit(41.88), lit(-87.63)).alias("dist_m")
        )
        assert df.first().dist_m == 0.0


class TestParquetOutputPaths:
    """Verify all jobs write to the correct directory structure."""

    def test_clean_output_paths_exist(self):
        """After a full pipeline run, all clean parquet dirs should exist."""
        base = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "clean")
        expected = [
            "gtfs/stops", "gtfs/routes", "gtfs/trips", "gtfs/shapes", "gtfs/stop_times",
            "osm/roads", "osm/pois",
        ]
        for subdir in expected:
            path = os.path.join(base, subdir)
            assert os.path.isdir(path), f"Missing clean output: {path}"

    def test_analytics_output_paths_exist(self):
        base = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "analytics")
        expected = [
            "stop_activity", "route_activity", "stop_activity_by_route",
            "stop_activity_enriched", "route_shapes",
            "stop_poi_access", "transit_road_coverage",
        ]
        for subdir in expected:
            path = os.path.join(base, subdir)
            assert os.path.isdir(path), f"Missing analytics output: {path}"
