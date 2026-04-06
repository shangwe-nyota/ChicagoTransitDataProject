import sys
import os

sys.path.insert(0, os.getcwd())

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, abs as spark_abs, lit, round as spark_round,
    max as spark_max,
)
from src.osm.transformers import haversine_distance

ROADS_DIR = "data/processed/clean/osm/roads"
STOPS_DIR = "data/processed/clean/gtfs/stops"
OUTPUT_DIR = "data/processed/analytics/transit_road_coverage"

MAX_DISTANCE_M = 200
LAT_THRESHOLD = 0.002
LON_THRESHOLD = 0.003


def main():
    spark = (
        SparkSession.builder
        .appName("Transit Road Coverage Analytics")
        .getOrCreate()
    )

    roads = spark.read.parquet(ROADS_DIR).alias("r")
    stops = spark.read.parquet(STOPS_DIR).alias("s")

    print(f"Roads: {roads.count()}, Stops: {stops.count()}")

    # For each road, check if any stop is within 200m of its start point
    joined = (
        roads.crossJoin(stops)
        .filter(
            (spark_abs(col("r.start_lat") - col("s.stop_lat")) < LAT_THRESHOLD)
            & (spark_abs(col("r.start_lon") - col("s.stop_lon")) < LON_THRESHOLD)
        )
        .withColumn(
            "distance_m",
            haversine_distance(
                col("r.start_lat"), col("r.start_lon"),
                col("s.stop_lat"), col("s.stop_lon"),
            ),
        )
        .filter(col("distance_m") <= MAX_DISTANCE_M)
    )

    # Get distinct roads that have at least one nearby stop
    covered_roads = (
        joined
        .select(col("r.osm_id"), col("r.highway"), col("r.length_m"))
        .dropDuplicates(["osm_id"])
    )

    # Total roads by highway type
    total_by_type = (
        roads.groupBy("highway")
        .agg(
            count("*").alias("total_road_segments"),
            spark_round(spark_sum("length_m") / 1000, 2).alias("total_length_km"),
        )
    )

    # Covered roads by highway type
    covered_by_type = (
        covered_roads.groupBy("highway")
        .agg(
            count("*").alias("road_segments_near_transit"),
            spark_round(spark_sum("length_m") / 1000, 2).alias("covered_length_km"),
        )
    )

    # Join totals with covered
    result = (
        total_by_type.alias("t")
        .join(covered_by_type.alias("c"), "highway", "left")
        .select(
            col("highway"),
            col("total_road_segments"),
            col("road_segments_near_transit"),
            spark_round(
                col("road_segments_near_transit") / col("total_road_segments") * 100, 2
            ).alias("coverage_pct"),
            col("total_length_km"),
            col("covered_length_km"),
        )
        .orderBy(col("coverage_pct").desc())
    )

    print(f"Result row count: {result.count()}")
    result.show(20, truncate=False)

    result.write.mode("overwrite").parquet(OUTPUT_DIR)
    print(f"Saved transit road coverage to {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
