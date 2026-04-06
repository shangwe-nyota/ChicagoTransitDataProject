import sys
import os

sys.path.insert(0, os.getcwd())

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, abs as spark_abs, collect_set, concat_ws,
    when, min as spark_min,
)
from src.osm.transformers import haversine_distance

STOPS_DIR = "data/processed/clean/gtfs/stops"
POIS_DIR = "data/processed/clean/osm/pois"
OUTPUT_DIR = "data/processed/analytics/stop_poi_access"

MAX_DISTANCE_M = 400
# Bounding box pre-filter in degrees (~400m at Chicago's latitude)
LAT_THRESHOLD = 0.004
LON_THRESHOLD = 0.005


def main():
    spark = (
        SparkSession.builder
        .appName("Stop POI Access Analytics")
        .getOrCreate()
    )

    stops = spark.read.parquet(STOPS_DIR).alias("s")
    pois = spark.read.parquet(POIS_DIR).alias("p")

    print(f"Stops: {stops.count()}, POIs: {pois.count()}")

    # Cross join with bounding box pre-filter
    joined = (
        stops.crossJoin(pois)
        .filter(
            (spark_abs(col("s.stop_lat") - col("p.lat")) < LAT_THRESHOLD)
            & (spark_abs(col("s.stop_lon") - col("p.lon")) < LON_THRESHOLD)
        )
    )

    # Compute Haversine distance
    joined = joined.withColumn(
        "distance_m",
        haversine_distance(col("s.stop_lat"), col("s.stop_lon"), col("p.lat"), col("p.lon")),
    ).filter(col("distance_m") <= MAX_DISTANCE_M)

    # Aggregate per stop
    result = (
        joined.groupBy(
            col("s.stop_id").alias("stop_id"),
            col("s.stop_name").alias("stop_name"),
            col("s.stop_lat").alias("stop_lat"),
            col("s.stop_lon").alias("stop_lon"),
        )
        .agg(
            count("*").alias("poi_count_within_400m"),
            spark_min(
                when(col("p.amenity") == "school", col("distance_m"))
            ).alias("nearest_school_m"),
            spark_min(
                when(col("p.amenity") == "hospital", col("distance_m"))
            ).alias("nearest_hospital_m"),
            concat_ws(", ", collect_set(col("p.amenity"))).alias("amenity_types"),
        )
        .orderBy(col("poi_count_within_400m").desc())
    )

    print(f"Result row count: {result.count()}")
    result.show(20, truncate=False)

    result.write.mode("overwrite").parquet(OUTPUT_DIR)
    print(f"Saved stop POI access to {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
