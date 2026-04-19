from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, trim

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.paths import clean_gtfs_dir, raw_gtfs_dir


def clean_stops(spark: SparkSession, raw_dir: Path, city: str) -> DataFrame:
    stops_df = spark.read.csv(str(raw_dir / "stops.txt"), header=True, inferSchema=True)
    return (
        stops_df.select(
            lit(city).alias("city"),
            trim(col("stop_id")).cast("string").alias("stop_id"),
            trim(col("stop_name")).alias("stop_name"),
            col("stop_lat").cast("double").alias("stop_lat"),
            col("stop_lon").cast("double").alias("stop_lon"),
            col("location_type").cast("int").alias("location_type"),
            trim(col("parent_station")).cast("string").alias("parent_station"),
        )
        .dropna(subset=["stop_id", "stop_name", "stop_lat", "stop_lon"])
        .dropDuplicates(["city", "stop_id"])
    )


def clean_routes(spark: SparkSession, raw_dir: Path, city: str) -> DataFrame:
    routes_df = spark.read.csv(str(raw_dir / "routes.txt"), header=True, inferSchema=True)
    return (
        routes_df.select(
            lit(city).alias("city"),
            trim(col("route_id")).cast("string").alias("route_id"),
            trim(col("route_short_name")).alias("route_short_name"),
            trim(col("route_long_name")).alias("route_long_name"),
            col("route_type").cast("int").alias("route_type"),
        )
        .dropna(subset=["route_id"])
        .dropDuplicates(["city", "route_id"])
    )


def clean_trips(spark: SparkSession, raw_dir: Path, city: str) -> DataFrame:
    trips_df = spark.read.csv(str(raw_dir / "trips.txt"), header=True, inferSchema=True)
    return (
        trips_df.select(
            lit(city).alias("city"),
            trim(col("route_id")).cast("string").alias("route_id"),
            trim(col("service_id").cast("string")).alias("service_id"),
            trim(col("trip_id").cast("string")).alias("trip_id"),
            col("direction_id").cast("int").alias("direction_id"),
            trim(col("shape_id").cast("string")).alias("shape_id"),
            trim(col("direction")).alias("direction"),
            col("wheelchair_accessible").cast("int").alias("wheelchair_accessible"),
            trim(col("schd_trip_id")).alias("schd_trip_id"),
        )
        .dropna(subset=["route_id", "trip_id"])
        .dropDuplicates(["city", "trip_id"])
    )


def clean_stop_times(spark: SparkSession, raw_dir: Path, city: str) -> DataFrame:
    stop_times_df = spark.read.csv(str(raw_dir / "stop_times.txt"), header=True, inferSchema=True)
    return (
        stop_times_df.select(
            lit(city).alias("city"),
            trim(col("trip_id")).cast("string").alias("trip_id"),
            trim(col("arrival_time")).alias("arrival_time"),
            trim(col("departure_time")).alias("departure_time"),
            trim(col("stop_id")).cast("string").alias("stop_id"),
            col("stop_sequence").cast("int").alias("stop_sequence"),
        )
        .dropna(subset=["trip_id", "stop_id", "stop_sequence"])
    )


def clean_shapes(spark: SparkSession, raw_dir: Path, city: str) -> DataFrame:
    shapes_df = spark.read.csv(str(raw_dir / "shapes.txt"), header=True, inferSchema=True)
    return (
        shapes_df.select(
            lit(city).alias("city"),
            trim(col("shape_id").cast("string")).alias("shape_id"),
            col("shape_pt_lat").cast("double").alias("shape_pt_lat"),
            col("shape_pt_lon").cast("double").alias("shape_pt_lon"),
            col("shape_pt_sequence").cast("int").alias("shape_pt_sequence"),
        )
        .dropna(subset=["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"])
    )


def main() -> None:
    args = parse_args()
    city = args.city.lower()
    raw_dir = raw_gtfs_dir(city)

    spark = SparkSession.builder.appName(f"Clean GTFS City - {city}").getOrCreate()

    datasets = {
        "stops": clean_stops(spark, raw_dir, city),
        "routes": clean_routes(spark, raw_dir, city),
        "trips": clean_trips(spark, raw_dir, city),
        "stop_times": clean_stop_times(spark, raw_dir, city),
        "shapes": clean_shapes(spark, raw_dir, city),
    }

    for dataset_name, df in datasets.items():
        output_dir = clean_gtfs_dir(city, dataset_name)
        print(f"Writing {dataset_name} for {city} to {output_dir}")
        df.write.mode("overwrite").parquet(str(output_dir))

    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean GTFS batch data for a city.")
    parser.add_argument("--city", required=True, choices=["chicago", "boston"])
    return parser.parse_args()


if __name__ == "__main__":
    main()
