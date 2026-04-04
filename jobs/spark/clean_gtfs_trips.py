from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
import os

RAW_GTFS_DIR = "data/raw/gtfs"
OUTPUT_DIR = "data/processed/clean/gtfs/trips"


def main():
    spark = (
        SparkSession.builder
        .appName("Clean GTFS Trips")
        .getOrCreate()
    )

    trips_path = os.path.join(RAW_GTFS_DIR, "trips.txt")
    trips_df = spark.read.csv(trips_path, header=True, inferSchema=True)

    print("Raw trips schema:")
    trips_df.printSchema()

    print("Raw trips row count:")
    print(trips_df.count())

    cleaned_trips_df = (
        trips_df
        .select(
            trim(col("route_id")).cast("string").alias("route_id"),
            trim(col("service_id").cast("string")).alias("service_id"),
            trim(col("trip_id").cast("string")).alias("trip_id"),
            col("direction_id").cast("int").alias("direction_id"),
            trim(col("shape_id").cast("string")).alias("shape_id"),
            trim(col("direction")).alias("direction"),
            col("wheelchair_accessible").cast("int").alias("wheelchair_accessible"),
            trim(col("schd_trip_id")).alias("schd_trip_id")
        )
        .dropna(subset=["route_id", "trip_id"])
        .dropDuplicates(["trip_id"])
    )

    print("Cleaned trips schema:")
    cleaned_trips_df.printSchema()

    print("Cleaned trips row count:")
    print(cleaned_trips_df.count())

    cleaned_trips_df.show(10, truncate=False)

    cleaned_trips_df.write.mode("overwrite").parquet(OUTPUT_DIR)

    print(f"Cleaned trips written to: {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()