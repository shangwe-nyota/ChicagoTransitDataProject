from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
import os

RAW_GTFS_DIR = "data/raw/gtfs"
OUTPUT_DIR = "data/processed/clean/gtfs/stops"


def main():
    spark = (
        SparkSession.builder
        .appName("Clean GTFS Stops")
        .getOrCreate()
    )

    stops_path = os.path.join(RAW_GTFS_DIR, "stops.txt")
    stops_df = spark.read.csv(stops_path, header=True, inferSchema=True)

    print("Raw schema:")
    stops_df.printSchema()

    print("Raw row count:")
    print(stops_df.count())

    cleaned_stops_df = (
        stops_df
        .select(
            trim(col("stop_id")).cast("string").alias("stop_id"),
            trim(col("stop_name")).alias("stop_name"),
            col("stop_lat").cast("double").alias("stop_lat"),
            col("stop_lon").cast("double").alias("stop_lon"),
            col("location_type").cast("int").alias("location_type"),
            trim(col("parent_station")).cast("string").alias("parent_station")
        )
        .dropna(subset=["stop_id", "stop_name", "stop_lat", "stop_lon"])
        .dropDuplicates(["stop_id"])
    )

    print("Cleaned schema:")
    cleaned_stops_df.printSchema()

    print("Cleaned row count:")
    print(cleaned_stops_df.count())

    print("Sample cleaned rows:")
    cleaned_stops_df.show(10, truncate=False)

    cleaned_stops_df.write.mode("overwrite").parquet(OUTPUT_DIR)  # Idempotent writes (will not append to dir)

    print(f"Cleaned stops written to: {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
