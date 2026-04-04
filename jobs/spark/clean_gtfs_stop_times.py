from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
import os

RAW_GTFS_DIR = "data/raw/gtfs"
OUTPUT_DIR = "data/processed/clean/gtfs/stop_times"


def main():
    spark = (
        SparkSession.builder
        .appName("Clean GTFS Stop Times")
        .getOrCreate()
    )

    stop_times_path = os.path.join(RAW_GTFS_DIR, "stop_times.txt")
    stop_times_df = spark.read.csv(stop_times_path, header=True, inferSchema=True)

    print("Raw stop_times schema:")
    stop_times_df.printSchema()

    print("Raw stop_times row count:")
    print(stop_times_df.count())

    cleaned_stop_times_df = (
        stop_times_df
        .select(
            trim(col("trip_id")).cast("string").alias("trip_id"),
            trim(col("arrival_time")).alias("arrival_time"),
            trim(col("departure_time")).alias("departure_time"),
            trim(col("stop_id")).cast("string").alias("stop_id"),
            col("stop_sequence").cast("int").alias("stop_sequence")
        )
        .dropna(subset=["trip_id", "stop_id", "stop_sequence"])
    )

    print("Cleaned stop_times schema:")
    cleaned_stop_times_df.printSchema()

    print("Cleaned stop_times row count:")
    print(cleaned_stop_times_df.count())

    cleaned_stop_times_df.show(10, truncate=False)

    cleaned_stop_times_df.write.mode("overwrite").parquet(OUTPUT_DIR)

    print(f"Cleaned stop_times written to: {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
