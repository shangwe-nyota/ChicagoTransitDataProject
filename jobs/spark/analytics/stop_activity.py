from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import os

RAW_GTFS_DIR = "data/raw/gtfs"
OUTPUT_DIR = "data/processed/analytics/stop_activity"


# Want to bild an analyitics table
# Which stops have the most trips passing through?

def main():
    spark = (
        SparkSession.builder
        .appName("Stop Activity Analytics")
        .getOrCreate()
    )

    # Load stop_times (Massive file)
    stop_times_path = os.path.join(RAW_GTFS_DIR, "stop_times.txt")
    df = spark.read.csv(stop_times_path, header=True, inferSchema=True)

    print("Loaded stop_times")
    print(df.count())

    # Aggregate: how many times each stop appears
    activity_df = (
        df
        .groupBy("stop_id")
        .agg(count("*").alias("trip_count"))
        .orderBy(col("trip_count").desc())
    )

    print("Top 10 busiest stops:")
    activity_df.show(10)

    # Save result
    activity_df.write.mode("overwrite").parquet(OUTPUT_DIR)  # Writing to analytics parquet

    print(f"Saved analytics to {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
