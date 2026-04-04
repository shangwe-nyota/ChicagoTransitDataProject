from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

STOPS_PATH = "data/processed/clean/gtfs/stops"
STOP_ACTIVITY_PATH = "data/processed/analytics/stop_activity"
OUTPUT_DIR = "data/processed/analytics/stop_activity_enriched"


def main():
    spark = (
        SparkSession.builder
        .appName("Join Stop Activity With Stops")
        .getOrCreate()
    )

    stops_df = spark.read.parquet(STOPS_PATH)
    activity_df = spark.read.parquet(STOP_ACTIVITY_PATH)

    enriched_df = (
        activity_df.alias("a")
        .join(
            stops_df.alias("s"),
            on="stop_id",
            how="inner"
        )
        .select(
            col("stop_id"),
            col("s.stop_name").alias("stop_name"),
            col("s.stop_lat").alias("stop_lat"),
            col("s.stop_lon").alias("stop_lon"),
            col("a.trip_count").alias("trip_count"),
            col("s.location_type").alias("location_type"),
            col("s.parent_station").alias("parent_station")
        )
        .orderBy(col("trip_count").desc())
    )

    print("Top 20 enriched busiest stops:")
    enriched_df.show(20, truncate=False)

    enriched_df.write.mode("overwrite").parquet(OUTPUT_DIR) # Write parquet file (joined stop activity with stops)

    print(f"Saved enriched stop activity to {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
