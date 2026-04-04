from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

STOP_TIMES_PATH = "data/processed/clean/gtfs/stop_times"
TRIPS_PATH = "data/processed/clean/gtfs/trips"
ROUTES_PATH = "data/processed/clean/gtfs/routes"
OUTPUT_DIR = "data/processed/analytics/route_activity"


def main():
    spark = (
        SparkSession.builder
        .appName("Route Activity Analytics")
        .getOrCreate()
    )

    stop_times_df = spark.read.parquet(STOP_TIMES_PATH)
    trips_df = spark.read.parquet(TRIPS_PATH)
    routes_df = spark.read.parquet(ROUTES_PATH)

    joined_df = (
        stop_times_df.alias("st")
        .join(
            trips_df.alias("t"),
            on="trip_id",
            how="inner"
        )
        .join(
            routes_df.alias("r"),
            on="route_id",
            how="inner"
        )
    )

    route_activity_df = (
        joined_df
        .groupBy(
            col("route_id"),
            col("route_short_name"),
            col("route_long_name"),
            col("route_type")
        )
        .agg(
            count("*").alias("stop_event_count"),
            countDistinct("trip_id").alias("distinct_trip_count"),
            countDistinct("stop_id").alias("distinct_stop_count")
        )
        .orderBy(col("stop_event_count").desc())
    )

    print("Top 20 busiest routes:")
    route_activity_df.show(20, truncate=False)

    route_activity_df.write.mode("overwrite").parquet(OUTPUT_DIR)

    print(f"Saved route activity to {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
