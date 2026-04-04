from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

STOP_TIMES_PATH = "data/processed/clean/gtfs/stop_times"
TRIPS_PATH = "data/processed/clean/gtfs/trips"
ROUTES_PATH = "data/processed/clean/gtfs/routes"
STOPS_PATH = "data/processed/clean/gtfs/stops"
OUTPUT_DIR = "data/processed/analytics/stop_activity_by_route"


def main():
    spark = (
        SparkSession.builder
        .appName("Stop Activity By Route")
        .getOrCreate()
    )

    stop_times_df = spark.read.parquet(STOP_TIMES_PATH)
    trips_df = spark.read.parquet(TRIPS_PATH)
    routes_df = spark.read.parquet(ROUTES_PATH)
    stops_df = spark.read.parquet(STOPS_PATH)

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
        .join(
            stops_df.alias("s"),
            on="stop_id",
            how="inner"
        )
    )

    stop_activity_by_route_df = (
        joined_df
        .groupBy(
            col("route_id"),
            col("route_short_name"),
            col("route_long_name"),
            col("stop_id"),
            col("stop_name"),
            col("stop_lat"),
            col("stop_lon")
        )
        .agg(
            count("*").alias("trip_count")
        )
        .orderBy(col("trip_count").desc())
    )

    print("Top 20 busiest route-stop combinations:")
    stop_activity_by_route_df.show(20, truncate=False)

    stop_activity_by_route_df.write.mode("overwrite").parquet(OUTPUT_DIR)

    print(f"Saved stop activity by route to {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
