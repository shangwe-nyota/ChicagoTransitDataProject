from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
import os

RAW_GTFS_DIR = "data/raw/gtfs"
OUTPUT_DIR = "data/processed/clean/gtfs/routes"


def main():
    spark = (
        SparkSession.builder
        .appName("Clean GTFS Routes")
        .getOrCreate()
    )

    routes_path = os.path.join(RAW_GTFS_DIR, "routes.txt")
    routes_df = spark.read.csv(routes_path, header=True, inferSchema=True)

    print("Raw routes schema:")
    routes_df.printSchema()

    print("Raw routes row count:")
    print(routes_df.count())

    cleaned_routes_df = (
        routes_df
        .select(
            trim(col("route_id")).cast("string").alias("route_id"),
            trim(col("route_short_name")).alias("route_short_name"),
            trim(col("route_long_name")).alias("route_long_name"),
            col("route_type").cast("int").alias("route_type")
        )
        .dropna(subset=["route_id"])
        .dropDuplicates(["route_id"])
    )

    print("Cleaned routes schema:")
    cleaned_routes_df.printSchema()

    print("Cleaned routes row count:")
    print(cleaned_routes_df.count())

    cleaned_routes_df.show(10, truncate=False)

    cleaned_routes_df.write.mode("overwrite").parquet(OUTPUT_DIR)

    print(f"Cleaned routes written to: {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
