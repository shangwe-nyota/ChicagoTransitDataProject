from pyspark.sql import SparkSession
from pyspark.sql.functions import col

TRIPS_PATH = "data/processed/clean/gtfs/trips"
ROUTES_PATH = "data/processed/clean/gtfs/routes"
SHAPES_PATH = "data/processed/clean/gtfs/shapes"
OUTPUT_DIR = "data/processed/analytics/route_shapes"


def main():
    spark = (
        SparkSession.builder
        .appName("Route Shapes Analytics")
        .getOrCreate()
    )

    trips_df = spark.read.parquet(TRIPS_PATH)
    routes_df = spark.read.parquet(ROUTES_PATH)
    shapes_df = spark.read.parquet(SHAPES_PATH)

    # Join trips -> routes to attach route metadata to each shape_id
    trip_routes_df = (
        trips_df.alias("t")
        .join(
            routes_df.alias("r"),
            on="route_id",
            how="inner"
        )
        .select(
            col("route_id"),
            col("route_short_name"),
            col("route_long_name"),
            col("route_type"),
            col("shape_id")
        )
        .dropna(subset=["shape_id"])
        .dropDuplicates(["route_id", "shape_id"])
    )

    # Join with shapes to get the actual geometry points
    route_shapes_df = (
        trip_routes_df.alias("tr")
        .join(
            shapes_df.alias("s"),
            on="shape_id",
            how="inner"
        )
        .select(
            col("route_id"),
            col("route_short_name"),
            col("route_long_name"),
            col("route_type"),
            col("shape_id"),
            col("shape_pt_sequence"),
            col("shape_pt_lat"),
            col("shape_pt_lon")
        )
        .orderBy(col("route_id"), col("shape_id"), col("shape_pt_sequence"))
    )

    print("Top 20 route shape points:")
    route_shapes_df.show(20, truncate=False)

    print("Route shapes row count:")
    print(route_shapes_df.count())

    route_shapes_df.write.mode("overwrite").parquet(OUTPUT_DIR)

    print(f"Saved route shapes to {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
