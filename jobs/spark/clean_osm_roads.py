from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

RAW_PATH = "data/raw/osm/roads.csv"
OUTPUT_DIR = "data/processed/clean/osm/roads"

RELEVANT_HIGHWAY_TYPES = [
    "motorway", "trunk", "primary", "secondary",
    "tertiary", "residential", "unclassified",
]


def main():
    spark = (
        SparkSession.builder
        .appName("Clean OSM Roads")
        .getOrCreate()
    )

    roads_df = spark.read.csv(RAW_PATH, header=True, inferSchema=True)

    print("Raw schema:")
    roads_df.printSchema()
    print(f"Raw row count: {roads_df.count()}")

    cleaned = (
        roads_df
        .select(
            col("osm_id").cast("string").alias("osm_id"),
            trim(col("name")).alias("name"),
            trim(col("highway")).alias("highway"),
            col("start_lat").cast("double").alias("start_lat"),
            col("start_lon").cast("double").alias("start_lon"),
            col("end_lat").cast("double").alias("end_lat"),
            col("end_lon").cast("double").alias("end_lon"),
            col("length_m").cast("double").alias("length_m"),
        )
        .dropna(subset=["osm_id", "start_lat", "start_lon", "end_lat", "end_lon"])
        .filter(col("highway").isin(RELEVANT_HIGHWAY_TYPES))
        .dropDuplicates(["osm_id"])
    )

    print(f"Cleaned row count: {cleaned.count()}")
    cleaned.show(10, truncate=False)

    cleaned.write.mode("overwrite").parquet(OUTPUT_DIR)
    print(f"Cleaned roads written to: {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
