from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
import os

RAW_GTFS_DIR = "data/raw/gtfs"
OUTPUT_DIR = "data/processed/clean/gtfs/shapes"


def main():
    spark = (
        SparkSession.builder
        .appName("Clean GTFS Shapes")
        .getOrCreate()
    )

    shapes_path = os.path.join(RAW_GTFS_DIR, "shapes.txt")
    shapes_df = spark.read.csv(shapes_path, header=True, inferSchema=True)

    print("Raw shapes schema:")
    shapes_df.printSchema()

    print("Raw shapes row count:")
    print(shapes_df.count())

    cleaned_shapes_df = (
        shapes_df
        .select(
            trim(col("shape_id").cast("string")).alias("shape_id"),
            col("shape_pt_lat").cast("double").alias("shape_pt_lat"),
            col("shape_pt_lon").cast("double").alias("shape_pt_lon"),
            col("shape_pt_sequence").cast("int").alias("shape_pt_sequence")
        )
        .dropna(subset=["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"])
    )

    print("Cleaned shapes schema:")
    cleaned_shapes_df.printSchema()

    print("Cleaned shapes row count:")
    print(cleaned_shapes_df.count())

    cleaned_shapes_df.show(10, truncate=False)

    cleaned_shapes_df.write.mode("overwrite").parquet(OUTPUT_DIR)

    print(f"Cleaned shapes written to: {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
