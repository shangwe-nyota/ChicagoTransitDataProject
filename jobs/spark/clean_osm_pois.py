from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

RAW_PATH = "data/raw/osm/pois.csv"
OUTPUT_DIR = "data/processed/clean/osm/pois"

RELEVANT_AMENITIES = [
    "school", "hospital", "clinic", "library", "pharmacy",
    "restaurant", "cafe", "bank", "place_of_worship",
    "community_centre", "police", "fire_station", "post_office",
]


def main():
    spark = (
        SparkSession.builder
        .appName("Clean OSM POIs")
        .getOrCreate()
    )

    pois_df = spark.read.csv(RAW_PATH, header=True, inferSchema=True)

    print("Raw schema:")
    pois_df.printSchema()
    print(f"Raw row count: {pois_df.count()}")

    cleaned = (
        pois_df
        .select(
            col("osm_id").cast("string").alias("osm_id"),
            trim(col("name")).alias("name"),
            trim(col("amenity")).alias("amenity"),
            col("lat").cast("double").alias("lat"),
            col("lon").cast("double").alias("lon"),
        )
        .dropna(subset=["osm_id", "amenity", "lat", "lon"])
        .filter(col("amenity").isin(RELEVANT_AMENITIES))
        .dropDuplicates(["osm_id"])
    )

    print(f"Cleaned row count: {cleaned.count()}")
    cleaned.show(10, truncate=False)

    cleaned.write.mode("overwrite").parquet(OUTPUT_DIR)
    print(f"Cleaned POIs written to: {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
