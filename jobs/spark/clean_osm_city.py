from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, trim, when

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.constants import OSM_RELEVANT_HIGHWAY_TYPES, OSM_RELEVANT_POI_CATEGORY_MAP
from src.common.paths import clean_osm_dir, raw_osm_dir


def main() -> None:
    args = parse_args()
    city = args.city.lower()

    spark = SparkSession.builder.appName(f"Clean OSM City - {city}").getOrCreate()

    raw_dir = raw_osm_dir(city)
    roads_df = spark.read.csv(str(raw_dir / "roads.csv"), header=True, inferSchema=True)
    pois_df = spark.read.csv(str(raw_dir / "pois.csv"), header=True, inferSchema=True)

    cleaned_roads = (
        roads_df.select(
            lit(city).alias("city"),
            col("osm_id").cast("string").alias("osm_id"),
            trim(col("name")).alias("name"),
            trim(col("highway")).alias("highway"),
            col("start_lat").cast("double").alias("start_lat"),
            col("start_lon").cast("double").alias("start_lon"),
            col("end_lat").cast("double").alias("end_lat"),
            col("end_lon").cast("double").alias("end_lon"),
            col("mid_lat").cast("double").alias("mid_lat"),
            col("mid_lon").cast("double").alias("mid_lon"),
            col("length_m").cast("double").alias("length_m"),
        )
        .dropna(subset=["osm_id", "start_lat", "start_lon", "end_lat", "end_lon", "mid_lat", "mid_lon"])
        .filter(col("highway").isin(OSM_RELEVANT_HIGHWAY_TYPES))
        .dropDuplicates(["city", "osm_id"])
    )

    cleaned_pois = (
        pois_df.select(
            lit(city).alias("city"),
            col("osm_id").cast("string").alias("osm_id"),
            trim(col("name")).alias("name"),
            trim(col("tag_key")).alias("tag_key"),
            trim(col("tag_value")).alias("tag_value"),
            trim(col("poi_category")).alias("poi_category"),
            col("lat").cast("double").alias("lat"),
            col("lon").cast("double").alias("lon"),
        )
        .dropna(subset=["osm_id", "poi_category", "lat", "lon"])
        .filter(col("poi_category").isin(list(OSM_RELEVANT_POI_CATEGORY_MAP.keys())))
        .dropDuplicates(["city", "osm_id"])
        .withColumn(
            "poi_group",
            lit(None).cast("string"),
        )
    )

    poi_group_expr = lit(None).cast("string")
    for poi_category, poi_group in OSM_RELEVANT_POI_CATEGORY_MAP.items():
        poi_group_expr = when(col("poi_category") == poi_category, lit(poi_group)).otherwise(poi_group_expr)

    cleaned_pois = cleaned_pois.withColumn("poi_group", poi_group_expr).fillna({"poi_group": "other"})

    cleaned_roads.write.mode("overwrite").parquet(str(clean_osm_dir(city, "roads")))
    cleaned_pois.write.mode("overwrite").parquet(str(clean_osm_dir(city, "pois")))

    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean OSM batch data for a city.")
    parser.add_argument("--city", required=True, choices=["chicago", "boston"])
    return parser.parse_args()


if __name__ == "__main__":
    main()
