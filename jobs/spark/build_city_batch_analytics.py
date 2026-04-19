from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    collect_set,
    col,
    concat_ws,
    count,
    countDistinct,
    lit,
    max as spark_max,
    mean as spark_mean,
    min as spark_min,
    round as spark_round,
    sum as spark_sum,
    when,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.constants import (
    STOP_POI_ACCESS_DISTANCE_M,
    TRANSIT_ROAD_COVERAGE_DISTANCE_M,
)
from src.common.paths import analytics_dir, clean_gtfs_dir, clean_osm_dir
from src.osm.transformers import haversine_distance


def stop_activity(stop_times_df: DataFrame) -> DataFrame:
    return (
        stop_times_df.groupBy("city", "stop_id")
        .agg(count("*").alias("trip_count"))
        .orderBy(col("trip_count").desc())
    )


def stop_activity_enriched(stops_df: DataFrame, stop_activity_df: DataFrame) -> DataFrame:
    return (
        stop_activity_df.alias("a")
        .join(stops_df.alias("s"), on=["city", "stop_id"], how="inner")
        .select(
            col("city"),
            col("stop_id"),
            col("s.stop_name").alias("stop_name"),
            col("s.stop_lat").alias("stop_lat"),
            col("s.stop_lon").alias("stop_lon"),
            col("a.trip_count").alias("trip_count"),
            col("s.location_type").alias("location_type"),
            col("s.parent_station").alias("parent_station"),
        )
        .orderBy(col("trip_count").desc())
    )


def route_activity(stop_times_df: DataFrame, trips_df: DataFrame, routes_df: DataFrame) -> DataFrame:
    joined_df = (
        stop_times_df.alias("st")
        .join(trips_df.alias("t"), on=["city", "trip_id"], how="inner")
        .join(routes_df.alias("r"), on=["city", "route_id"], how="inner")
    )
    return (
        joined_df.groupBy("city", "route_id", "route_short_name", "route_long_name", "route_type")
        .agg(
            count("*").alias("stop_event_count"),
            countDistinct("trip_id").alias("distinct_trip_count"),
            countDistinct("stop_id").alias("distinct_stop_count"),
        )
        .orderBy(col("stop_event_count").desc())
    )


def stop_activity_by_route(
    stop_times_df: DataFrame,
    trips_df: DataFrame,
    routes_df: DataFrame,
    stops_df: DataFrame,
) -> DataFrame:
    joined_df = (
        stop_times_df.alias("st")
        .join(trips_df.alias("t"), on=["city", "trip_id"], how="inner")
        .join(routes_df.alias("r"), on=["city", "route_id"], how="inner")
        .join(stops_df.alias("s"), on=["city", "stop_id"], how="inner")
    )
    return (
        joined_df.groupBy(
            "city",
            "route_id",
            "route_short_name",
            "route_long_name",
            "route_type",
            "stop_id",
            "stop_name",
            "stop_lat",
            "stop_lon",
        )
        .agg(count("*").alias("trip_count"))
        .orderBy(col("trip_count").desc())
    )


def route_shapes(trips_df: DataFrame, routes_df: DataFrame, shapes_df: DataFrame) -> DataFrame:
    trip_routes_df = (
        trips_df.alias("t")
        .join(routes_df.alias("r"), on=["city", "route_id"], how="inner")
        .select("city", "route_id", "route_short_name", "route_long_name", "route_type", "shape_id")
        .dropna(subset=["shape_id"])
        .dropDuplicates(["city", "route_id", "shape_id"])
    )
    return (
        trip_routes_df.alias("tr")
        .join(shapes_df.alias("s"), on=["city", "shape_id"], how="inner")
        .select(
            "city",
            "route_id",
            "route_short_name",
            "route_long_name",
            "route_type",
            "shape_id",
            "shape_pt_sequence",
            "shape_pt_lat",
            "shape_pt_lon",
        )
        .orderBy("route_id", "shape_id", "shape_pt_sequence")
    )


def stop_poi_access(stops_df: DataFrame, pois_df: DataFrame, city: str) -> DataFrame:
    lat_threshold = 0.004
    lon_threshold = 0.005

    joined = (
        stops_df.alias("s")
        .crossJoin(pois_df.alias("p"))
        .filter(
            (spark_abs(col("s.stop_lat") - col("p.lat")) < lat_threshold)
            & (spark_abs(col("s.stop_lon") - col("p.lon")) < lon_threshold)
        )
        .withColumn(
            "distance_m",
            haversine_distance(col("s.stop_lat"), col("s.stop_lon"), col("p.lat"), col("p.lon")),
        )
        .filter(col("distance_m") <= STOP_POI_ACCESS_DISTANCE_M)
    )

    return (
        joined.groupBy("s.city", "s.stop_id", "s.stop_name", "s.stop_lat", "s.stop_lon")
        .agg(
            count("*").alias("poi_count_within_400m"),
            count(when(col("p.poi_group") == "food", 1)).alias("food_poi_count_within_400m"),
            count(when(col("p.poi_group").isin("healthcare", "education", "grocery"), 1)).alias(
                "critical_service_poi_count_within_400m"
            ),
            count(when(col("p.poi_group") == "park", 1)).alias("park_poi_count_within_400m"),
            spark_min(when(col("p.poi_category").isin("school", "university", "college"), col("distance_m"))).alias(
                "nearest_school_m"
            ),
            spark_min(when(col("p.poi_category").isin("hospital", "clinic"), col("distance_m"))).alias(
                "nearest_hospital_m"
            ),
            spark_min(when(col("p.poi_group") == "grocery", col("distance_m"))).alias("nearest_grocery_m"),
            spark_min(when(col("p.poi_group") == "park", col("distance_m"))).alias("nearest_park_m"),
            concat_ws(", ", collect_set(col("p.poi_category"))).alias("poi_categories"),
        )
        .select(
            col("city"),
            col("stop_id"),
            col("stop_name"),
            col("stop_lat"),
            col("stop_lon"),
            col("poi_count_within_400m"),
            col("food_poi_count_within_400m"),
            col("critical_service_poi_count_within_400m"),
            col("park_poi_count_within_400m"),
            col("nearest_school_m"),
            col("nearest_hospital_m"),
            col("nearest_grocery_m"),
            col("nearest_park_m"),
            col("poi_categories"),
        )
        .orderBy(col("poi_count_within_400m").desc())
    )


def busiest_stops_with_poi_context(
    stop_activity_enriched_df: DataFrame,
    stop_poi_access_df: DataFrame,
) -> DataFrame:
    return (
        stop_activity_enriched_df.alias("sa")
        .join(stop_poi_access_df.alias("spa"), on=["city", "stop_id"], how="left")
        .select(
            col("sa.city").alias("city"),
            col("sa.stop_id").alias("stop_id"),
            col("sa.stop_name").alias("stop_name"),
            col("sa.stop_lat").alias("stop_lat"),
            col("sa.stop_lon").alias("stop_lon"),
            col("sa.trip_count").alias("trip_count"),
            col("spa.poi_count_within_400m").alias("poi_count_within_400m"),
            col("spa.food_poi_count_within_400m").alias("food_poi_count_within_400m"),
            col("spa.critical_service_poi_count_within_400m").alias("critical_service_poi_count_within_400m"),
            col("spa.park_poi_count_within_400m").alias("park_poi_count_within_400m"),
            col("spa.nearest_school_m").alias("nearest_school_m"),
            col("spa.nearest_hospital_m").alias("nearest_hospital_m"),
            col("spa.nearest_grocery_m").alias("nearest_grocery_m"),
            col("spa.nearest_park_m").alias("nearest_park_m"),
            col("spa.poi_categories").alias("poi_categories"),
        )
        .orderBy(col("trip_count").desc(), col("poi_count_within_400m").desc_nulls_last())
    )


def route_poi_access(stop_activity_by_route_df: DataFrame, stop_poi_access_df: DataFrame) -> DataFrame:
    joined = (
        stop_activity_by_route_df.alias("sar")
        .join(stop_poi_access_df.alias("spa"), on=["city", "stop_id"], how="left")
    )
    return (
        joined.groupBy("city", "route_id", "route_short_name", "route_long_name", "route_type")
        .agg(
            countDistinct("stop_id").alias("stop_count"),
            spark_sum(when(col("poi_count_within_400m").isNull(), lit(0)).otherwise(col("poi_count_within_400m"))).alias(
                "total_poi_access"
            ),
            spark_round(spark_mean(col("poi_count_within_400m")), 2).alias("avg_poi_access_per_stop"),
            spark_max(col("poi_count_within_400m")).alias("max_poi_access_at_stop"),
            countDistinct(when(col("nearest_hospital_m").isNotNull(), col("stop_id"))).alias("stops_near_hospital"),
            countDistinct(when(col("nearest_grocery_m").isNotNull(), col("stop_id"))).alias("stops_near_grocery"),
            countDistinct(when(col("nearest_park_m").isNotNull(), col("stop_id"))).alias("stops_near_park"),
        )
        .orderBy(col("total_poi_access").desc())
    )


def transit_road_coverage(roads_df: DataFrame, stops_df: DataFrame) -> DataFrame:
    lat_threshold = 0.002
    lon_threshold = 0.003

    joined = (
        roads_df.alias("r")
        .crossJoin(stops_df.alias("s"))
        .filter(
            (spark_abs(col("r.mid_lat") - col("s.stop_lat")) < lat_threshold)
            & (spark_abs(col("r.mid_lon") - col("s.stop_lon")) < lon_threshold)
        )
        .withColumn(
            "distance_m",
            haversine_distance(col("r.mid_lat"), col("r.mid_lon"), col("s.stop_lat"), col("s.stop_lon")),
        )
        .filter(col("distance_m") <= TRANSIT_ROAD_COVERAGE_DISTANCE_M)
    )

    covered_roads = joined.select("r.city", "r.osm_id", "r.highway", "r.length_m").dropDuplicates(["city", "osm_id"])
    total_by_type = (
        roads_df.groupBy("city", "highway")
        .agg(
            count("*").alias("total_road_segments"),
            spark_round(spark_sum("length_m") / 1000, 2).alias("total_length_km"),
        )
    )
    covered_by_type = (
        covered_roads.groupBy("city", "highway")
        .agg(
            count("*").alias("road_segments_near_transit"),
            spark_round(spark_sum("length_m") / 1000, 2).alias("covered_length_km"),
        )
    )

    return (
        total_by_type.alias("t")
        .join(covered_by_type.alias("c"), on=["city", "highway"], how="left")
        .select(
            col("city"),
            col("highway"),
            col("total_road_segments"),
            col("road_segments_near_transit"),
            spark_round(
                col("road_segments_near_transit") / col("total_road_segments") * 100,
                2,
            ).alias("coverage_pct"),
            col("total_length_km"),
            col("covered_length_km"),
        )
        .orderBy(col("coverage_pct").desc_nulls_last())
    )


def write_dataset(df: DataFrame, city: str, dataset_name: str) -> None:
    output_dir = analytics_dir(city, dataset_name)
    print(f"Writing analytics dataset {dataset_name} for {city} -> {output_dir}")
    df.write.mode("overwrite").parquet(str(output_dir))


def main() -> None:
    args = parse_args()
    city = args.city.lower()

    spark = SparkSession.builder.appName(f"Build City Batch Analytics - {city}").getOrCreate()

    stops_df = spark.read.parquet(str(clean_gtfs_dir(city, "stops")))
    routes_df = spark.read.parquet(str(clean_gtfs_dir(city, "routes")))
    trips_df = spark.read.parquet(str(clean_gtfs_dir(city, "trips")))
    stop_times_df = spark.read.parquet(str(clean_gtfs_dir(city, "stop_times")))
    shapes_df = spark.read.parquet(str(clean_gtfs_dir(city, "shapes")))
    roads_df = spark.read.parquet(str(clean_osm_dir(city, "roads")))
    pois_df = spark.read.parquet(str(clean_osm_dir(city, "pois")))

    stop_activity_df = stop_activity(stop_times_df)
    stop_activity_enriched_df = stop_activity_enriched(stops_df, stop_activity_df)
    route_activity_df = route_activity(stop_times_df, trips_df, routes_df)
    stop_activity_by_route_df = stop_activity_by_route(stop_times_df, trips_df, routes_df, stops_df)
    route_shapes_df = route_shapes(trips_df, routes_df, shapes_df)
    stop_poi_access_df = stop_poi_access(stops_df, pois_df, city)
    busiest_stops_with_poi_context_df = busiest_stops_with_poi_context(
        stop_activity_enriched_df,
        stop_poi_access_df,
    )
    route_poi_access_df = route_poi_access(stop_activity_by_route_df, stop_poi_access_df)
    transit_road_coverage_df = transit_road_coverage(roads_df, stops_df)

    outputs = {
        "stop_activity": stop_activity_df,
        "stop_activity_enriched": stop_activity_enriched_df,
        "route_activity": route_activity_df,
        "stop_activity_by_route": stop_activity_by_route_df,
        "route_shapes": route_shapes_df,
        "stop_poi_access": stop_poi_access_df,
        "busiest_stops_with_poi_context": busiest_stops_with_poi_context_df,
        "route_poi_access": route_poi_access_df,
        "transit_road_coverage": transit_road_coverage_df,
    }

    for dataset_name, df in outputs.items():
        write_dataset(df, city, dataset_name)

    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build city-aware GTFS + OSM analytics.")
    parser.add_argument("--city", required=True, choices=["chicago", "boston"])
    return parser.parse_args()


if __name__ == "__main__":
    main()
