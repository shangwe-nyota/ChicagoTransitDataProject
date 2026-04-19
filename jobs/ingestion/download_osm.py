from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.config import get_batch_city_config
from src.common.constants import OSM_POI_TAGS
from src.common.paths import raw_osm_dir
from src.common.run_metadata import StageTracker, generate_run_id


def _normalize_scalar(value):
    if isinstance(value, list):
        return value[0]
    return value


def download_roads(place_name: str, output_path: Path) -> None:
    import osmnx as ox

    print(f"Downloading OSM road network for {place_name}...")
    graph = ox.graph_from_place(place_name, network_type="drive")
    nodes, edges = ox.graph_to_gdfs(graph)

    roads_df = edges.reset_index()[["u", "v", "osmid", "name", "highway", "length"]].copy()
    roads_df["start_lat"] = roads_df["u"].map(nodes["y"])
    roads_df["start_lon"] = roads_df["u"].map(nodes["x"])
    roads_df["end_lat"] = roads_df["v"].map(nodes["y"])
    roads_df["end_lon"] = roads_df["v"].map(nodes["x"])
    roads_df["mid_lat"] = (roads_df["start_lat"] + roads_df["end_lat"]) / 2
    roads_df["mid_lon"] = (roads_df["start_lon"] + roads_df["end_lon"]) / 2
    roads_df["highway"] = roads_df["highway"].apply(_normalize_scalar)
    roads_df["name"] = roads_df["name"].apply(_normalize_scalar)
    roads_df = roads_df.rename(columns={"osmid": "osm_id", "length": "length_m"})
    roads_df = roads_df[
        [
            "osm_id",
            "name",
            "highway",
            "start_lat",
            "start_lon",
            "end_lat",
            "end_lon",
            "mid_lat",
            "mid_lon",
            "length_m",
        ]
    ]
    roads_df.to_csv(output_path, index=False)
    print(f"Roads saved: {len(roads_df)} rows -> {output_path}")


def download_pois(place_name: str, output_path: Path) -> None:
    import pandas as pd
    import osmnx as ox

    print(f"Downloading OSM POIs for {place_name}...")
    gdf = ox.features_from_place(place_name, tags={key: values for key, values in OSM_POI_TAGS.items()})

    if gdf.empty:
        pd.DataFrame(
            columns=["osm_id", "name", "tag_key", "tag_value", "poi_category", "lat", "lon"]
        ).to_csv(output_path, index=False)
        print(f"No POIs returned for {place_name}; wrote empty file to {output_path}")
        return

    projected = gdf.to_crs(gdf.estimate_utm_crs())
    centroids = projected.geometry.centroid.to_crs(gdf.crs)
    records: list[dict[str, object]] = []

    for index, row in gdf.iterrows():
        tag_key = None
        tag_value = None
        for candidate_key, allowed_values in OSM_POI_TAGS.items():
            value = row.get(candidate_key)
            if value in allowed_values:
                tag_key = candidate_key
                tag_value = value
                break

        if tag_key is None or tag_value is None:
            continue

        geometry = centroids.loc[index]
        records.append(
            {
                "osm_id": str(index[1]) if isinstance(index, tuple) else str(index),
                "name": row.get("name"),
                "tag_key": tag_key,
                "tag_value": tag_value,
                "poi_category": tag_value,
                "lat": geometry.y,
                "lon": geometry.x,
            }
        )

    pd.DataFrame.from_records(records).to_csv(output_path, index=False)
    print(f"POIs saved: {len(records)} rows -> {output_path}")


def download_osm(city: str, run_id: str | None = None, force: bool = False) -> None:
    city_config = get_batch_city_config(city)
    output_dir = raw_osm_dir(city)
    output_dir.mkdir(parents=True, exist_ok=True)
    roads_path = output_dir / "roads.csv"
    pois_path = output_dir / "pois.csv"
    tracker = StageTracker(
        stage="download_osm",
        city=city,
        run_id=run_id or generate_run_id(city),
        force=force,
    )
    command = f"python jobs/ingestion/download_osm.py --city {city}"
    expected_outputs = [roads_path, pois_path]

    if tracker.should_skip(expected_outputs):
        print(f"Skipping download_osm for {city}; checkpoint exists for run_id={tracker.run_id}")
        tracker.mark_skipped(command=command, output_paths=expected_outputs)
        return

    tracker.mark_running(
        command=command,
        output_paths=expected_outputs,
        metrics={"osm_place_name": city_config.osm_place_name},
    )

    try:
        download_roads(city_config.osm_place_name, roads_path)
        download_pois(city_config.osm_place_name, pois_path)
        tracker.mark_success(
            command=command,
            output_paths=expected_outputs,
            metrics={
                "osm_place_name": city_config.osm_place_name,
                "roads_csv_bytes": roads_path.stat().st_size if roads_path.exists() else 0,
                "pois_csv_bytes": pois_path.stat().st_size if pois_path.exists() else 0,
            },
        )
        print(f"OSM download complete for {city_config.display_name}.")
    except Exception as error:
        tracker.mark_failed(
            command=command,
            error=error,
            output_paths=expected_outputs,
            metrics={"osm_place_name": city_config.osm_place_name},
        )
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download OSM roads and POIs for a city.")
    parser.add_argument("--city", default="chicago", choices=["chicago", "boston"])
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    download_osm(args.city, run_id=args.run_id, force=args.force)
