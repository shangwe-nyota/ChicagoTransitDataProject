import os
import osmnx as ox
import pandas as pd

OUTPUT_DIR = "data/raw/osm"
ROADS_PATH = os.path.join(OUTPUT_DIR, "roads.csv")
POIS_PATH = os.path.join(OUTPUT_DIR, "pois.csv")

PLACE = "Chicago, Illinois, USA"


def download_roads():
    print("Downloading Chicago road network...")
    G = ox.graph_from_place(PLACE, network_type="drive")
    nodes, edges = ox.graph_to_gdfs(G)

    roads_df = edges.reset_index()[["u", "v", "osmid", "name", "highway", "length"]].copy()

    # Get node coordinates for start/end points
    roads_df["start_lat"] = roads_df["u"].map(nodes["y"])
    roads_df["start_lon"] = roads_df["u"].map(nodes["x"])
    roads_df["end_lat"] = roads_df["v"].map(nodes["y"])
    roads_df["end_lon"] = roads_df["v"].map(nodes["x"])

    # highway can be a list for edges with multiple types — take first
    roads_df["highway"] = roads_df["highway"].apply(
        lambda x: x[0] if isinstance(x, list) else x
    )
    # name can also be a list
    roads_df["name"] = roads_df["name"].apply(
        lambda x: x[0] if isinstance(x, list) else x
    )

    roads_df = roads_df.rename(columns={"osmid": "osm_id", "length": "length_m"})
    roads_df = roads_df[["osm_id", "name", "highway", "start_lat", "start_lon", "end_lat", "end_lon", "length_m"]]

    roads_df.to_csv(ROADS_PATH, index=False)
    print(f"Roads saved: {len(roads_df)} edges -> {ROADS_PATH}")


def download_pois():
    print("Downloading Chicago POIs...")
    gdf = ox.features_from_place(PLACE, tags={"amenity": True})

    # Keep only point geometries (not polygons/multipolygons)
    points = gdf[gdf.geometry.geom_type == "Point"].copy()

    pois_df = pd.DataFrame({
        "osm_id": points.index.get_level_values("id").astype(str),
        "name": points["name"].values if "name" in points.columns else None,
        "amenity": points["amenity"].values,
        "lat": points.geometry.y.values,
        "lon": points.geometry.x.values,
    })

    pois_df.to_csv(POIS_PATH, index=False)
    print(f"POIs saved: {len(pois_df)} points -> {POIS_PATH}")


def download_osm():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    download_roads()
    download_pois()
    print("OSM download complete.")


if __name__ == "__main__":
    download_osm()
