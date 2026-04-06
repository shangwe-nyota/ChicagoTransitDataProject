import math
import os
import pandas as pd
import streamlit as st
import pydeck as pdk
import altair as alt

# -----------------------------
# Data paths
# -----------------------------
OVERALL_DATA_PATH = "data/processed/analytics/stop_activity_enriched"
ROUTE_DATA_PATH = "data/processed/analytics/stop_activity_by_route"
ROUTE_ACTIVITY_PATH = "data/processed/analytics/route_activity"
ROUTE_SHAPES_PATH = "data/processed/analytics/route_shapes"
POI_ACCESS_PATH = "data/processed/analytics/stop_poi_access"
ROAD_COVERAGE_PATH = "data/processed/analytics/transit_road_coverage"

st.set_page_config(page_title="Chicago Transit Dashboard", layout="wide", page_icon="🚌")

st.title("Chicago Transit Data Project")
st.subheader("CTA Static + OSM Analytics")

# -----------------------------
# Load data
# -----------------------------
overall_df = pd.read_parquet(OVERALL_DATA_PATH)
overall_df = overall_df.sort_values("trip_count", ascending=False).copy()

route_df = pd.read_parquet(ROUTE_DATA_PATH)
route_df = route_df.sort_values("trip_count", ascending=False).copy()

route_activity_df = None
if os.path.exists(ROUTE_ACTIVITY_PATH):
    route_activity_df = pd.read_parquet(ROUTE_ACTIVITY_PATH)
    route_activity_df = route_activity_df.sort_values("stop_event_count", ascending=False).copy()

# -----------------------------
# Global KPI Summary
# -----------------------------
st.markdown("---")
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
kpi1.metric("Total Stops", f"{len(overall_df):,}")
if route_activity_df is not None:
    kpi2.metric("Total Routes", f"{len(route_activity_df):,}")
    kpi5.metric("Busiest Route", route_activity_df.iloc[0]["route_short_name"] if len(route_activity_df) else "N/A")
else:
    kpi2.metric("Total Routes", route_df["route_id"].nunique())
    kpi5.metric("Busiest Route", "N/A")
kpi3.metric("Total Stop Events", f"{int(overall_df['trip_count'].sum()):,}")
kpi4.metric("Busiest Stop", overall_df.iloc[0]["stop_name"] if len(overall_df) else "N/A")
st.markdown("---")

# -----------------------------
# Sidebar / controls
# -----------------------------
st.sidebar.header("Dashboard Controls")

view_mode = st.sidebar.radio(
    "Choose view",
    ["Overall busiest stops", "Explore a bus route", "Route Analytics", "Transit + POI Access", "Live Realtime (Streaming)"]
)

# Stop name search (global)
stop_search = st.sidebar.text_input("Search stop by name", "")

# -----------------------------
# Helper styling functions
# -----------------------------
def color_for_trip_count(x: int):
    if x < 500:
        return [100, 149, 237, 160]   # blue
    elif x < 1000:
        return [255, 165, 0, 180]     # orange
    elif x < 1500:
        return [220, 20, 60, 190]     # red
    else:
        return [128, 0, 128, 210]     # purple

def radius_for_trip_count(x: int):
    return max(40, min(300, 8 * math.sqrt(x)))


# =========================================================
# VIEW 1: OVERALL BUSIEST STOPS
# =========================================================
if view_mode == "Overall busiest stops":
    st.markdown("## Overall Busiest CTA Stops")
    st.caption("Which stops have the most scheduled stop events across all routes and trips?")

    min_trips = st.slider(
        "Show stops with at least this many scheduled stop events",
        min_value=0,
        max_value=int(overall_df["trip_count"].max()),
        value=1000,
        key="overall_slider"
    )

    max_rows = st.slider(
        "Maximum number of stops to display on map",
        min_value=50,
        max_value=min(2000, len(overall_df)),
        value=min(300, len(overall_df)),
        step=50,
        key="overall_max_rows"
    )

    filtered_df = overall_df[overall_df["trip_count"] >= min_trips].copy()
    if stop_search:
        filtered_df = filtered_df[filtered_df["stop_name"].str.contains(stop_search, case=False, na=False)]
    filtered_df = filtered_df.head(max_rows).copy()

    filtered_df["color"] = filtered_df["trip_count"].apply(color_for_trip_count)
    filtered_df["radius"] = filtered_df["trip_count"].apply(radius_for_trip_count)

    col1, col2, col3 = st.columns(3)
    col1.metric("Stops shown", len(filtered_df))
    col2.metric("Highest stop activity", int(filtered_df["trip_count"].max()) if len(filtered_df) else 0)
    col3.metric("Average stop activity", round(filtered_df["trip_count"].mean(), 1) if len(filtered_df) else 0)

    # Stop activity distribution chart
    st.markdown("### Stop Activity Distribution")
    st.caption("How stop events are distributed across the network.")
    hist_data = overall_df[["trip_count"]].copy()
    median_val = hist_data["trip_count"].median()
    p90_val = hist_data["trip_count"].quantile(0.9)

    hist_chart = alt.Chart(hist_data).mark_bar(color="#4682B4", opacity=0.7).encode(
        alt.X("trip_count:Q", bin=alt.Bin(maxbins=40), title="Scheduled Stop Events"),
        alt.Y("count()", title="Number of Stops"),
    ).properties(height=250)

    median_rule = alt.Chart(pd.DataFrame({"x": [median_val]})).mark_rule(color="orange", strokeDash=[4, 4]).encode(x="x:Q")
    p90_rule = alt.Chart(pd.DataFrame({"x": [p90_val]})).mark_rule(color="red", strokeDash=[4, 4]).encode(x="x:Q")

    st.altair_chart(hist_chart + median_rule + p90_rule, use_container_width=True)
    st.caption(f"Orange line = median ({int(median_val)}), Red line = 90th percentile ({int(p90_val)})")

    st.markdown("### Top 20 Busiest Stops")
    display_cols = ["stop_name", "trip_count", "stop_lat", "stop_lon"]
    st.dataframe(
        filtered_df[display_cols].head(20).rename(columns={
            "stop_name": "Stop Name", "trip_count": "Stop Events",
            "stop_lat": "Latitude", "stop_lon": "Longitude",
        }),
        use_container_width=True
    )

    st.markdown("### Interactive Map")
    st.caption("Hover over a stop to see details. Size and color reflect activity level: blue (low) -> orange -> red -> purple (high).")

    if len(filtered_df) > 0:
        view_state = pdk.ViewState(
            latitude=float(filtered_df["stop_lat"].mean()),
            longitude=float(filtered_df["stop_lon"].mean()),
            zoom=10, pitch=0,
        )

        layer = pdk.Layer(
            "ScatterplotLayer", data=filtered_df,
            get_position="[stop_lon, stop_lat]",
            get_fill_color="color", get_radius="radius",
            pickable=True, stroked=True, filled=True,
            radius_scale=1, radius_min_pixels=3, radius_max_pixels=20,
            line_width_min_pixels=1, get_line_color=[20, 20, 20, 160],
        )

        tooltip = {
            "html": "<b>{stop_name}</b><br/>Stop ID: {stop_id}<br/>Scheduled Stop Events: {trip_count}<br/>Location Type: {location_type}<br/>Parent Station: {parent_station}",
            "style": {"backgroundColor": "steelblue", "color": "white"}
        }

        st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip, map_style="light"), use_container_width=True)


# =========================================================
# VIEW 2: ROUTE EXPLORATION
# =========================================================
elif view_mode == "Explore a bus route":
    st.markdown("## Explore Stops for a Selected Route")
    st.caption("Select a route to see its stops, activity levels, and route path on the map.")

    route_options = (
        route_df[["route_id", "route_short_name", "route_long_name"]]
        .drop_duplicates()
        .sort_values(["route_short_name", "route_long_name"])
    )
    route_options["route_label"] = (
        route_options["route_short_name"].fillna(route_options["route_id"]) +
        " - " + route_options["route_long_name"].fillna("")
    )

    selected_label = st.selectbox("Choose a route", route_options["route_label"].tolist())
    selected_row = route_options[route_options["route_label"] == selected_label].iloc[0]
    selected_route_id = selected_row["route_id"]

    selected_df = route_df[route_df["route_id"] == selected_route_id].copy()
    if stop_search:
        selected_df = selected_df[selected_df["stop_name"].str.contains(stop_search, case=False, na=False)]

    max_route_value = int(selected_df["trip_count"].max()) if len(selected_df) else 0

    min_route_trips = st.slider(
        "Show stops with at least this many scheduled stop events on the selected route",
        min_value=0, max_value=max_route_value, value=0, key="route_slider"
    )

    selected_df = selected_df[selected_df["trip_count"] >= min_route_trips].copy()
    selected_df = selected_df.sort_values("trip_count", ascending=False)
    selected_df["color"] = selected_df["trip_count"].apply(color_for_trip_count)
    selected_df["radius"] = selected_df["trip_count"].apply(radius_for_trip_count)

    col1, col2, col3 = st.columns(3)
    col1.metric("Route ID", selected_route_id)
    col2.metric("Stops shown", len(selected_df))
    col3.metric("Highest stop activity", int(selected_df["trip_count"].max()) if len(selected_df) else 0)

    st.markdown("### Top Stops for This Route")
    st.dataframe(
        selected_df[["stop_name", "trip_count", "stop_lat", "stop_lon"]].head(25).rename(columns={
            "stop_name": "Stop Name", "trip_count": "Stop Events",
            "stop_lat": "Latitude", "stop_lon": "Longitude",
        }),
        use_container_width=True
    )

    st.markdown("### Route Map")
    st.caption("Hover over a stop to see route-specific stop activity.")

    if len(selected_df) > 0:
        layers = []

        # Route shape layer (if available)
        if os.path.exists(ROUTE_SHAPES_PATH):
            shapes_df = pd.read_parquet(ROUTE_SHAPES_PATH)
            route_shapes = shapes_df[shapes_df["route_id"] == selected_route_id].copy()
            if len(route_shapes) > 0:
                route_shapes = route_shapes.sort_values(["shape_id", "shape_pt_sequence"])
                # Build path data: group by shape_id into coordinate lists
                paths = []
                for shape_id, group in route_shapes.groupby("shape_id"):
                    coords = group[["shape_pt_lon", "shape_pt_lat"]].values.tolist()
                    paths.append({"path": coords, "name": f"Shape {shape_id}"})

                path_df = pd.DataFrame(paths)
                path_layer = pdk.Layer(
                    "PathLayer", data=path_df,
                    get_path="path", get_color=[0, 100, 200, 180],
                    width_scale=2, width_min_pixels=2, width_max_pixels=5,
                    pickable=False,
                )
                layers.append(path_layer)

        # Stop scatter layer
        stop_layer = pdk.Layer(
            "ScatterplotLayer", data=selected_df,
            get_position="[stop_lon, stop_lat]",
            get_fill_color="color", get_radius="radius",
            pickable=True, stroked=True, filled=True,
            radius_scale=1, radius_min_pixels=3, radius_max_pixels=20,
            line_width_min_pixels=1, get_line_color=[20, 20, 20, 160],
        )
        layers.append(stop_layer)

        view_state = pdk.ViewState(
            latitude=float(selected_df["stop_lat"].mean()),
            longitude=float(selected_df["stop_lon"].mean()),
            zoom=11, pitch=0,
        )

        tooltip = {
            "html": "<b>{stop_name}</b><br/>Route: {route_short_name} - {route_long_name}<br/>Stop ID: {stop_id}<br/>Stop Events: {trip_count}",
            "style": {"backgroundColor": "darkgreen", "color": "white"}
        }

        st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view_state, tooltip=tooltip, map_style="light"), use_container_width=True)


# =========================================================
# VIEW 3: ROUTE ANALYTICS
# =========================================================
elif view_mode == "Route Analytics":
    st.markdown("## Route-Level Analytics")
    st.caption("Compare routes by total stop events, distinct trips, and distinct stops served.")

    if route_activity_df is not None and len(route_activity_df) > 0:
        # Top 15 busiest routes bar chart
        st.markdown("### Top 15 Busiest Routes")
        top_routes = route_activity_df.head(15).copy()
        top_routes["route_label"] = top_routes["route_short_name"].fillna("") + " - " + top_routes["route_long_name"].fillna("")

        bar_chart = alt.Chart(top_routes).mark_bar(color="#4682B4").encode(
            x=alt.X("stop_event_count:Q", title="Total Stop Events"),
            y=alt.Y("route_label:N", sort="-x", title="Route"),
            tooltip=["route_label", "stop_event_count", "distinct_trip_count", "distinct_stop_count"],
        ).properties(height=400)
        st.altair_chart(bar_chart, use_container_width=True)

        # Route comparison: trips vs stops scatter
        st.markdown("### Trips vs Stops per Route")
        st.caption("Each dot is a route. Larger routes serve more trips across more stops.")
        scatter = alt.Chart(route_activity_df).mark_circle(opacity=0.7).encode(
            x=alt.X("distinct_trip_count:Q", title="Distinct Trips"),
            y=alt.Y("distinct_stop_count:Q", title="Distinct Stops"),
            size=alt.Size("stop_event_count:Q", title="Stop Events", scale=alt.Scale(range=[50, 500])),
            color=alt.Color("route_type:N", title="Route Type"),
            tooltip=["route_short_name", "route_long_name", "stop_event_count", "distinct_trip_count", "distinct_stop_count"],
        ).properties(height=400)
        st.altair_chart(scatter, use_container_width=True)

        # Full route table
        st.markdown("### All Routes")
        st.dataframe(
            route_activity_df[["route_short_name", "route_long_name", "route_type", "stop_event_count", "distinct_trip_count", "distinct_stop_count"]].rename(columns={
                "route_short_name": "Route", "route_long_name": "Name", "route_type": "Type",
                "stop_event_count": "Stop Events", "distinct_trip_count": "Trips", "distinct_stop_count": "Stops",
            }),
            use_container_width=True,
        )
    else:
        st.warning("Route activity data not found. Run the analytics pipeline first.")


# =========================================================
# VIEW 4: TRANSIT + POI ACCESS (OSM)
# =========================================================
elif view_mode == "Transit + POI Access":
    st.markdown("## Transit + POI Access (OpenStreetMap)")
    st.caption("How many amenities (schools, hospitals, restaurants, etc.) are within 400m of each CTA stop?")

    if os.path.exists(POI_ACCESS_PATH):
        poi_df = pd.read_parquet(POI_ACCESS_PATH)
        poi_df = poi_df.sort_values("poi_count_within_400m", ascending=False).copy()

        if stop_search:
            poi_df = poi_df[poi_df["stop_name"].str.contains(stop_search, case=False, na=False)]

        min_pois = st.slider(
            "Show stops with at least this many nearby amenities",
            min_value=0,
            max_value=int(poi_df["poi_count_within_400m"].max()),
            value=5, key="poi_slider"
        )

        filtered_poi = poi_df[poi_df["poi_count_within_400m"] >= min_pois].copy()

        col1, col2, col3 = st.columns(3)
        col1.metric("Stops shown", len(filtered_poi))
        col2.metric("Max nearby POIs", int(filtered_poi["poi_count_within_400m"].max()) if len(filtered_poi) else 0)
        col3.metric("Avg nearby POIs", round(filtered_poi["poi_count_within_400m"].mean(), 1) if len(filtered_poi) else 0)

        def poi_color(x):
            if x < 5:
                return [100, 149, 237, 160]
            elif x < 15:
                return [255, 165, 0, 180]
            elif x < 30:
                return [220, 20, 60, 190]
            else:
                return [128, 0, 128, 210]

        filtered_poi["color"] = filtered_poi["poi_count_within_400m"].apply(poi_color)
        filtered_poi["radius"] = filtered_poi["poi_count_within_400m"].apply(
            lambda x: max(40, min(300, 10 * math.sqrt(x)))
        )

        st.markdown("### Top Stops by Nearby Amenities")
        st.dataframe(
            filtered_poi[["stop_name", "poi_count_within_400m", "nearest_school_m", "nearest_hospital_m", "amenity_types"]].head(20).rename(columns={
                "stop_name": "Stop Name", "poi_count_within_400m": "POIs (400m)",
                "nearest_school_m": "Nearest School (m)", "nearest_hospital_m": "Nearest Hospital (m)",
                "amenity_types": "Amenity Types",
            }),
            use_container_width=True,
        )

        st.markdown("### POI Access Map")
        st.caption("Blue = few, Orange = moderate, Red = many, Purple = very high amenity access.")

        if len(filtered_poi) > 0:
            view_state = pdk.ViewState(
                latitude=float(filtered_poi["stop_lat"].mean()),
                longitude=float(filtered_poi["stop_lon"].mean()),
                zoom=10, pitch=0,
            )
            layer = pdk.Layer(
                "ScatterplotLayer", data=filtered_poi.head(1000),
                get_position="[stop_lon, stop_lat]",
                get_fill_color="color", get_radius="radius",
                pickable=True, stroked=True, filled=True,
                radius_scale=1, radius_min_pixels=3, radius_max_pixels=20,
                line_width_min_pixels=1, get_line_color=[20, 20, 20, 160],
            )
            tooltip = {
                "html": "<b>{stop_name}</b><br/>Stop ID: {stop_id}<br/>Nearby amenities (400m): {poi_count_within_400m}<br/>Nearest school: {nearest_school_m}m<br/>Nearest hospital: {nearest_hospital_m}m<br/>Types: {amenity_types}",
                "style": {"backgroundColor": "#2c3e50", "color": "white"},
            }
            st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip, map_style="light"), use_container_width=True)

        # Road coverage summary
        if os.path.exists(ROAD_COVERAGE_PATH):
            st.markdown("### Transit Road Coverage by Road Type")
            st.caption("What percentage of Chicago's roads have a CTA stop within 200m?")
            road_df = pd.read_parquet(ROAD_COVERAGE_PATH)
            road_df = road_df.sort_values("coverage_pct", ascending=False)

            coverage_chart = alt.Chart(road_df).mark_bar().encode(
                x=alt.X("coverage_pct:Q", title="Transit Coverage (%)", scale=alt.Scale(domain=[0, 100])),
                y=alt.Y("highway:N", sort="-x", title="Road Type"),
                color=alt.Color("coverage_pct:Q", scale=alt.Scale(scheme="blues"), legend=None),
                tooltip=["highway", "coverage_pct", "total_road_segments", "road_segments_near_transit"],
            ).properties(height=250)
            st.altair_chart(coverage_chart, use_container_width=True)

            st.dataframe(
                road_df.rename(columns={
                    "highway": "Road Type", "total_road_segments": "Total Segments",
                    "road_segments_near_transit": "Near Transit", "coverage_pct": "Coverage %",
                    "total_length_km": "Total km", "covered_length_km": "Covered km",
                }),
                use_container_width=True,
            )
    else:
        st.warning("POI access data not found. Run the OSM pipeline first.")


# =========================================================
# VIEW 5: LIVE REALTIME (STREAMING)
# =========================================================
else:
    st.markdown("## Live Realtime Dashboard (Streaming)")
    st.caption("Real-time CTA vehicle positions and delay metrics from the Kafka/Flink streaming pipeline.")

    st.markdown("""
    ### Setup Required

    This view reads from Snowflake realtime tables populated by the streaming pipeline.
    To enable it:

    1. **Get a CTA API key** — Register at [transitchicago.com/developers](https://www.transitchicago.com/developers/traintrackerapply/)
    2. **Start infrastructure** — `docker compose up -d` (Kafka + Flink)
    3. **Run the producer** — `python3 jobs/streaming/cta_realtime_producer.py`
    4. **Submit Flink jobs** — See `jobs/streaming/README.md`
    5. **Run the sink** — `python3 jobs/streaming/sink_to_snowflake.py`
    """)

    # Try to read from local parquet fallback or Snowflake
    realtime_delay_path = "data/realtime/delay_metrics"
    realtime_vehicles_path = "data/realtime/vehicle_positions"

    has_delay_data = os.path.exists(realtime_delay_path)
    has_vehicle_data = os.path.exists(realtime_vehicles_path)

    if has_delay_data:
        st.markdown("### Delay Metrics by Route")
        delay_df = pd.read_parquet(realtime_delay_path)
        delay_df = delay_df.sort_values("delay_ratio", ascending=False)

        col1, col2, col3 = st.columns(3)
        col1.metric("Routes tracked", delay_df["route_id"].nunique())
        col2.metric("Avg delay ratio", f"{delay_df['delay_ratio'].mean():.1%}")
        col3.metric("Most delayed route", delay_df.iloc[0]["route_id"] if len(delay_df) else "N/A")

        delay_chart = alt.Chart(delay_df).mark_bar().encode(
            x=alt.X("delay_ratio:Q", title="Delay Ratio", scale=alt.Scale(domain=[0, 1])),
            y=alt.Y("route_id:N", sort="-x", title="Route"),
            color=alt.Color("delay_ratio:Q", scale=alt.Scale(scheme="reds"), legend=None),
        ).properties(height=300)
        st.altair_chart(delay_chart, use_container_width=True)

    if has_vehicle_data:
        st.markdown("### Live Vehicle Positions")
        vehicles_df = pd.read_parquet(realtime_vehicles_path)
        if "latitude" in vehicles_df.columns and "longitude" in vehicles_df.columns:
            vehicles_df["latitude"] = pd.to_numeric(vehicles_df["latitude"], errors="coerce")
            vehicles_df["longitude"] = pd.to_numeric(vehicles_df["longitude"], errors="coerce")
            valid = vehicles_df.dropna(subset=["latitude", "longitude"])

            if len(valid) > 0:
                layer = pdk.Layer(
                    "ScatterplotLayer", data=valid,
                    get_position="[longitude, latitude]",
                    get_fill_color="[220, 20, 60, 200]",
                    get_radius=100, pickable=True,
                )
                view_state = pdk.ViewState(
                    latitude=valid["latitude"].mean(), longitude=valid["longitude"].mean(),
                    zoom=10,
                )
                st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, map_style="light"), use_container_width=True)

    if not has_delay_data and not has_vehicle_data:
        st.info("No realtime data available yet. Start the streaming pipeline to populate this view.")


# -----------------------------
# Footer
# -----------------------------
st.markdown("---")
with st.expander("See raw overall data"):
    st.dataframe(overall_df.head(50), use_container_width=True)

st.caption("Data source: CTA GTFS Static Feed + OpenStreetMap | Built with PySpark, Snowflake, and Streamlit")
