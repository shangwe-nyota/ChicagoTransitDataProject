import math
import pandas as pd
import streamlit as st
import pydeck as pdk

# Initial Streamlit Dashboard for EDA
OVERALL_DATA_PATH = "data/processed/analytics/stop_activity_enriched"
ROUTE_DATA_PATH = "data/processed/analytics/stop_activity_by_route"

st.set_page_config(page_title="Chicago Transit Dashboard", layout="wide")

st.title("Chicago Transit Data Project")
st.subheader("Phase 1: CTA Static Analytics")

# -----------------------------
# Load data
# -----------------------------
overall_df = pd.read_parquet(OVERALL_DATA_PATH)
overall_df = overall_df.sort_values("trip_count", ascending=False).copy()

route_df = pd.read_parquet(ROUTE_DATA_PATH)
route_df = route_df.sort_values("trip_count", ascending=False).copy()

# -----------------------------
# Sidebar / controls
# -----------------------------
st.sidebar.header("Dashboard Controls")

view_mode = st.sidebar.radio(
    "Choose view",
    ["Overall busiest stops", "Explore a bus route"]
)


# -----------------------------
# Helper styling functions
# -----------------------------
def color_for_trip_count(x: int):
    if x < 500:
        return [100, 149, 237, 160]  # blue
    elif x < 1000:
        return [255, 165, 0, 180]  # orange
    elif x < 1500:
        return [220, 20, 60, 190]  # red
    else:
        return [128, 0, 128, 210]  # purple


def radius_for_trip_count(x: int):
    return max(40, min(300, 8 * math.sqrt(x)))


# =========================================================
# VIEW 1: OVERALL BUSIEST STOPS
# =========================================================
if view_mode == "Overall busiest stops":
    st.markdown("## Overall busiest CTA stops")

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
    filtered_df = filtered_df.head(max_rows).copy()

    filtered_df["color"] = filtered_df["trip_count"].apply(color_for_trip_count)
    filtered_df["radius"] = filtered_df["trip_count"].apply(radius_for_trip_count)

    col1, col2, col3 = st.columns(3)
    col1.metric("Stops shown", len(filtered_df))
    col2.metric("Highest stop activity", int(filtered_df["trip_count"].max()) if len(filtered_df) else 0)
    col3.metric("Average stop activity", round(filtered_df["trip_count"].mean(), 1) if len(filtered_df) else 0)

    st.markdown("### Legend")
    st.markdown("""
    - 🔵 Blue: lower activity  
    - 🟠 Orange: medium activity  
    - 🔴 Red: high activity  
    - 🟣 Purple: very high activity  
    """)

    st.markdown("### Top 20 busiest stops")
    st.dataframe(
        filtered_df[["stop_id", "stop_name", "trip_count", "stop_lat", "stop_lon"]].head(20),
        use_container_width=True
    )

    st.markdown("### Interactive map")
    st.caption("Hover over a stop to see details. Size and color reflect stop activity.")

    if len(filtered_df) > 0:
        view_state = pdk.ViewState(
            latitude=float(filtered_df["stop_lat"].mean()),
            longitude=float(filtered_df["stop_lon"].mean()),
            zoom=10,
            pitch=0,
        )

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=filtered_df,
            get_position="[stop_lon, stop_lat]",
            get_fill_color="color",
            get_radius="radius",
            pickable=True,
            stroked=True,
            filled=True,
            radius_scale=1,
            radius_min_pixels=3,
            radius_max_pixels=20,
            line_width_min_pixels=1,
            get_line_color=[20, 20, 20, 160],
        )

        tooltip = {
            "html": """
            <b>{stop_name}</b><br/>
            Stop ID: {stop_id}<br/>
            Scheduled Stop Events: {trip_count}<br/>
            Location Type: {location_type}<br/>
            Parent Station: {parent_station}
            """,
            "style": {"backgroundColor": "steelblue", "color": "white"}
        }

        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip=tooltip,
            map_style="light"
        )

        st.pydeck_chart(deck, use_container_width=True)

# =========================================================
# VIEW 2: ROUTE EXPLORATION
# =========================================================
else:
    st.markdown("## Explore stops for a selected route")

    route_options = (
        route_df[["route_id", "route_short_name", "route_long_name"]]
        .drop_duplicates()
        .sort_values(["route_short_name", "route_long_name"])
    )

    route_options["route_label"] = (
            route_options["route_short_name"].fillna(route_options["route_id"]) +
            " - " +
            route_options["route_long_name"].fillna("")
    )

    selected_label = st.selectbox(
        "Choose a route",
        route_options["route_label"].tolist()
    )

    selected_row = route_options[route_options["route_label"] == selected_label].iloc[0]
    selected_route_id = selected_row["route_id"]

    selected_df = route_df[route_df["route_id"] == selected_route_id].copy()

    max_route_value = int(selected_df["trip_count"].max()) if len(selected_df) else 0

    min_route_trips = st.slider(
        "Show stops with at least this many scheduled stop events on the selected route",
        min_value=0,
        max_value=max_route_value,
        value=0,
        key="route_slider"
    )

    selected_df = selected_df[selected_df["trip_count"] >= min_route_trips].copy()
    selected_df = selected_df.sort_values("trip_count", ascending=False)

    selected_df["color"] = selected_df["trip_count"].apply(color_for_trip_count)
    selected_df["radius"] = selected_df["trip_count"].apply(radius_for_trip_count)

    col1, col2, col3 = st.columns(3)
    col1.metric("Route ID", selected_route_id)
    col2.metric("Stops shown", len(selected_df))
    col3.metric("Highest stop activity on this route", int(selected_df["trip_count"].max()) if len(selected_df) else 0)

    st.markdown("### Top stops for this route")
    st.dataframe(
        selected_df[["route_short_name", "route_long_name", "stop_name", "trip_count", "stop_lat", "stop_lon"]].head(
            25),
        use_container_width=True
    )

    st.markdown("### Route map")
    st.caption("Hover over a stop to see route-specific stop activity.")

    if len(selected_df) > 0:
        view_state = pdk.ViewState(
            latitude=float(selected_df["stop_lat"].mean()),
            longitude=float(selected_df["stop_lon"].mean()),
            zoom=10,
            pitch=0,
        )

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=selected_df,
            get_position="[stop_lon, stop_lat]",
            get_fill_color="color",
            get_radius="radius",
            pickable=True,
            stroked=True,
            filled=True,
            radius_scale=1,
            radius_min_pixels=3,
            radius_max_pixels=20,
            line_width_min_pixels=1,
            get_line_color=[20, 20, 20, 160],
        )

        tooltip = {
            "html": """
            <b>{stop_name}</b><br/>
            Route: {route_short_name} - {route_long_name}<br/>
            Stop ID: {stop_id}<br/>
            Scheduled Stop Events on This Route: {trip_count}
            """,
            "style": {"backgroundColor": "darkgreen", "color": "white"}
        }

        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip=tooltip,
            map_style="light"
        )

        st.pydeck_chart(deck, use_container_width=True)

with st.expander("See raw overall data"):
    st.dataframe(overall_df.head(50), use_container_width=True)
