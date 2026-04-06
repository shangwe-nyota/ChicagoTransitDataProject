"""
Issue #10: Map UI Prototype — Plotly Mapbox

Alternative to Streamlit + PyDeck. Replicates two existing views:
1. Overall busiest stops (scatter map)
2. Route exploration with route shape paths

Run: streamlit run dashboard/plotly_prototype.py
"""
import os
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

OVERALL_DATA_PATH = "data/processed/analytics/stop_activity_enriched"
ROUTE_DATA_PATH = "data/processed/analytics/stop_activity_by_route"
ROUTE_SHAPES_PATH = "data/processed/analytics/route_shapes"

st.set_page_config(page_title="CTA Map — Plotly Prototype", layout="wide")
st.title("Map UI Prototype: Plotly Mapbox")
st.caption("Comparison prototype for Issue #10 — evaluating alternatives to PyDeck.")

overall_df = pd.read_parquet(OVERALL_DATA_PATH)
overall_df = overall_df.sort_values("trip_count", ascending=False).copy()

view = st.radio("View", ["Busiest Stops (Scatter)", "Route + Shape Path"], horizontal=True)


if view == "Busiest Stops (Scatter)":
    st.markdown("### Busiest CTA Stops — Plotly scatter_mapbox")

    min_trips = st.slider("Min stop events", 0, int(overall_df["trip_count"].max()), 500)
    df = overall_df[overall_df["trip_count"] >= min_trips].head(500).copy()

    fig = px.scatter_mapbox(
        df,
        lat="stop_lat",
        lon="stop_lon",
        size="trip_count",
        color="trip_count",
        color_continuous_scale="Viridis",
        size_max=20,
        hover_name="stop_name",
        hover_data={"trip_count": True, "stop_id": True, "stop_lat": ":.4f", "stop_lon": ":.4f"},
        zoom=10,
        center={"lat": df["stop_lat"].mean(), "lon": df["stop_lon"].mean()},
        mapbox_style="carto-positron",
        height=650,
        title="CTA Stop Activity (Plotly)",
    )
    fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
    st.plotly_chart(fig, use_container_width=True)


else:
    st.markdown("### Route Exploration with Shape Paths — Plotly")

    route_df = pd.read_parquet(ROUTE_DATA_PATH)
    route_options = route_df[["route_id", "route_short_name", "route_long_name"]].drop_duplicates().sort_values("route_short_name")
    route_options["label"] = route_options["route_short_name"].fillna("") + " - " + route_options["route_long_name"].fillna("")

    selected = st.selectbox("Route", route_options["label"].tolist())
    route_id = route_options[route_options["label"] == selected].iloc[0]["route_id"]

    stops = route_df[route_df["route_id"] == route_id].copy()

    fig = go.Figure()

    # Route shape lines
    if os.path.exists(ROUTE_SHAPES_PATH):
        shapes_df = pd.read_parquet(ROUTE_SHAPES_PATH)
        route_shapes = shapes_df[shapes_df["route_id"] == route_id].sort_values(["shape_id", "shape_pt_sequence"])

        for shape_id, group in route_shapes.groupby("shape_id"):
            fig.add_trace(go.Scattermapbox(
                lat=group["shape_pt_lat"],
                lon=group["shape_pt_lon"],
                mode="lines",
                line=dict(width=3, color="royalblue"),
                name=f"Shape {shape_id}",
                showlegend=False,
            ))

    # Stop markers
    fig.add_trace(go.Scattermapbox(
        lat=stops["stop_lat"],
        lon=stops["stop_lon"],
        mode="markers",
        marker=dict(size=stops["trip_count"].clip(upper=2000) / 100 + 5, color="red", opacity=0.8),
        text=stops["stop_name"] + "<br>Events: " + stops["trip_count"].astype(str),
        hoverinfo="text",
        name="Stops",
    ))

    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            center={"lat": stops["stop_lat"].mean(), "lon": stops["stop_lon"].mean()},
            zoom=11,
        ),
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        height=650,
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        stops[["stop_name", "trip_count", "stop_lat", "stop_lon"]].sort_values("trip_count", ascending=False).head(20),
        use_container_width=True,
    )
