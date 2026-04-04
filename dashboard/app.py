import math
import pandas as pd
import streamlit as st
import pydeck as pdk

DATA_PATH = "data/processed/analytics/stop_activity_enriched"

st.set_page_config(page_title="Chicago Transit Dashboard", layout="wide")

st.title("Chicago Transit Data Project")
st.subheader("Phase 1: Busiest CTA Stops")

# -----------------------------
# Load data
# -----------------------------
df = pd.read_parquet(DATA_PATH)
df = df.sort_values("trip_count", ascending=False).copy()

# -----------------------------
# Filters
# -----------------------------
st.markdown("## Filter controls")

min_trips = st.slider(
    "Minimum trip count",
    min_value=0,
    max_value=int(df["trip_count"].max()),
    value=1000
)

max_rows = st.slider(
    "Maximum number of stops to display on map",
    min_value=50,
    max_value=min(2000, len(df)),
    value=min(300, len(df)),
    step=50
)

filtered_df = df[df["trip_count"] >= min_trips].copy()
filtered_df = filtered_df.head(max_rows).copy()

# -----------------------------
# Helper columns for styling
# -----------------------------
#Colors are based on trip_count (aka how busy the stop is)
#Blue=low activity, orange = medium activity , red = high acivity, purple is very high activity
def color_for_trip_count(x: int):
    # low -> medium -> high activity
    if x < 1000:
        return [100, 149, 237, 160]   # cornflower-ish blue
    elif x < 2000:
        return [255, 165, 0, 180]     # orange
    elif x < 3000:
        return [220, 20, 60, 190]     # crimson
    else:
        return [128, 0, 128, 210]     # purple
def color_for_trip_count(x):
    # gradient from blue → red
    intensity = min(255, int(x / 15))
    return [intensity, 50, 255 - intensity, 180]

def radius_for_trip_count(x: int):
    # smooth scaling so huge values don’t dominate too hard
    return max(40, min(300, 8 * math.sqrt(x)))

filtered_df["color"] = filtered_df["trip_count"].apply(color_for_trip_count)
filtered_df["radius"] = filtered_df["trip_count"].apply(radius_for_trip_count)

# -----------------------------
# KPIs
# -----------------------------
col1, col2, col3 = st.columns(3)
col1.metric("Stops shown", len(filtered_df))
col2.metric("Max trip count in view", int(filtered_df["trip_count"].max()) if len(filtered_df) else 0)
col3.metric("Average trip count in view", round(filtered_df["trip_count"].mean(), 1) if len(filtered_df) else 0)

# -----------------------------
# Top table
# -----------------------------
st.markdown("## Top 20 busiest stops")
st.dataframe(
    filtered_df[["stop_id", "stop_name", "trip_count", "stop_lat", "stop_lon"]].head(20),
    use_container_width=True
)

# -----------------------------
# Interactive map
# -----------------------------
st.markdown("## Interactive map of busiest stops")
st.caption("Hover over a stop to see details. Circle size and color reflect trip activity.")
st.markdown("### Legend")
st.markdown("""
- 🔵 Blue: < 1000 trips  
- 🟠 Orange: 1000–1999 trips  
- 🔴 Red: 2000–2999 trips  
- 🟣 Purple: 3000+ trips  
""")

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
        Trip Count: {trip_count}<br/>
        Location Type: {location_type}<br/>
        Parent Station: {parent_station}
        """,
        "style": {
            "backgroundColor": "steelblue",
            "color": "white"
        }
    }

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="light"
    )

    st.pydeck_chart(deck, use_container_width=True)
else:
    st.warning("No stops match the current filters.")

# -----------------------------
# Optional raw preview
# -----------------------------
with st.expander("See raw enriched data"):
    st.dataframe(filtered_df.head(50), use_container_width=True)