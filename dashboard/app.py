import pandas as pd
import streamlit as st

DATA_PATH = "data/processed/analytics/stop_activity_enriched"

st.set_page_config(page_title="Chicago Transit Dashboard", layout="wide")

st.title("Chicago Transit Data Project")
st.subheader("Phase 1: Busiest CTA Stops")

# Load parquet output from Spark
df = pd.read_parquet(DATA_PATH)

# Sort descending just to be safe
df = df.sort_values("trip_count", ascending=False)

# Show top 20 busiest chicago stops
st.markdown("## Top 20 busiest stops")
st.dataframe(
    df[["stop_id", "stop_name", "trip_count", "stop_lat", "stop_lon"]].head(20),
    use_container_width=True
)

# Map section
st.markdown("## Map of busiest stops")

map_df = df[["stop_lat", "stop_lon"]].rename(
    columns={"stop_lat": "lat", "stop_lon": "lon"}
)

st.map(map_df.head(200))

# Optional raw preview (Just so people can see the data pulled from)
with st.expander("See raw enriched data"):
    st.dataframe(df.head(50), use_container_width=True)
