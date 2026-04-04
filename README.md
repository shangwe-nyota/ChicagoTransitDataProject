# ChicagoTransitDataProject



## Pipeline Overview

1. Ingest GTFS static data from CTA
2. Clean and transform data using PySpark
3. Aggregate stop activity (trip counts per stop)
4. Enrich with stop metadata (name, location)
5. Visualize results using Streamlit dashboard