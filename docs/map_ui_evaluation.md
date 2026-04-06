# Map UI Evaluation for Transit Visualization

## Overview

This document compares three map visualization approaches for the CTA analytics dashboard. The current implementation uses Streamlit + PyDeck. We evaluated Plotly Mapbox and Kepler.gl as alternatives.

---

## Candidates

### 1. PyDeck (Current)

The existing dashboard uses `pydeck` via Streamlit's native `st.pydeck_chart()`.

**Pros:**
- Native Streamlit integration (no plugins)
- Good performance with 10k+ points
- Supports ScatterplotLayer, PathLayer, HeatmapLayer
- Simple Python API
- Tooltips on hover

**Cons:**
- Limited chart interactivity (no click events back to Streamlit)
- No built-in time-series animation
- Heatmap support requires manual setup
- Styling options are moderate

### 2. Plotly Mapbox (Prototype Built)

A working prototype exists at `dashboard/plotly_prototype.py`.

**Pros:**
- Easy to set up (pure Python, no API key needed with carto-positron)
- Smooth zoom/pan interactions
- Built-in color scales (Viridis, etc.) for continuous data
- Native scatter + line trace on same map (good for route shapes + stops)
- Consistent with Plotly charts elsewhere in the dashboard
- Hover data is highly configurable

**Cons:**
- Performance degrades above ~5k points (OK for filtered views, not full dataset)
- No built-in heatmap layer (can approximate with density_mapbox)
- Less control over layer composition vs PyDeck
- Map styles limited to Mapbox/Carto presets without API key

### 3. Kepler.gl

Could not install due to build dependency conflict with Python 3.14 + pyarrow. Evaluated based on documentation.

**Pros:**
- Most powerful geospatial visualization tool
- Built-in heatmaps, hex bins, arcs, trips animation
- Time-series playback (ideal for future realtime data)
- Drag-and-drop layer configuration
- Excellent for demos and presentations

**Cons:**
- Heavy dependency (failed to install on Python 3.14)
- Requires streamlit-keplergl plugin
- Configuration is JSON-based (less Pythonic)
- Overkill for current batch analytics
- Slow initial load with large datasets

---

## Comparison Matrix

| Criteria                    | PyDeck (current) | Plotly Mapbox | Kepler.gl     |
|-----------------------------|------------------|---------------|---------------|
| Setup complexity            | Low              | Low           | High          |
| Streamlit integration       | Native           | Native        | Plugin needed |
| Performance (11k points)    | Good             | Fair          | Good          |
| Scatter maps                | Yes              | Yes           | Yes           |
| Route path rendering        | Yes (PathLayer)  | Yes (lines)   | Yes           |
| Heatmaps                    | Manual           | Approximate   | Built-in      |
| Time-series animation       | No               | No            | Yes           |
| Color scales                | Manual           | Built-in      | Built-in      |
| Hover tooltips              | Yes              | Yes           | Yes           |
| Click interactivity         | Limited          | Limited       | Limited       |
| Custom styling              | Medium           | Medium        | Excellent     |
| Python 3.14 compatibility   | Yes              | Yes           | No            |

---

## Recommendation

**Stay with PyDeck for the production dashboard.** It's native to Streamlit, performs well at our data scale, and supports both scatter and path layers. The new dashboard already uses PathLayer for route shapes.

**Use Plotly Mapbox for supplementary views** where continuous color scales or scatter-line combos are more natural (e.g., the route exploration view). The prototype at `dashboard/plotly_prototype.py` demonstrates this.

**Revisit Kepler.gl when implementing realtime streaming** (Issue #7). Its time-series playback and animation features would be valuable for live vehicle tracking. Wait for Python 3.14 compatibility to stabilize.

---

## Prototype

Run the Plotly prototype:
```bash
streamlit run dashboard/plotly_prototype.py
```

It replicates two views:
1. Busiest stops scatter map with Viridis color scale
2. Route exploration with shape paths rendered as Mapbox line traces

---

## References

- PyDeck: https://deckgl.readthedocs.io/en/latest/
- Plotly Mapbox: https://plotly.com/python/mapbox-scatter-plots/
- Kepler.gl: https://kepler.gl/
