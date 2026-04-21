# Presentation Outline

## Goal

Deliver a 15-minute presentation that:

- is clear to a semi-technical audience
- covers the full data pipeline
- shows meaningful insights
- makes every teammate sound confident and informed

## Recommended 15-Minute Flow

### Slide 1: Title + What We Built

Speaker:

- Fortuna

Message:

- We built a multi-city transit intelligence platform with both daily batch analytics and live operations monitoring.

### Slide 2: Why This Problem Matters

Speaker:

- Fortuna

Message:

- Transit systems generate different kinds of data:
  - planned service
  - urban context
  - current live movement
- most tools emphasize only one of those views
- our project combines them

### Slide 3: Data Sources

Speaker:

- Fortuna

Message:

- GTFS static feeds from CTA and MBTA
- OpenStreetMap roads and POIs
- MBTA and CTA live feeds

Speaker notes:

- Keep this intuitive, not technical.
- End by saying the project has two coordinated views: historical batch and live operations.

### Slide 4: Batch Pipeline + Data Model

Speaker:

- Shangwe

Message:

- Ingest GTFS and OSM
- clean with Spark
- compute analytics
- load to Snowflake

Use the `raw -> clean -> analytics` framing.

Also say:

- Raw preserves source-of-truth source files
- Clean creates typed normalized joinable tables
- Analytics stores precomputed metrics for the dashboard

Speaker notes:

- This is where you sound most like a data engineer.
- Explain why the three-layer storage design helps usability, consistency, and performance.

### Slide 5: Batch Analytics Highlights

Speaker:

- Shangwe

Message:

- busiest stops
- busiest routes
- stop-level amenity access
- route-level access
- road coverage near transit

Speaker notes:

- Focus on what these metrics mean, not only how they were computed.
- This is your bridge from engineering to insight.

### Slide 6: Live Pipeline + End-To-End Flow Story

Speaker:

- Scott

Message:

- realtime pollers
- Kafka raw topics
- Flink latest-state job
- Kafka latest topics
- Redis serving store
- FastAPI + WebSocket
- React live map

Say this out loud:

- A single live vehicle update flows from the agency feed to Kafka to Flink to Redis to FastAPI to the React map, while historical GTFS and OSM data flow through Spark and Snowflake into dashboard queries.

Speaker notes:

- Keep the flow crisp and sequential.
- Emphasize that Redis is for serving and Kafka/Flink are for streaming/state handling.

### Slide 7: Why These Tools / What’s Technically Interesting

Speaker:

- Scott

Message:

- Spark for efficient processing over large GTFS schedule tables
- Snowflake for scalable historical analytical queries
- Kafka + Flink for real streaming architecture
- Redis for low-latency latest-state serving
- one API and one UI for both modes

Also emphasize:

- combining batch + streaming in one UI
- multi-city architecture
- joining transit with OSM context
- not querying the warehouse directly from the browser

Speaker notes:

- This is where you explain the architecture choices, not just list tools.
- End by handing off to the demo.

### Slide 8: Live Demo Through The Site

Speaker:

- Shangwe

Show:

- Batch Atlas
- Live Ops
- one city comparison
- one route spotlight

Keep it short enough that Q&A time stays protected.

Speaker notes:

- Show only the strongest views.
- Narrate what the audience is seeing in architecture terms.
- Don’t improvise too much here.

### Slide 9: Questions

Speaker:

- All three

Have backup notes ready for:

- source of truth
- reliability
- limitations
- scale justification

Speaker notes:

- Let the most relevant teammate answer first.
- Shangwe can unify or rescue answers if needed.

## Suggested Division For 3 Teammates

### Fortuna

- Slides 1 to 3
- problem, motivation, data sources

### Shangwe

- Slides 4 to 5
- batch architecture, storage model, analytics

### Scott

- Slides 6 to 9
- live architecture, tool choices

### Shangwe Again

- Slide 8 demo
- Slide 9 questions moderation

## One-Line Explanations To Memorize

### GTFS

GTFS is the transit schedule standard that tells us what service is planned.

### OSM

OpenStreetMap gives us neighborhood context like roads, food, healthcare, parks, and schools near transit.

### Spark

Spark lets us clean and aggregate large GTFS datasets like stop times and shapes into parquet analytics outputs.

### Snowflake

Snowflake is our warehouse and batch source of truth for historical analytics and dashboard batch queries.

### Kafka

Kafka is the event buffer for live vehicle updates.

### Flink

Flink is the stateful streaming layer that keeps only the latest event per vehicle.

### Redis

Redis is the fast serving layer for latest live vehicle state.

### FastAPI

FastAPI is the shared API layer for both live and batch data.

### React + deck.gl + MapLibre

That stack powers the interactive dashboard and map rendering.

## Questions To Be Ready For

1. Why use both batch and live data?
2. Why Boston and Chicago?
3. Why Spark?
4. Why Snowflake?
5. Why Kafka and Flink instead of just polling?
6. Why Redis?
7. Why not query Snowflake directly from the browser?
8. What does raw vs clean vs analytics mean?
9. What is your source of truth in live mode and batch mode?
10. How does the system recover from failure?
11. What would you improve next?

## Final Advice

Do not try to explain every file.

Do explain:

- the problem
- the data model
- the pipeline stages
- the architecture choices
- the main insights
- what is technically impressive

That is what will make the presentation feel clear and credible.

## Suggested Handoff Lines

Fortuna -> Shangwe:

- “Now that we’ve introduced the data sources and the product goal, Shangwe will show how we organized that raw data into a daily batch pipeline.”

Shangwe -> Scott:

- “That covers the historical side of the system. Scott will now explain the realtime streaming architecture that powers the live mode.”

Scott -> Shangwe:

- “Now that we’ve shown how the live and batch systems are built, Shangwe will show how both appear in the unified dashboard.”
