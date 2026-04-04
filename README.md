# Chicago Transit Data Pipeline

## Overview

This project builds an end-to-end data pipeline for analyzing public transit data in Chicago using GTFS (General Transit Feed Specification) data.

The pipeline ingests raw transit data, processes it using Spark, and loads structured datasets into Snowflake for analytics and querying. The goal is to create a scalable data engineering workflow that can later be extended to real-time streaming.

---

## What’s Implemented (Current State)

I currently have a fully working **batch data pipeline**:

### ✅ Data Ingestion

* GTFS static data downloaded via Python scripts
* Stored locally under `data/raw/gtfs`

### ✅ Data Processing (Spark)

* Cleaned GTFS datasets:

  * Stops
  * Routes
  * Trips
  * Stop Times
  * Shapes
* Output stored as partitioned Parquet files

### ✅ Data Warehouse (Snowflake)

Three-layer modeling approach:

* **RAW (source-of-truth)**
* **CLEAN (processed datasets)**
* **ANALYTICS (aggregated insights)**

### 📊 Loaded Data Stats

* CLEAN_GTFS_STOPS → 11,184 rows

* CLEAN_GTFS_ROUTES → 131 rows

* CLEAN_GTFS_TRIPS → 94,496 rows

* CLEAN_GTFS_STOP_TIMES → 5,838,352 rows

* CLEAN_GTFS_SHAPES → 1,402,954 rows

* ANALYTICS_STOP_ACTIVITY → 11,027 rows

* ANALYTICS_STOP_ACTIVITY_ENRICHED → 11,027 rows

* ANALYTICS_ROUTE_ACTIVITY → 131 rows

* ANALYTICS_STOP_ACTIVITY_BY_ROUTE → 14,388 rows

* ANALYTICS_ROUTE_SHAPES → 1,402,954 rows

---

## 🏗️ Pipeline Architecture (Batch)

```
GTFS Static (CSV)        OSM Data (PBF/GeoJSON)
        ↓                        ↓
     Python Scripts (download)
                ↓
             Airflow
        (orchestration)
                ↓
            Spark (PySpark)
     - cleaning
     - filtering
     - joins
                ↓
           Snowflake
     - raw tables
     - clean tables
     - analytics tables
                ↓
          SQL Queries
                ↓
        Dashboard / Map UI
```

---

## 🔮 Future Architecture (Streaming)

Planned real-time pipeline:

```
GTFS-Realtime Feed
        ↓
   Python Poller
        ↓
      Kafka
        ↓
      Flink
   - windowed aggregations
   - delay metrics
        ↓
   Snowflake / Serving Layer
        ↓
   Live Dashboard
```

---

## 📁 Project Structure

Key folders:

* `jobs/`

  * ingestion → download scripts
  * spark → data processing jobs
  * load → Snowflake loading
  * validation → data checks

* `data/`

  * raw → original GTFS files
  * processed → cleaned + analytics parquet

* `sql/`

  * ddl → table definitions
  * queries → analysis queries
  * validation → quality checks

* `dags/`

  * Airflow pipelines

* `src/`

  * reusable logic (connectors, transformers, configs)

* `dashboard/`

  * UI layer (future / optional)

---

## ⚙️ How to Run (Local)

### 1. Setup environment

```
pip install -r requirements.txt
```

### 2. Configure Snowflake

Create `.env` with:

```
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PRIVATE_KEY_FILE=...
SNOWFLAKE_ROLE=TRAINING_ROLE
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
```

### 3. Test connection

```
python -m scripts.test_snowflake_connection
```

---

### 4. Run full pipeline

```
bash scripts/run_local_pipeline.sh
```

OR manually:

```
python jobs/ingestion/download_gtfs.py
python jobs/spark/build_analytics.py
python -m jobs.load.load_to_snowflake
```

---

## ✅ Data Validation

Example checks:

```sql
SELECT COUNT(*) FROM CLEAN_GTFS_STOPS;
SELECT COUNT(*) FROM ANALYTICS_ROUTE_ACTIVITY;
```

---

## 💡 Key Design Decisions

* Used **Snowflake** for scalable analytics storage
* Used **Spark** for distributed data processing
* Used **Parquet** for efficient intermediate storage
* Separated data into **raw → clean → analytics layers**
* Designed pipeline to be **extendable to streaming (Kafka + Flink)**

---

## 🎯 Next Steps

* Add Airflow orchestration (full DAG execution)
* Implement data quality validation checks
* Build dashboard for visualization
* Integrate GTFS-realtime streaming pipeline

---

---
