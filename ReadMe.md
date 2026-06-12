# 📊 Scalable Social Media & Dropshipping Analytics Data Pipeline on GCP

> A production-grade, real-time data analytics pipeline on Google Cloud Platform that unifies social media campaign data, likes, manager performance, and dropshipping transactions — delivering actionable insights via BigQuery data marts and Looker dashboards.

---

## 📌 Table of Contents

- [Project Overview](#project-overview)
- [Architecture Diagram](#architecture-diagram)
- [Data Flow](#data-flow)
- [Data Sources](#data-sources)
- [Tech Stack](#tech-stack)
- [Data Quality Challenges & Solutions](#data-quality-challenges--solutions)
- [BigQuery Data Marts & Reports](#bigquery-data-marts--reports)
- [Looker Dashboards](#looker-dashboards)
- [Sample Datasets](#sample-datasets)
- [Pseudocode](#pseudocode)
- [Screenshots](#screenshots)
- [Setup Guide](#setup-guide)

---

## 📖 Project Overview

Modern e-commerce relies on social media marketing, but fragmented data across platforms, managers, and transactions makes evaluating campaign effectiveness difficult.

This pipeline solves that by:

- **Ingesting** data from 4 sources into Google Cloud Storage
- **Loading** raw data into Snowflake (primary data warehouse)
- **Cleaning & transforming** using Snowflake staging + PySpark on Dataproc
- **Exporting** curated Parquet datasets back to GCS (date-partitioned)
- **Loading** enriched data into BigQuery for analytical data marts
- **Visualising** via Looker dashboards connected directly to BigQuery
- **Orchestrating** the entire pipeline with GCP Workflows

### Key Objectives

| Objective | How It's Met |
|-----------|-------------|
| Unify fragmented data sources | 4-source integration (Social, Likes, Managers, Dropshipping) |
| Ensure data quality | 8+ validation rules applied at ingestion and transformation |
| Handle JSON-embedded metadata | PySpark JSON parsing on Dataproc |
| Enable scalable processing | Dataproc (managed Spark) + BigQuery columnar store |
| Deliver actionable insights | 7 BigQuery data marts + Looker dashboards |

---

## 🏗️ Architecture Diagram

See [`docs/architecture.md`](docs/architecture.md) for the full diagram.

```
┌─────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                   │
│  [Social Media API] [Likes CSV] [Managers CSV] [Dropship CSV]  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  Google Cloud Storage  │  ← Raw zone (4 source folders)
              │  (Raw Landing Zone)    │
              └────────────┬───────────┘
                           │  Snowpipe / Batch Load
                           ▼
              ┌────────────────────────┐
              │       Snowflake        │  ← Primary Data Warehouse
              │  (Staging → Curated)   │     Schema validation
              └────────────┬───────────┘     Deduplication
                           │  Cleaning       Type casting
                           │  Export (Parquet, partitioned by date)
                           ▼
              ┌────────────────────────┐
              │  Google Cloud Storage  │  ← Curated Parquet zone
              │  (Curated / Processed) │
              └────────────┬───────────┘
                           │  Dataproc cluster reads Parquet
                           ▼
              ┌────────────────────────┐
              │  PySpark on Dataproc   │  ← Advanced Processing
              │  • JSON parsing        │     Cross-source joins
              │  • Feature engineering │     Outlier detection (Z-score)
              │  • Enriched dataset    │
              └────────────┬───────────┘
                           │  Write enriched_performance_data
                           ▼
              ┌────────────────────────┐
              │       BigQuery         │  ← Analytical Data Warehouse
              │  enriched_performance_ │     7 Data Marts
              │  data + Data Marts     │
              └────────────┬───────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │        Looker          │  ← BI & Dashboards
              │  LookML models         │     Connected to BigQuery
              │  Campaign dashboards   │
              └────────────────────────┘

  ┌───────────────────────────────────┐
  │  GCP Workflows (Orchestration)   │  ← Coordinates all stages
  └───────────────────────────────────┘
```

---

## 🔄 Data Flow

See [`docs/data_flow.md`](docs/data_flow.md) for full documentation.

| Stage | Input | Process | Output |
|-------|-------|---------|--------|
| Ingestion | 4 source files | Upload to GCS `/raw/` | Partitioned raw files |
| Warehousing | GCS raw files | Snowflake COPY INTO | Staging tables |
| Cleaning | Staging tables | SQL transformations | Curated tables |
| Export | Snowflake curated | UNLOAD to GCS | Parquet files |
| Processing | Parquet files | PySpark on Dataproc | Enriched dataset |
| Analytics | Enriched data | BigQuery load | Data marts |
| Visualization | BigQuery marts | Looker LookML | Dashboards |

---

## 📂 Data Sources

### 1. Social Media Analytics Data
Campaign-level advertising metrics from social platforms.

Key fields: `Reach`, `Impressions`, `Engagement_Rate`, `CTR`, `Video_Views`, `Conversion_Rate`, `Ad_Spend`, `CPC`, `CPM`, `ROAS`, `Audience_Demographics`, `Campaign_ID`, `Ad_Set_ID`, `Ad_ID`

### 2. Likes Data
Post-level like counts for evaluating content popularity.

Key field: `Number_of_Likes`, `Post_ID`

### 3. Managers Data
Internal data on campaign managers and their assigned platforms.

Key fields: `Manager_Name`, `Senior_Manager_Name`, `Platform`

### 4. Dropshipping Data
Transaction-level order data with **embedded JSON metadata** requiring parsing.

Key fields: `Post_ID`, `Post_Metadata` (JSON string containing Engagement_Score, Ad_Frequency, Conversion_Cost, Region_Code, Device_Type, Browser_Info, Bounce_Rate, Scroll_Depth, Viewability_Score, etc.)

---

## 🛠️ Tech Stack

See [`docs/tech_stack.md`](docs/tech_stack.md) for details.

| Stage | Technology |
|-------|-----------|
| Raw Storage | Google Cloud Storage |
| Data Warehouse | Snowflake |
| Advanced Processing | PySpark on GCP Dataproc |
| Analytical Store | Google BigQuery |
| BI & Dashboards | Looker |
| Orchestration | GCP Workflows |
| Language | Python 3.10, SQL, LookML |

---

## 🧹 Data Quality Challenges & Solutions

| Challenge | Impact | Solution |
|-----------|--------|---------|
| Missing values (NULL, NA, -1, 999) | Distorts engagement averages | Replace placeholders, apply imputation |
| Duplicate records (API retries, re-uploads) | Inflates impressions/conversions | Dedup on `Post_ID` with `DISTINCT` |
| Inconsistent data types (POST_ID, dates) | Join failures | `CAST(POST_DATE AS DATE)` at ingestion |
| Formatting inconsistencies (FB vs Facebook) | Wrong groupings | `LOWER(platform)` + normalization map |
| Invalid values (Engagement Rate > 100, negative spend) | Domain violations | Validation rules in transformation |
| Inconsistent units (% vs decimal, currencies) | Non-comparable metrics | Standardization during processing |
| Embedded JSON strings in Dropshipping data | Unusable nested attributes | PySpark JSON parsing |
| Scale/range issues (Likes 10–1M) | Skewed ML features | Z-score normalization |

---

## 📊 BigQuery Data Marts & Reports

| Data Mart | Description |
|-----------|-------------|
| `ad_roi_analysis` | Return on ad investment per campaign |
| `campaign_efficiency_summary` | Marketing campaign performance KPIs |
| `daily_post_performance` | Daily engagement trends |
| `platform_performance_summary` | Cross-platform comparison |
| `region_performance_summary` | Geographic campaign effectiveness |
| `time_performance_summary` | Performance across time periods |
| `manager_performance_summary` | Campaign manager productivity |

---

## 📈 Looker Dashboards

LookML models define metrics including:
- Engagement Score
- Likes per Dollar
- Conversion Cost
- Ad Type Effectiveness

Example dashboards:
- **Top Performing Campaigns** — ROAS, CTR, conversion rankings
- **Manager Performance Analysis** — manager vs. senior manager KPIs
- **Geographic Performance** — region-wise ROAS and reach heatmaps
- **Likes vs. Ad Spend Correlation** — scatter plots and trend lines

---

## 📁 Sample Datasets

| File | Description |
|------|-------------|
| [`data/social_media.csv`](data/social_media.csv) | Sample ad campaign metrics |
| [`data/likes.csv`](data/likes.csv) | Post like counts |
| [`data/managers.csv`](data/managers.csv) | Manager-platform mapping |
| [`data/dropshipping.csv`](data/dropshipping.csv) | Dropshipping orders with embedded JSON |
| [`data/enriched_performance_data.csv`](data/enriched_performance_data.csv) | Sample PySpark output |

---

## 💻 Pseudocode

| File | Description |
|------|-------------|
| [`pseudocode/01_gcs_ingestion.md`](pseudocode/01_gcs_ingestion.md) | Upload data to GCS raw zone |
| [`pseudocode/02_snowflake_load_clean.md`](pseudocode/02_snowflake_load_clean.md) | COPY INTO staging + cleaning SQL |
| [`pseudocode/03_pyspark_processing.md`](pseudocode/03_pyspark_processing.md) | JSON parsing, joins, feature engineering |
| [`pseudocode/04_bigquery_marts.md`](pseudocode/04_bigquery_marts.md) | Data mart creation SQL |
| [`pseudocode/05_looker_lookml.md`](pseudocode/05_looker_lookml.md) | LookML model definitions |
| [`pseudocode/06_gcp_workflow.md`](pseudocode/06_gcp_workflow.md) | GCP Workflows orchestration YAML |

---

## 🖼️ Screenshots

See [`screenshots/README.md`](screenshots/README.md) for the full list.

---

## 🚀 Setup Guide

### Prerequisites
- GCP Project with billing enabled
- Snowflake account (free trial works)
- Python 3.10+, `gcloud` CLI, `pyspark`

### Steps

1. **Create GCS Buckets**
   ```bash
   gsutil mb gs://social-dropship-raw
   gsutil mb gs://social-dropship-curated
   ```

2. **Upload sample data**
   ```bash
   gsutil cp data/social_media.csv gs://social-dropship-raw/social_media/
   gsutil cp data/likes.csv        gs://social-dropship-raw/likes/
   gsutil cp data/managers.csv     gs://social-dropship-raw/managers/
   gsutil cp data/dropshipping.csv gs://social-dropship-raw/dropshipping/
   ```

3. **Configure Snowflake**
   - Create warehouse, database, and staging tables
   - Run `pseudocode/02_snowflake_load_clean.md` SQL

4. **Run PySpark on Dataproc**
   ```bash
   gcloud dataproc jobs submit pyspark pseudocode/03_pyspark_processing.md \
     --cluster=analytics-cluster \
     --region=us-central1
   ```

5. **Load into BigQuery**
   ```bash
   bq load --source_format=PARQUET \
     analytics.enriched_performance_data \
     gs://social-dropship-curated/enriched/*.parquet
   ```

6. **Create Data Marts** — Run SQL from `pseudocode/04_bigquery_marts.md`

7. **Connect Looker** — Point to BigQuery dataset, apply `pseudocode/05_looker_lookml.md`

8. **Deploy GCP Workflow** — Deploy YAML from `pseudocode/06_gcp_workflow.md`

---

## 👤 Author

**Aditya**  
GCP Social Media & Dropshipping Analytics Pipeline  
Built with Google Cloud Platform · Snowflake · PySpark · BigQuery · Looker
