# 🏗️ Architecture — GCP Social Media & Dropshipping Analytics Pipeline

## Overview

This pipeline follows a **multi-layer medallion architecture** (Raw → Staging → Curated → Enriched → Marts) deployed entirely on GCP, with Snowflake as the primary warehouse for cleaning and Dataproc/BigQuery for analytics-scale processing.

---

## Full Architecture Diagram

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                        DATA SOURCES (External)                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  ┌──────────────────┐  ┌──────────────┐  ┌──────────────┐  ┌────────────┐  ║
║  │ Social Media API │  │  Likes CSV   │  │ Managers CSV │  │ Dropship   │  ║
║  │ (Campaigns, Ads) │  │ (Post Likes) │  │ (HR/Mgmt)    │  │ CSV (JSON) │  ║
║  └────────┬─────────┘  └──────┬───────┘  └──────┬───────┘  └─────┬──────┘  ║
╚═══════════╪══════════════════╪═══════════════════╪════════════════╪═════════╝
            │                  │                   │                │
            └──────────────────┴───────────────────┴────────────────┘
                                        │
                                        ▼
╔══════════════════════════════════════════════════════════════════════════════╗
║                    LAYER 1: RAW INGESTION                                   ║
║                    Google Cloud Storage (Raw Zone)                          ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  Bucket: gs://social-dropship-raw/                                          ║
║  ┌──────────────────────────────────────────────────────────────────────┐   ║
║  │  /social_media/      /likes/       /managers/     /dropshipping/    │   ║
║  │  social_media.csv    likes.csv     managers.csv   dropship.csv      │   ║
║  └──────────────────────────────────────────────────────────────────────┘   ║
╚══════════════════════════════════════════════════════════════════════════════╝
                                        │
                                        │ Snowpipe / Scheduled COPY INTO
                                        ▼
╔══════════════════════════════════════════════════════════════════════════════╗
║                    LAYER 2: STAGING & CLEANING                              ║
║                    Snowflake Data Warehouse                                 ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  Database: SOCIAL_ANALYTICS_DB                                              ║
║                                                                              ║
║  Staging Tables (raw copy):          Curated Tables (cleaned):              ║
║  ┌──────────────────────┐            ┌──────────────────────────┐           ║
║  │ STG_SOCIAL_MEDIA     │  ──────►   │ CUR_SOCIAL_MEDIA         │           ║
║  │ STG_LIKES            │  clean &   │ CUR_LIKES                │           ║
║  │ STG_MANAGERS         │  validate  │ CUR_MANAGERS             │           ║
║  │ STG_DROPSHIPPING     │  ──────►   │ CUR_DROPSHIPPING         │           ║
║  └──────────────────────┘            └──────────────────────────┘           ║
║                                                                              ║
║  Cleaning Operations:                                                        ║
║  • NULL/placeholder replacement     • DISTINCT deduplication on Post_ID     ║
║  • CAST data types                  • LOWER(platform) normalization         ║
║  • Date format standardization      • Domain constraint validation          ║
╚══════════════════════════════════════════════════════════════════════════════╝
                                        │
                                        │ UNLOAD to GCS (Parquet, date-partitioned)
                                        ▼
╔══════════════════════════════════════════════════════════════════════════════╗
║                    LAYER 3: CURATED ZONE                                    ║
║                    Google Cloud Storage (Parquet)                           ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  Bucket: gs://social-dropship-curated/                                      ║
║  ┌──────────────────────────────────────────────────────────────────────┐   ║
║  │  /social_media/dt=2024-03-20/part-*.parquet                         │   ║
║  │  /likes/dt=2024-03-20/part-*.parquet                                │   ║
║  │  /managers/dt=2024-03-20/part-*.parquet                             │   ║
║  │  /dropshipping/dt=2024-03-20/part-*.parquet                         │   ║
║  └──────────────────────────────────────────────────────────────────────┘   ║
║  Format: Parquet (columnar, compressed, fast analytics reads)               ║
╚══════════════════════════════════════════════════════════════════════════════╝
                                        │
                                        │ Dataproc reads Parquet via Spark
                                        ▼
╔══════════════════════════════════════════════════════════════════════════════╗
║                    LAYER 4: ADVANCED PROCESSING                             ║
║                    PySpark on GCP Dataproc                                  ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  Cluster: analytics-cluster (managed Spark)                                 ║
║                                                                              ║
║  Processing Steps:                                                           ║
║  ┌──────────────────────────────────────────────────────────────────────┐   ║
║  │  1. JSON Parsing        Extract fields from Post_Metadata strings    │   ║
║  │  2. Cross-source Joins  Social ⋈ Likes ⋈ Managers ⋈ Dropshipping   │   ║
║  │  3. Feature Engineering likes_per_dollar, engagement_score, etc.    │   ║
║  │  4. Outlier Detection   Z-score on ad_spend; flag anomalies          │   ║
║  │  5. Output              enriched_performance_data                    │   ║
║  └──────────────────────────────────────────────────────────────────────┘   ║
╚══════════════════════════════════════════════════════════════════════════════╝
                                        │
                                        │ Write to BigQuery
                                        ▼
╔══════════════════════════════════════════════════════════════════════════════╗
║                    LAYER 5: ANALYTICAL DATA WAREHOUSE                       ║
║                    Google BigQuery                                          ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  Dataset: social_analytics                                                  ║
║                                                                              ║
║  Core Table:                      Data Marts (Aggregated):                  ║
║  ┌───────────────────────┐        ┌───────────────────────────────────┐     ║
║  │ enriched_performance_ │──────► │ ad_roi_analysis                   │     ║
║  │ data                  │        │ campaign_efficiency_summary        │     ║
║  │ (unified, enriched)   │        │ daily_post_performance             │     ║
║  └───────────────────────┘        │ platform_performance_summary       │     ║
║                                   │ region_performance_summary         │     ║
║                                   │ time_performance_summary           │     ║
║                                   │ manager_performance_summary        │     ║
║                                   └───────────────────────────────────┘     ║
╚══════════════════════════════════════════════════════════════════════════════╝
                                        │
                                        │ Looker connects directly
                                        ▼
╔══════════════════════════════════════════════════════════════════════════════╗
║                    LAYER 6: VISUALIZATION                                   ║
║                    Looker BI Platform                                       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  LookML Models → Explores → Dashboards                                      ║
║  • Top Performing Campaigns        • Manager Performance Analysis            ║
║  • Geographic Performance          • Likes vs Ad Spend Correlation          ║
╚══════════════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════════╗
║                    ORCHESTRATION: GCP Workflows                             ║
║  Triggers and coordinates all 6 layers end-to-end on schedule or event     ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

---

## Component Descriptions

### Google Cloud Storage (Raw + Curated)
- **Raw zone:** Landing pad for all 4 source files; immutable, source of truth
- **Curated zone:** Date-partitioned Parquet files exported from Snowflake

### Snowflake
- Acts as the **primary enterprise data warehouse** for staging and cleaning
- Staging tables hold raw copies; curated tables hold validated, typed data
- Snowpipe enables near-real-time ingestion from GCS

### PySpark on Dataproc
- Handles tasks that require distributed compute: JSON parsing, multi-source joins, Z-score outlier detection
- Output: `enriched_performance_data` — the unified analytics-ready dataset

### BigQuery
- Columnar, serverless analytical store
- Holds the enriched table + 7 pre-aggregated data marts for fast BI queries

### Looker
- Connected directly to BigQuery via LookML
- Defines reusable metrics (engagement score, likes per dollar, ROAS)
- Powers campaign, manager, and geographic dashboards

### GCP Workflows
- YAML-based orchestration service
- Coordinates GCS upload → Snowflake load → clean → export → Dataproc → BigQuery → notifications
