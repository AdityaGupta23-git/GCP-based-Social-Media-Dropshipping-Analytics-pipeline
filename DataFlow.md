# 🔄 Data Flow — GCP Social Media & Dropshipping Analytics Pipeline

## End-to-End Flow

```
[1] DATA SOURCES
    ├── Social Media API  ──┐
    ├── Likes CSV           ├── Manual export / scheduled pull
    ├── Managers CSV        │
    └── Dropshipping CSV  ──┘
              │
              │  Upload (gsutil / GCP Transfer Service)
              ▼
[2] GOOGLE CLOUD STORAGE — Raw Zone
    gs://social-dropship-raw/
    ├── social_media/social_media.csv
    ├── likes/likes.csv
    ├── managers/managers.csv
    └── dropshipping/dropshipping.csv

    → GCS event notification fires → GCP Workflows triggered
              │
              │  Snowpipe COPY INTO (continuous) or scheduled batch
              ▼
[3] SNOWFLAKE — Staging Tables
    Database: SOCIAL_ANALYTICS_DB
    Schema:   RAW
    ├── STG_SOCIAL_MEDIA     (raw copy, no transformations)
    ├── STG_LIKES
    ├── STG_MANAGERS
    └── STG_DROPSHIPPING

    Data Issues present at this stage:
    ✗ NULLs, NA, -1, 999 placeholders
    ✗ Duplicates (repeated API calls)
    ✗ Mixed date formats
    ✗ Facebook vs FB inconsistencies
    ✗ Engagement Rate > 100
    ✗ Embedded JSON strings in Dropshipping
              │
              │  SQL Transformation jobs in Snowflake
              ▼
[4] SNOWFLAKE — Curated Tables
    Schema: CURATED
    ├── CUR_SOCIAL_MEDIA     (validated, deduped, typed, normalized)
    ├── CUR_LIKES
    ├── CUR_MANAGERS
    └── CUR_DROPSHIPPING

    Transformations applied:
    ✓ NULLIF(-1, ''), COALESCE for placeholders
    ✓ DISTINCT on Post_ID (deduplication)
    ✓ CAST(POST_DATE AS DATE)
    ✓ LOWER(platform), platform normalization map
    ✓ WHERE Engagement_Rate <= 100 AND Ad_Spend >= 0
    ✓ Unit standardization (% to decimal, UTC timezone)
              │
              │  Snowflake UNLOAD → GCS (Parquet, partitioned by date)
              ▼
[5] GOOGLE CLOUD STORAGE — Curated Zone (Parquet)
    gs://social-dropship-curated/
    ├── social_media/dt=2024-03-20/part-00000.parquet
    ├── likes/dt=2024-03-20/part-00000.parquet
    ├── managers/dt=2024-03-20/part-00000.parquet
    └── dropshipping/dt=2024-03-20/part-00000.parquet

    Format benefits:
    → Columnar storage (fast analytical reads)
    → Snappy compression (~60% size reduction)
    → Schema embedded (no header parsing needed)
    → Date partitioning (scan only relevant days)
              │
              │  Dataproc cluster reads via Spark
              ▼
[6] PYSPARK ON GCP DATAPROC
    Cluster: analytics-cluster

    Step 6a: Read all 4 Parquet sources
    ┌─────────────────────────────────────────────────────────┐
    │  social_df    = spark.read.parquet("gs://.../social/")  │
    │  likes_df     = spark.read.parquet("gs://.../likes/")   │
    │  managers_df  = spark.read.parquet("gs://.../managers/")│
    │  dropship_df  = spark.read.parquet("gs://.../dropship/")│
    └─────────────────────────────────────────────────────────┘

    Step 6b: JSON Parsing (Dropshipping Post_Metadata)
    ┌─────────────────────────────────────────────────────────┐
    │  Extract from embedded JSON string:                     │
    │  Engagement_Score, Ad_Frequency, Conversion_Cost,       │
    │  Region_Code, Device_Type, Browser_Info, Bounce_Rate,   │
    │  Scroll_Depth, Viewability_Score, Ad_Relevance,         │
    │  Keyword_Density, Spam_Flag, Retention_Rate,            │
    │  Churn_Probability, Session_Duration, Conversion_Window │
    └─────────────────────────────────────────────────────────┘

    Step 6c: Cross-source Join
    ┌─────────────────────────────────────────────────────────┐
    │  enriched = social_df                                   │
    │    .join(likes_df,    on="Post_ID",    how="left")      │
    │    .join(managers_df, on="Platform",   how="left")      │
    │    .join(dropship_df, on="Post_ID",    how="left")      │
    └─────────────────────────────────────────────────────────┘

    Step 6d: Feature Engineering
    ┌─────────────────────────────────────────────────────────┐
    │  likes_per_dollar    = Likes / Ad_Spend                 │
    │  engagement_score    = (Likes + Comments + Shares)      │
    │                        / Impressions * 100              │
    │  effective_ctr       = Conversions / Clicks * 100       │
    │  spend_efficiency    = ROAS / CPC                       │
    │  viewability_adj_cpm = CPM * Viewability_Score          │
    └─────────────────────────────────────────────────────────┘

    Step 6e: Outlier Detection (Z-score on Ad_Spend)
    ┌─────────────────────────────────────────────────────────┐
    │  mean_spend = Ad_Spend.mean()                           │
    │  std_spend  = Ad_Spend.std()                            │
    │  z_score    = (Ad_Spend - mean_spend) / std_spend       │
    │  is_outlier = abs(z_score) > 3.0                        │
    └─────────────────────────────────────────────────────────┘
              │
              │  Write enriched_performance_data to BigQuery
              ▼
[7] GOOGLE BIGQUERY — Core Table
    Dataset: social_analytics
    Table:   enriched_performance_data
    (Unified dataset: ~40+ columns, all sources merged)
              │
              │  SQL aggregation jobs
              ▼
[8] BIGQUERY — Data Marts
    ├── ad_roi_analysis              (ROAS, CPA, revenue per campaign)
    ├── campaign_efficiency_summary  (CTR, conversion, spend efficiency)
    ├── daily_post_performance       (date-wise engagement trends)
    ├── platform_performance_summary (Facebook vs Instagram vs TikTok)
    ├── region_performance_summary   (APAC vs NA vs EMEA)
    ├── time_performance_summary     (hourly, weekly, monthly patterns)
    └── manager_performance_summary  (manager-level ROAS, spend, reach)
              │
              │  Looker direct connection
              ▼
[9] LOOKER DASHBOARDS
    ├── Top Performing Campaigns
    ├── Manager Performance Analysis
    ├── Geographic Performance Map
    └── Likes vs Ad Spend Correlation
```

---

## GCS Folder Structure

```
gs://social-dropship-raw/
├── social_media/
│   └── social_media_YYYYMMDD.csv
├── likes/
│   └── likes_YYYYMMDD.csv
├── managers/
│   └── managers.csv
└── dropshipping/
    └── dropshipping_YYYYMMDD.csv

gs://social-dropship-curated/
├── social_media/
│   └── dt=2024-03-20/
│       └── part-00000.parquet
├── likes/dt=.../
├── managers/dt=.../
└── dropshipping/dt=.../
```

---

## Data States Summary

| Layer | Location | Format | Quality State |
|-------|----------|--------|--------------|
| Raw | GCS `/raw/` | CSV | Original, unmodified |
| Staging | Snowflake STG_* | Native | Raw copy, no transforms |
| Curated | Snowflake CUR_* | Native | Cleaned, typed, deduped |
| Parquet | GCS `/curated/` | Parquet | Compressed, partitioned |
| Enriched | BigQuery `enriched_performance_data` | Columnar | Joined, feature-engineered |
| Marts | BigQuery `*_summary` | Columnar | Aggregated, BI-ready |

---

## Error Handling

| Failure Point | Detection | Action |
|--------------|-----------|--------|
| GCS upload failure | GCP Workflows step failure | Retry 3x, alert via Cloud Monitoring |
| Snowflake COPY INTO error | Snowflake task failure | Log to error table, skip bad rows |
| Dataproc job failure | GCP Workflows step failure | Retry 2x, send PubSub notification |
| BigQuery load error | Table insert rejection | Log rejected rows, alert admin |
| Validation rule violation | Row-level WHERE filter | Move to quarantine table |
