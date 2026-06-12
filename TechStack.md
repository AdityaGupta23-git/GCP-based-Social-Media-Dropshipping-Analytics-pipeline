# 🛠️ Tech Stack — GCP Social Media & Dropshipping Analytics Pipeline

## Primary Services

| Stage | Technology | Version / Tier | Role |
|-------|-----------|---------------|------|
| Raw Storage | Google Cloud Storage | Standard | Immutable raw zone + curated Parquet zone |
| Data Warehouse | Snowflake | Enterprise (trial) | Staging, cleaning, type enforcement, UNLOAD |
| Big Data Processing | PySpark on GCP Dataproc | Spark 3.3 | JSON parsing, joins, feature engineering, outlier detection |
| Analytical Store | Google BigQuery | Serverless | enriched table + 7 data mart tables |
| BI & Dashboards | Looker | Enterprise | LookML models, campaign & manager dashboards |
| Orchestration | GCP Workflows | Serverless | End-to-end pipeline automation |

---

## Supporting Services

| Service | Purpose |
|---------|---------|
| GCP Cloud Monitoring | Pipeline health alerts, SLA tracking |
| GCP Pub/Sub | Event-driven triggers between stages |
| Snowpipe | Near-real-time ingestion from GCS to Snowflake |
| BigQuery Data Transfer Service | Scheduled loads from GCS to BigQuery |

---

## Programming Languages & Tools

| Technology | Usage |
|-----------|-------|
| **Python 3.10** | PySpark job scripts, GCP Workflows helper functions |
| **PySpark** | Distributed data processing on Dataproc |
| **SQL (Snowflake)** | Staging, cleaning, CTAS transformations |
| **SQL (BigQuery)** | Data mart creation, aggregation queries |
| **LookML** | Looker metric definitions and dashboard models |
| **YAML** | GCP Workflows orchestration definition |
| **gsutil / gcloud CLI** | GCS operations, Dataproc job submission |

---

## Key Snowflake SQL Patterns

```sql
-- Deduplication
SELECT DISTINCT * FROM STG_SOCIAL_MEDIA;

-- Type casting
SELECT
  CAST(POST_DATE AS DATE) AS post_date,
  CAST(AD_SPEND AS FLOAT) AS ad_spend
FROM STG_SOCIAL_MEDIA;

-- Placeholder replacement
SELECT
  NULLIF(engagement_rate, -1) AS engagement_rate,
  NULLIF(ad_spend, 999) AS ad_spend
FROM STG_SOCIAL_MEDIA;

-- Platform normalization
SELECT
  CASE LOWER(platform)
    WHEN 'fb'       THEN 'facebook'
    WHEN 'ig'       THEN 'instagram'
    WHEN 'a.p.a.c.' THEN 'apac'
    ELSE LOWER(platform)
  END AS platform
FROM STG_SOCIAL_MEDIA;

-- Domain validation
SELECT * FROM STG_SOCIAL_MEDIA
WHERE engagement_rate <= 100
  AND ad_spend >= 0
  AND number_of_likes <= impressions;
```

---

## Key PySpark Patterns

```python
# JSON parsing from embedded string
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, DoubleType, StringType

schema = StructType([...])  # define Post_Metadata fields
df = df.withColumn("metadata", from_json(col("Post_Metadata"), schema))
df = df.select("*", "metadata.*")

# Cross-source join
enriched = social_df \
    .join(likes_df,    on="Post_ID",  how="left") \
    .join(managers_df, on="Platform", how="left") \
    .join(dropship_df, on="Post_ID",  how="left")

# Feature engineering
from pyspark.sql.functions import col
enriched = enriched \
    .withColumn("likes_per_dollar", col("Number_of_Likes") / col("Ad_Spend")) \
    .withColumn("engagement_score", (col("Number_of_Likes") / col("Impressions")) * 100)

# Z-score outlier detection
from pyspark.sql.functions import mean, stddev, abs as spark_abs
stats = enriched.select(mean("Ad_Spend"), stddev("Ad_Spend")).collect()[0]
enriched = enriched \
    .withColumn("z_score", (col("Ad_Spend") - stats[0]) / stats[1]) \
    .withColumn("is_spend_outlier", spark_abs(col("z_score")) > 3.0)
```

---

## Architecture Decision Log

| Decision | Rationale |
|----------|-----------|
| Snowflake for cleaning (not BigQuery) | Superior SQL, VARIANT type for semi-structured data, time-travel for data recovery |
| Parquet export to GCS between Snowflake & Dataproc | Columnar format reduces Spark I/O by ~70%; date partitioning enables pruning |
| PySpark for JSON parsing | Native `from_json` handles malformed embedded strings robustly; Spark distributes parse across nodes |
| BigQuery as analytical layer | Serverless scaling, Looker native integration, cost-effective for ad-hoc analytics |
| GCP Workflows for orchestration | Serverless, YAML-native, integrates with all GCP services without Airflow overhead |
| Z-score for outlier detection | Statistically robust for ad spend distributions; threshold of 3σ flags ~0.3% of data |
