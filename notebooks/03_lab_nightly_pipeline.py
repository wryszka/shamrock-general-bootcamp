# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2 — The nightly pipeline (60 min)
# MAGIC
# MAGIC **What is this for?** The tables you build here become the *single source of truth*
# MAGIC for the rest of the day — the dashboard, the Genie room, and the pricing model all
# MAGIC read them. Today someone refreshes numbers by hand; after this lab it's a scheduled
# MAGIC job nobody has to remember.
# MAGIC
# MAGIC Pattern: **bronze** (ingest as-is) → **silver** (clean, typed, deduped) → **gold**
# MAGIC (business-ready aggregates).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze — land the raw files exactly as they arrived
# MAGIC
# MAGIC Everything as **string**, plus metadata. If a question ever comes up about what
# MAGIC the source system actually sent, bronze is the answer.

# COMMAND ----------

for name in ["policies", "claims"]:
    (
        spark.read.csv(f"{RAW_VOLUME}/{name}.csv", header=True)  # all strings, on purpose
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(f"{name}.csv"))
        .write.mode("overwrite").saveAsTable(f"1_bronze_{name}")
    )
display(spark.sql("SELECT count(*) AS bronze_policy_rows FROM 1_bronze_policies"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver — fix what the source got wrong
# MAGIC
# MAGIC The raw files contain (on purpose): duplicate rows, impossible dates like
# MAGIC `31/02/2025`, negative premiums, and `-1` claim amounts. Silver policies below is
# MAGIC done for you — **you** do silver claims.

# COMMAND ----------

silver_policies = (
    spark.table("1_bronze_policies")
    .dropDuplicates(["policy_id"])
    # try_to_date: bad dates (like 31/02/2025) become NULL instead of failing the job.
    # Plain to_date would crash on the first bad row — try again with it later and see.
    .withColumn("start_date", F.expr("try_to_date(start_date, 'yyyy-MM-dd')"))
    .withColumn("annual_premium", F.col("annual_premium").cast("double"))
    .withColumn("driver_age", F.col("driver_age").cast("int"))
    .withColumn("ncd_years", F.col("ncd_years").cast("int"))
    .withColumn("penalty_points", F.col("penalty_points").cast("int"))
    .withColumn("engine_cc", F.col("engine_cc").cast("int"))
    .filter("start_date IS NOT NULL AND annual_premium > 0")
    .drop("_ingested_at", "_source_file")
)
silver_policies.write.mode("overwrite").saveAsTable("2_silver_policies")

before = spark.table("1_bronze_policies").count()
after = spark.table("2_silver_policies").count()
print(f"bronze {before:,} -> silver {after:,}  ({before - after:,} rows cleaned out)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✏️ EXERCISE — silver claims
# MAGIC
# MAGIC Build `2_silver_claims` from `1_bronze_claims`:
# MAGIC - dedupe on `claim_id`
# MAGIC - cast `claim_date` to date (use `try_to_date` like above), `incurred_amount` to double
# MAGIC - drop rows where `incurred_amount` is negative (the `-1` placeholders)
# MAGIC - drop the `_ingested_at` / `_source_file` columns

# COMMAND ----------

# TODO: your code here
silver_claims = (
    spark.table("1_bronze_claims")
    # ...
)
# silver_claims.write.mode("overwrite").saveAsTable("2_silver_claims")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold — the tables the business actually asks for
# MAGIC
# MAGIC Two tables everyone downstream uses. First one is done for you; you build the second.

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE 3_gold_portfolio_summary AS
    SELECT
        p.county,
        CASE WHEN p.driver_age < 25 THEN 'under 25'
             WHEN p.driver_age < 40 THEN '25-39'
             WHEN p.driver_age < 60 THEN '40-59'
             ELSE '60+' END                          AS age_band,
        p.cover_type,
        count(*)                                     AS policies,
        round(sum(p.annual_premium), 2)              AS gross_written_premium,
        count(c.claim_id)                            AS claims,
        round(count(c.claim_id) / count(*), 4)       AS claim_frequency,
        round(coalesce(sum(c.incurred_amount), 0), 2) AS total_incurred
    FROM 2_silver_policies p
    LEFT JOIN 2_silver_claims c USING (policy_id)
    GROUP BY ALL
""")
display(spark.table("3_gold_portfolio_summary").orderBy(F.desc("gross_written_premium")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✏️ EXERCISE — monthly loss ratio
# MAGIC
# MAGIC Build `3_gold_loss_ratio_monthly`: by **county** and **claim month**
# MAGIC (`date_trunc('month', claim_date)`), total incurred, total premium of the policies
# MAGIC that claimed, and `loss_ratio = incurred / premium`. Bonus: add claim counts by type.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE TABLE 3_gold_loss_ratio_monthly AS ...

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schedule it — this is the "nightly" part (UI, ~5 min)
# MAGIC
# MAGIC 1. Click **Schedule** (top right of this notebook) → *Add schedule*
# MAGIC 2. Daily at **02:00**, serverless compute
# MAGIC 3. Open **Jobs & Pipelines** in the left nav and find your job — that page is where
# MAGIC    you'd see run history, failures, and alerts
# MAGIC
# MAGIC **Checkpoint:** the job exists and a manual *Run now* succeeds end-to-end.
# MAGIC
# MAGIC **Discussion:** what's the equivalent process today, and what happens when the
# MAGIC person who runs it is on holiday?
