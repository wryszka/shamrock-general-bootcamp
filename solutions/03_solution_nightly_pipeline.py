# Databricks notebook source
# MAGIC %md
# MAGIC # SOLUTION — Lab 2: The nightly pipeline
# MAGIC
# MAGIC Instructor version: runs end-to-end with all exercises completed.
# MAGIC Also invoked by `01_data_generator` when `build_all = yes`.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "shamrock_bootcamp", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RAW_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
spark.sql(f"USE {CATALOG}.{SCHEMA}")

from pyspark.sql import functions as F

# COMMAND ----------

# Bronze — land raw files as-is (all strings) + metadata
for name in ["policies", "claims"]:
    (
        spark.read.csv(f"{RAW_VOLUME}/{name}.csv", header=True)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(f"{name}.csv"))
        .write.mode("overwrite").saveAsTable(f"1_bronze_{name}")
    )

# COMMAND ----------

# Silver policies — dedupe, cast, drop bad rows
(
    spark.table("1_bronze_policies")
    .dropDuplicates(["policy_id"])
    .withColumn("start_date", F.to_date("start_date", "yyyy-MM-dd"))
    .withColumn("annual_premium", F.col("annual_premium").cast("double"))
    .withColumn("driver_age", F.col("driver_age").cast("int"))
    .withColumn("ncd_years", F.col("ncd_years").cast("int"))
    .withColumn("penalty_points", F.col("penalty_points").cast("int"))
    .withColumn("engine_cc", F.col("engine_cc").cast("int"))
    .filter("start_date IS NOT NULL AND annual_premium > 0")
    .drop("_ingested_at", "_source_file")
    .write.mode("overwrite").saveAsTable("2_silver_policies")
)

# COMMAND ----------

# Silver claims — EXERCISE SOLUTION
(
    spark.table("1_bronze_claims")
    .dropDuplicates(["claim_id"])
    .withColumn("claim_date", F.to_date("claim_date", "yyyy-MM-dd"))
    .withColumn("incurred_amount", F.col("incurred_amount").cast("double"))
    .filter("incurred_amount >= 0")
    .drop("_ingested_at", "_source_file")
    .write.mode("overwrite").saveAsTable("2_silver_claims")
)

# COMMAND ----------

# Gold 1 — portfolio summary
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

# COMMAND ----------

# Gold 2 — monthly loss ratio — EXERCISE SOLUTION
spark.sql("""
    CREATE OR REPLACE TABLE 3_gold_loss_ratio_monthly AS
    SELECT
        p.county,
        date_trunc('month', c.claim_date)                            AS claim_month,
        count(c.claim_id)                                            AS claims,
        sum(CASE WHEN c.claim_type = 'injury' THEN 1 ELSE 0 END)     AS injury_claims,
        round(sum(c.incurred_amount), 2)                             AS total_incurred,
        round(sum(p.annual_premium), 2)                              AS premium_of_claiming_policies,
        round(sum(c.incurred_amount) / sum(p.annual_premium), 4)     AS loss_ratio
    FROM 2_silver_claims c
    JOIN 2_silver_policies p USING (policy_id)
    GROUP BY p.county, date_trunc('month', c.claim_date)
""")

print("pipeline complete: bronze -> silver -> gold")
