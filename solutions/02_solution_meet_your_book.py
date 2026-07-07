# Databricks notebook source
# MAGIC %md
# MAGIC # SOLUTION — Lab 1: Meet your book

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "shamrock_bootcamp", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RAW_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

spark.read.csv(f"{RAW_VOLUME}/policies.csv", header=True, inferSchema=True).createOrReplaceTempView("policies")
spark.read.csv(f"{RAW_VOLUME}/claims.csv", header=True, inferSchema=True).createOrReplaceTempView("claims")

# COMMAND ----------

# MAGIC %md ## Exercise solution — loss ratio per county

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.county,
# MAGIC        count(DISTINCT p.policy_id)                          AS policies,
# MAGIC        round(sum(p.annual_premium), 0)                      AS total_premium,
# MAGIC        round(sum(c.incurred_amount), 0)                     AS total_incurred,
# MAGIC        round(sum(c.incurred_amount) / sum(p.annual_premium), 3) AS loss_ratio
# MAGIC FROM policies p
# MAGIC LEFT JOIN claims c
# MAGIC   ON p.policy_id = c.policy_id
# MAGIC  AND c.incurred_amount >= 0          -- exclude the -1 legacy placeholders
# MAGIC GROUP BY p.county
# MAGIC ORDER BY loss_ratio DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Teaching notes:
# MAGIC - The raw file still contains duplicate policy rows — `count(DISTINCT policy_id)`
# MAGIC   sidesteps them here, but the premium sum is still slightly inflated by dupes.
# MAGIC   That imperfection is deliberate: it motivates Lab 2 (clean once, in one place,
# MAGIC   instead of every analyst defensively deduping in every query).
# MAGIC - Expect Dublin / Kildare / Meath near the top on loss ratio.
