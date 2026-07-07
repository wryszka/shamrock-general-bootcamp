# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Config
# MAGIC
# MAGIC **About this demo:** all data is synthetic; Shamrock General is a fictional insurer.
# MAGIC
# MAGIC Set the catalog and schema once here. Every other notebook runs this via `%run ./00_config`.
# MAGIC
# MAGIC | Environment | catalog |
# MAGIC |---|---|
# MAGIC | Databricks Free Edition | `main` |
# MAGIC | Customer / demo workspace | any catalog you can create schemas in |

# COMMAND ----------

# Each attendee works in their OWN schema (tables, jobs and registered model versions
# don't collide across 20+ people). Default derives from your username — leave it alone
# unless told otherwise.
_user = spark.sql("SELECT current_user()").first()[0]
_user_slug = _user.split("@")[0].replace(".", "_").replace("-", "_")

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", f"shamrock_{_user_slug}", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"
RAW_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

MODEL_NAME = f"{FQ}.shamrock_risk_model"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.raw")
spark.sql(f"USE {FQ}")

print(f"Using {FQ} — raw files in {RAW_VOLUME}")
