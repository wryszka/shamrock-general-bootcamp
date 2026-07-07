# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1 — Meet your book (40 min)
# MAGIC
# MAGIC **What is this for?** Everything you'd normally do by pulling a CSV into Excel —
# MAGIC but on the full book, repeatable, and shareable. SQL and Python side by side on
# MAGIC the same data, plus the Assistant writing queries for you.
# MAGIC
# MAGIC By the end you can: run cells, mix `%sql` and Python, chart with `display()`,
# MAGIC and use the Assistant.
# MAGIC
# MAGIC > Uses the **raw CSVs** so this lab works before the pipeline lab. From Lab 2
# MAGIC > onwards we use proper tables.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md ## 1. Read the policy book (Python)

# COMMAND ----------

policies = spark.read.csv(f"{RAW_VOLUME}/policies.csv", header=True, inferSchema=True)
display(policies.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Same thing in SQL
# MAGIC
# MAGIC A temp view makes the DataFrame queryable from `%sql` cells. Use whichever
# MAGIC language fits the task — they see the same data.

# COMMAND ----------

policies.createOrReplaceTempView("policies")
claims = spark.read.csv(f"{RAW_VOLUME}/claims.csv", header=True, inferSchema=True)
claims.createOrReplaceTempView("claims")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How is the book distributed? Click the chart icon under the result and
# MAGIC -- switch to a bar chart: county on X, avg premium on Y.
# MAGIC SELECT county,
# MAGIC        count(*)                 AS policies,
# MAGIC        round(avg(annual_premium), 0) AS avg_premium
# MAGIC FROM policies
# MAGIC GROUP BY county
# MAGIC ORDER BY policies DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Let the Assistant work for you
# MAGIC
# MAGIC Open the **Assistant** (sparkle icon, top right) and ask:
# MAGIC
# MAGIC > *"Using the policies view, show average annual premium by cover_type and
# MAGIC > driver age band (under 25, 25-40, 40-60, over 60)"*
# MAGIC
# MAGIC Paste the result into the next cell and run it. If it's not quite right —
# MAGIC tell the Assistant what to fix instead of fixing it by hand.

# COMMAND ----------

# Paste the Assistant's query here (as %sql or Python — your choice)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ✏️ EXERCISE — which county is losing us money?
# MAGIC
# MAGIC Compute the **loss ratio per county**: total claims incurred ÷ total premium.
# MAGIC Which county is worst, and is it the one with the highest premiums?
# MAGIC
# MAGIC Hints:
# MAGIC - Join `claims` to `policies` on `policy_id` (claims don't have a county)
# MAGIC - Watch out: some `incurred_amount` values are `-1` (legacy placeholder) — exclude them
# MAGIC - `sum(incurred_amount) / sum(annual_premium)` per county

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: your query here
# MAGIC SELECT ...

# COMMAND ----------

# MAGIC %md
# MAGIC **Checkpoint:** you should see loss ratios roughly between 0.3 and 0.7, with the
# MAGIC high-risk counties (Dublin, Kildare, Meath) near the top.
# MAGIC
# MAGIC **Discussion:** those `-1` amounts and the duplicate rows you may have spotted —
# MAGIC who fixes those today, and how often? That's Lab 2.
