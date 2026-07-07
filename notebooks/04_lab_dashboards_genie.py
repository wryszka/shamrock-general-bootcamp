# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3 — Dashboards + a Genie room (60 min)
# MAGIC
# MAGIC **What is this for?** This is the session that ends the *"can you pull me those
# MAGIC numbers?"* queue. A dashboard answers the questions you knew people would ask;
# MAGIC Genie answers the ones you didn't. Both sit on the gold tables you built in Lab 2 —
# MAGIC build once, serve everyone.
# MAGIC
# MAGIC This lab is UI-driven; this notebook is your recipe card.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A — AI/BI Dashboard (25 min)
# MAGIC
# MAGIC **New → Dashboard**, then add datasets `3_gold_portfolio_summary` and
# MAGIC `3_gold_loss_ratio_monthly` from your schema.
# MAGIC
# MAGIC Build these four widgets (use the dashboard AI assistant — type what you want):
# MAGIC
# MAGIC | # | Widget | Spec |
# MAGIC |---|---|---|
# MAGIC | 1 | KPI counters | total GWP, total policies, overall claim frequency |
# MAGIC | 2 | Bar | claim frequency by county, sorted descending |
# MAGIC | 3 | Line | monthly loss ratio trend, one line per county (top 5 by GWP) |
# MAGIC | 4 | Filter | cover_type, wired to widgets 1–2 |
# MAGIC
# MAGIC **Checkpoint:** click the filter to `comprehensive` — do the KPIs react?
# MAGIC Then hit **Publish** and open the published view: *that* link is what a claims or
# MAGIC pricing manager would get, not the canvas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B — Genie room (25 min)
# MAGIC
# MAGIC **New → Genie space** with tables: `2_silver_policies`, `2_silver_claims`,
# MAGIC `2_silver_quotes`, `2_silver_renewals`, `3_gold_portfolio_summary`.
# MAGIC
# MAGIC ### Round 1 — just ask (no setup)
# MAGIC
# MAGIC - *Which county has the highest loss ratio?*
# MAGIC - *What is the average claim cost for drivers under 25?*
# MAGIC - *How many injury claims did we have, and what did they cost in total?*
# MAGIC - *Which sales channel converts quotes best?*
# MAGIC
# MAGIC Check the generated SQL each time (click **Show code**) — is it right?
# MAGIC
# MAGIC ### Round 2 — break it, then fix it with curation
# MAGIC
# MAGIC Ask: ***"What is our retention rate?"***
# MAGIC
# MAGIC Genie has to guess what retention means — and may count rows instead of using
# MAGIC renewal outcomes. Now add **Instructions** to the space:
# MAGIC
# MAGIC ```
# MAGIC - Retention rate = share of renewal offers where renewed = true, from 2_silver_renewals.
# MAGIC - Loss ratio = total incurred_amount / total annual_premium.
# MAGIC - All monetary amounts are EUR.
# MAGIC - "Young drivers" means driver_age < 25.
# MAGIC ```
# MAGIC
# MAGIC Ask the same question again. **That difference is the whole job of Genie curation** —
# MAGIC the analyst's role shifts from writing every query to teaching Genie the business
# MAGIC definitions once.
# MAGIC
# MAGIC ### Round 3 — stress test
# MAGIC
# MAGIC Try to break it with your own real questions — the ones your stakeholders actually
# MAGIC ask. Note what works and what needs an instruction.
# MAGIC
# MAGIC **Checkpoint:** at least one question that failed in Round 1 now answers correctly.
# MAGIC
# MAGIC **Discussion:** who in your team would you give this space to on Monday — and what
# MAGIC are the first three instructions you'd write for them?
