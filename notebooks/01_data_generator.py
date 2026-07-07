# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Data generator: the Shamrock General motor book
# MAGIC
# MAGIC Generates a synthetic Irish motor portfolio with **real signal** (young drivers,
# MAGIC penalty points, big engines and Dublin genuinely drive claims and premium), then writes:
# MAGIC
# MAGIC - **Messy CSVs** (`policies.csv`, `claims.csv`) into the `raw` volume — Lab 2 cleans these
# MAGIC - **Clean silver tables** for quotes and renewals (they "arrive from other systems")
# MAGIC - Optionally (`build_all = yes`) the silver + gold tables Lab 2 would build —
# MAGIC   use this for the half-day / instructor-led run, or if a team falls behind
# MAGIC
# MAGIC Idempotent and seeded: rerunning rebuilds identical data. No libraries needed.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.dropdown("build_all", "no", ["no", "yes"], "Also build silver/gold (skip Lab 2)")
BUILD_ALL = dbutils.widgets.get("build_all") == "yes"

N_POLICIES = 50_000
N_QUOTES = 120_000
SEED = 42

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

counties = [  # (county, weight, risk_factor)
    ("Dublin", 0.30, 1.25), ("Cork", 0.12, 1.05), ("Galway", 0.07, 1.00),
    ("Kildare", 0.06, 1.10), ("Limerick", 0.05, 1.05), ("Meath", 0.05, 1.08),
    ("Tipperary", 0.04, 0.95), ("Donegal", 0.04, 0.90), ("Wexford", 0.04, 0.95),
    ("Kerry", 0.04, 0.90), ("Wicklow", 0.04, 1.05), ("Louth", 0.03, 1.00),
    ("Mayo", 0.03, 0.90), ("Clare", 0.03, 0.95), ("Waterford", 0.03, 0.95),
    ("Kilkenny", 0.03, 0.95),
]
makes = ["Toyota", "Volkswagen", "Hyundai", "Ford", "Skoda", "Kia", "Nissan", "BMW", "Audi", "Dacia"]

def weighted_case(col, items):
    """Build a CASE expression picking from (value, weight, ...) tuples using a uniform column."""
    expr, cum = "CASE", 0.0
    for val, w, *_ in items:
        cum += w
        expr += f" WHEN {col} < {cum:.4f} THEN '{val}'"
    return expr + f" ELSE '{items[-1][0]}' END"

county_risk = "CASE " + " ".join(f"WHEN county = '{c}' THEN {r}" for c, w, r in counties) + " ELSE 1.0 END"

# COMMAND ----------

# MAGIC %md ## Policies — premium and claim probability share the same drivers

# COMMAND ----------

pol = (
    spark.range(N_POLICIES)
    .withColumn("policy_id", F.format_string("POL-%07d", F.col("id")))
    .withColumn("u1", F.rand(SEED)).withColumn("u2", F.rand(SEED + 1))
    .withColumn("u3", F.rand(SEED + 2)).withColumn("u4", F.rand(SEED + 3))
    .withColumn("u5", F.rand(SEED + 4)).withColumn("u6", F.rand(SEED + 5))
    .withColumn("county", F.expr(weighted_case("u1", counties)))
    .withColumn("driver_age", (18 + F.pow(F.col("u2"), 0.9) * 60).cast("int"))
    .withColumn("ncd_years", F.least(F.floor(F.col("u3") * 10), F.lit(9)).cast("int"))
    .withColumn("penalty_points", F.floor(F.pow(F.col("u4"), 3) * 9).cast("int"))
    .withColumn("vehicle_make", F.expr(f"element_at(array({','.join(repr(m) for m in makes)}), cast(u5 * {len(makes)} as int) + 1)"))
    .withColumn("engine_cc", (1000 + F.floor(F.col("u6") * 15) * 100).cast("int"))
    .withColumn("cover_type", F.expr("CASE WHEN u6 < 0.70 THEN 'comprehensive' WHEN u6 < 0.90 THEN 'tpft' ELSE 'tpo' END"))
    .withColumn("start_date", F.date_sub(F.current_date(), (F.rand(SEED + 6) * 730).cast("int")))
    .withColumn("age_factor", F.expr("CASE WHEN driver_age < 25 THEN 1.8 WHEN driver_age < 30 THEN 1.3 WHEN driver_age >= 70 THEN 1.2 ELSE 1.0 END"))
    .withColumn("county_factor", F.expr(county_risk))
    .withColumn(
        "annual_premium",
        F.round(
            F.lit(380) * F.col("age_factor") * F.col("county_factor")
            * (1 + F.col("penalty_points") * 0.05)
            * (1 - F.least(F.col("ncd_years"), F.lit(5)) * 0.05)
            * (1 + (F.col("engine_cc") - 1000) / 8000)
            * F.expr("CASE cover_type WHEN 'comprehensive' THEN 1.0 WHEN 'tpft' THEN 0.85 ELSE 0.75 END")
            * (0.9 + F.rand(SEED + 7) * 0.2),
            2,
        ),
    )
    .withColumn(
        "claim_prob",
        F.lit(0.045) * F.col("age_factor") * F.col("county_factor")
        * (1 + F.col("penalty_points") * 0.12)
        * (1 - F.least(F.col("ncd_years"), F.lit(5)) * 0.04)
        * (1 + (F.col("engine_cc") - 1000) / 6000),
    )
    .withColumn("has_claim", F.rand(SEED + 8) < F.col("claim_prob"))
    .select(
        "policy_id", "county", "driver_age", "ncd_years", "penalty_points",
        "vehicle_make", "engine_cc", "cover_type", "start_date", "annual_premium",
        "has_claim",
    )
)  # note: no .cache() — not supported on serverless; seeded rand() recomputes identically
print(f"policies: {pol.count():,}  |  claim frequency: {pol.filter('has_claim').count() / N_POLICIES:.1%}")

# COMMAND ----------

# MAGIC %md ## Claims — type mix and severities that look like a real motor book

# COMMAND ----------

claim_types = [("windscreen", 0.40), ("damage", 0.35), ("theft", 0.10), ("injury", 0.15)]

claims = (
    pol.filter("has_claim")
    .withColumn("u", F.rand(SEED + 9))
    .withColumn("claim_seq", F.row_number().over(Window.orderBy("policy_id")))
    .withColumn("claim_id", F.format_string("CLM-%07d", F.col("claim_seq")))
    .withColumn("claim_type", F.expr(weighted_case("u", [(t, w) for t, w in claim_types])))
    .withColumn("claim_date", F.date_add(F.col("start_date"), (F.rand(SEED + 10) * 330).cast("int") + 15))
    .withColumn("sev_u", F.rand(SEED + 11))
    .withColumn(
        "incurred_amount",
        F.round(
            F.expr("""
                CASE claim_type
                    WHEN 'windscreen' THEN 250 + sev_u * 550
                    WHEN 'damage'     THEN 800 + pow(sev_u, 2) * 9000
                    WHEN 'theft'      THEN 3000 + sev_u * 15000
                    WHEN 'injury'     THEN 8000 + pow(sev_u, 2) * 90000
                END
            """),
            2,
        ),
    )
    .withColumn("status", F.expr("CASE WHEN sev_u < 0.75 THEN 'settled' WHEN sev_u < 0.92 THEN 'open' ELSE 'in_review' END"))
    .select("claim_id", "policy_id", "claim_type", "claim_date", "incurred_amount", "status")
)
print(f"claims: {claims.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Messy raw CSVs for Lab 2
# MAGIC
# MAGIC Planted problems the pipeline lab must handle:
# MAGIC - ~1.5% duplicated policy rows
# MAGIC - ~1% of `start_date` values corrupted to `31/02/2025`
# MAGIC - ~0.5% negative `annual_premium`
# MAGIC - claims with `incurred_amount` set to `-1` (unset placeholder from the "legacy system")

# COMMAND ----------

pol_raw = pol.drop("has_claim")

dupes = pol_raw.sample(fraction=0.015, seed=SEED)
pol_messy = (
    pol_raw.unionAll(dupes)
    .withColumn("corrupt", F.rand(SEED + 12))
    .withColumn("start_date", F.expr("CASE WHEN corrupt < 0.01 THEN '31/02/2025' ELSE cast(start_date as string) END"))
    .withColumn("annual_premium", F.expr("CASE WHEN corrupt > 0.995 THEN -annual_premium ELSE annual_premium END"))
    .drop("corrupt")
)

claims_messy = claims.withColumn(
    "incurred_amount", F.expr(f"CASE WHEN rand({SEED + 13}) < 0.01 THEN -1 ELSE incurred_amount END")
)

pol_messy.toPandas().to_csv(f"{RAW_VOLUME}/policies.csv", index=False)
claims_messy.toPandas().to_csv(f"{RAW_VOLUME}/claims.csv", index=False)
print(f"wrote messy CSVs to {RAW_VOLUME}")

# COMMAND ----------

# MAGIC %md ## Quotes and renewals — clean silver tables "from other systems"

# COMMAND ----------

quotes = (
    spark.range(N_QUOTES)
    .withColumn("quote_id", F.format_string("QTE-%07d", F.col("id")))
    .withColumn("u1", F.rand(SEED + 20)).withColumn("u2", F.rand(SEED + 21))
    .withColumn("quote_date", F.date_sub(F.current_date(), (F.col("u1") * 365).cast("int")))
    .withColumn("channel", F.expr("CASE WHEN u2 < 0.45 THEN 'web' WHEN u2 < 0.70 THEN 'broker' WHEN u2 < 0.90 THEN 'phone' ELSE 'app' END"))
    .withColumn("driver_age", (18 + F.pow(F.rand(SEED + 22), 0.9) * 60).cast("int"))
    .withColumn("county", F.expr(weighted_case(f"rand({SEED + 23})", counties)))
    .withColumn("quoted_premium", F.round(300 + F.rand(SEED + 24) * 1400, 2))
    # cheaper quotes and web/app channels convert better
    .withColumn(
        "converted",
        F.rand(SEED + 25)
        < (0.28 - (F.col("quoted_premium") - 300) / 1400 * 0.18
           + F.expr("CASE WHEN channel IN ('web','app') THEN 0.04 ELSE 0 END")),
    )
    .select("quote_id", "quote_date", "channel", "driver_age", "county", "quoted_premium", "converted")
)
quotes.write.mode("overwrite").saveAsTable("2_silver_quotes")

renewals = (
    pol.sample(fraction=0.6, seed=SEED)
    .withColumn("renewal_date", F.date_add(F.col("start_date"), 365))
    .withColumn("premium_change_pct", F.round((F.rand(SEED + 30) - 0.35) * 30, 1))
    .withColumn("new_premium", F.round(F.col("annual_premium") * (1 + F.col("premium_change_pct") / 100), 2))
    # bigger premium hikes and a recent claim make lapse more likely
    .withColumn(
        "renewed",
        F.rand(SEED + 31)
        < (F.lit(0.90) - F.greatest(F.col("premium_change_pct"), F.lit(0)) * 0.015
           - F.expr("CASE WHEN has_claim THEN 0.05 ELSE 0 END")),
    )
    .select("policy_id", "renewal_date", "annual_premium", "premium_change_pct", "new_premium", "renewed")
)
renewals.write.mode("overwrite").saveAsTable("2_silver_renewals")

print(f"2_silver_quotes: {quotes.count():,} | 2_silver_renewals: {renewals.count():,}")

# COMMAND ----------

# MAGIC %md ## Optional: build everything Lab 2 would build (half-day / catch-up mode)

# COMMAND ----------

if BUILD_ALL:
    dbutils.notebook.run("../solutions/03_solution_nightly_pipeline", 600,
                         {"catalog": CATALOG, "schema": SCHEMA})
    print("silver + gold tables built via Lab 2 solution")
else:
    print("build_all = no — Lab 2 attendees will build silver/gold themselves")

# COMMAND ----------

# MAGIC %md ✅ **Done.** Data is ready — start with `02_lab_meet_your_book`.
