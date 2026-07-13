# Databricks notebook source
# MAGIC %md
# MAGIC # R on Databricks — your script runs as-is
# MAGIC
# MAGIC **The point of this demo:** the R you write today — data frames, GLMs, plots —
# MAGIC runs unchanged in a Databricks notebook on a classic cluster. What changes is
# MAGIC *where the data comes from* (governed tables instead of CSV extracts) and
# MAGIC *what happens after* (results land somewhere shared, versioned and audited).
# MAGIC
# MAGIC > Needs a **classic cluster** (any Databricks Runtime — R ships with it).
# MAGIC > Serverless notebooks are Python/SQL only today, so R teams share a small
# MAGIC > classic cluster, defined once by a compute policy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · This is your existing script — unchanged
# MAGIC A tiny motor frequency model, exactly the shape of code a pricing team has today.

# COMMAND ----------

# Plain R: build a small synthetic motor portfolio
set.seed(42)
n <- 5000
portfolio <- data.frame(
  driver_age     = pmin(pmax(round(rnorm(n, 42, 13)), 18), 85),
  penalty_points = rpois(n, 0.6),
  engine_cc      = sample(c(1000, 1200, 1400, 1600, 2000), n, replace = TRUE),
  exposure       = runif(n, 0.3, 1.0)
)
# claim frequency rises for young drivers, points and big engines
lambda <- with(portfolio,
  exposure * exp(-3.2 + 0.035 * (30 - pmin(driver_age, 30)) +
                 0.28 * penalty_points + 0.00025 * (engine_cc - 1000)))
portfolio$claim_count <- rpois(n, lambda)

head(portfolio)

# COMMAND ----------

# The GLM you already know — Poisson frequency with an exposure offset
freq_model <- glm(claim_count ~ driver_age + penalty_points + engine_cc,
                  offset = log(exposure),
                  family = poisson(link = "log"),
                  data   = portfolio)

summary(freq_model)

# COMMAND ----------

# Base-R plotting works too (ggplot2 is pre-installed if you prefer it)
age_bands <- cut(portfolio$driver_age, breaks = c(17, 25, 35, 50, 65, 86))
obs_freq  <- tapply(portfolio$claim_count, age_bands, sum) /
             tapply(portfolio$exposure,    age_bands, sum)
barplot(obs_freq,
        main = "Observed claim frequency by age band",
        ylab = "Claims per unit exposure", col = "steelblue")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Reaching governed data from R
# MAGIC The real change: instead of `read.csv("extract_final_v3.csv")`, read the
# MAGIC governed table directly. Same permissions, lineage and audit as everyone else —
# MAGIC R is not a side door.

# COMMAND ----------

library(sparklyr)
sc <- spark_connect(method = "databricks")

# read a Unity Catalog table (the samples catalog exists in every workspace)
trips <- dplyr::tbl(sc, dbplyr::in_catalog("samples", "nyctaxi", "trips"))

# dplyr verbs run IN Spark — nothing is downloaded until you collect()
library(dplyr)
trips %>%
  group_by(pickup_zip) %>%
  summarise(trips = n(), avg_fare = mean(fare_amount, na.rm = TRUE)) %>%
  arrange(desc(trips)) %>%
  head(10) %>%
  collect()

# COMMAND ----------

# Prefer SQL? Same connection, standard DBI
library(DBI)
dbGetQuery(sc, "
  SELECT date_trunc('MONTH', tpep_pickup_datetime) AS month,
         COUNT(*)                                  AS trips,
         ROUND(AVG(fare_amount), 2)                AS avg_fare
  FROM samples.nyctaxi.trips
  GROUP BY 1 ORDER BY 1 LIMIT 6")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Translating your existing R script — the four steps
# MAGIC
# MAGIC 1. **Paste it in.** Base R and CRAN packages work as-is on a classic cluster —
# MAGIC    `install.packages()` in the notebook, or pin team libraries to the cluster
# MAGIC    so every session starts ready.
# MAGIC 2. **Swap the file reads.** `read.csv("extract.csv")` becomes
# MAGIC    `tbl(sc, in_catalog("catalog","schema","table")) %>% collect()` — or
# MAGIC    `dbGetQuery(sc, "SELECT ...")` if you think in SQL. No more waiting for
# MAGIC    someone to email you an extract.
# MAGIC 3. **Write results back.** `copy_to()` / `spark_write_table()` puts model
# MAGIC    output into a governed table — shared, permissioned, with lineage —
# MAGIC    instead of a workbook on a shared drive.
# MAGIC 4. **Productionize when ready.** Keep heavy joins/aggregations in Spark via
# MAGIC    dplyr (skip the `collect()`), schedule this notebook as a **Job**
# MAGIC    (daily/weekly/monthly), and log models with **MLflow's R API** so every
# MAGIC    version lives in Unity Catalog next to the Python models.
# MAGIC
# MAGIC **Also good to know**
# MAGIC - **RStudio Desktop** connects to the same tables via ODBC / Databricks
# MAGIC   Connect — keep your IDE, lose the extracts.
# MAGIC - **Shiny** apps deploy on **Databricks Apps** — next to the data, no
# MAGIC   separate server.
# MAGIC - Serverless is Python/SQL only today → the pattern for an R team is one
# MAGIC   small shared classic cluster, created from a compute policy.
# MAGIC
# MAGIC ---
# MAGIC *About this demo: the portfolio above is synthetic and generated in-notebook;
# MAGIC the taxi data is the public Databricks samples catalog.*
