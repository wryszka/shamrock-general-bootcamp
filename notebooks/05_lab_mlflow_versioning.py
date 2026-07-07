# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4 — MLflow versioning: retiring the R-script upload (75 min)
# MAGIC
# MAGIC **What is this for?** Today a risk model is an R script someone emails to IT.
# MAGIC Nobody can say which version priced last quarter's quotes, comparing two candidate
# MAGIC models is a manual diff, and rolling back a bad model is a week of meetings.
# MAGIC
# MAGIC After this lab, a model is a **registered, versioned, governed asset** in Unity
# MAGIC Catalog. The four beats — and what each is for:
# MAGIC
# MAGIC | Beat | What is this for? |
# MAGIC |---|---|
# MAGIC | Compare versions | "which model is better? prove it" |
# MAGIC | Load an old version | "reproduce last quarter's prices for the auditor" |
# MAGIC | Alias swap | "release without redeploying anything" |
# MAGIC | Rollback | "bad model live? fixed in 30 seconds" |
# MAGIC
# MAGIC The model itself is deliberately simple (sklearn gradient boosting, tabular
# MAGIC features) — the point is the *workflow*, not the maths.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import pandas as pd

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()
print(f"Model will be registered as: {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Training data — did this policy have a claim?
# MAGIC
# MAGIC Same silver tables as everything else today. Label = policy had a claim.

# COMMAND ----------

df = spark.sql("""
    SELECT p.driver_age, p.ncd_years, p.penalty_points, p.engine_cc,
           p.county, p.cover_type,
           CASE WHEN c.policy_id IS NULL THEN 0 ELSE 1 END AS had_claim
    FROM 2_silver_policies p
    LEFT JOIN (SELECT DISTINCT policy_id FROM 2_silver_claims) c USING (policy_id)
""").toPandas()

# one-hot the categoricals; keep the full column list for reuse at inference time
X_all = pd.get_dummies(df.drop(columns="had_claim"), columns=["county", "cover_type"], dtype=int)
y = df["had_claim"]
FEATURE_COLS = list(X_all.columns)

X_train, X_test, y_train, y_test = train_test_split(X_all, y, test_size=0.25, random_state=42)
print(f"{len(X_train):,} train / {len(X_test):,} test — claim rate {y.mean():.1%}")

# COMMAND ----------

# MAGIC %md ## 2. Train **v1** — four basic features, log and register

# COMMAND ----------

V1_FEATURES = [c for c in FEATURE_COLS if c.startswith(("driver_age", "ncd_years", "penalty_points", "county_"))]

with mlflow.start_run(run_name="risk_model_v1") as run:
    model = GradientBoostingClassifier(n_estimators=80, max_depth=3, random_state=42)
    model.fit(X_train[V1_FEATURES], y_train)
    auc = roc_auc_score(y_test, model.predict_proba(X_test[V1_FEATURES])[:, 1])

    mlflow.log_params({"features": "age, ncd, penalty_points, county", "n_estimators": 80})
    mlflow.log_metric("auc", auc)
    info = mlflow.sklearn.log_model(
        model, name="model",
        input_example=X_train[V1_FEATURES].head(3),
    )

# Registering the logged model creates a new VERSION in Unity Catalog.
# On a fresh schema this is version 1 — we capture it so reruns also work.
V1 = mlflow.register_model(info.model_uri, MODEL_NAME).version
print(f"v1 AUC: {auc:.3f} — registered as version {V1}")

# COMMAND ----------

# Point the 'champion' alias at v1 — downstream code will only ever ask for @champion
client.set_registered_model_alias(MODEL_NAME, "champion", V1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ✏️ EXERCISE — train **v2** with more signal
# MAGIC
# MAGIC Copy the v1 training cell and change three things:
# MAGIC - use **all** features (`FEATURE_COLS` — adds engine size and cover type)
# MAGIC - `run_name="risk_model_v2"` and log `"features": "all"`
# MAGIC - capture the new version as `V2` instead of `V1`
# MAGIC
# MAGIC Registering to the same `MODEL_NAME` automatically creates the **next version** —
# MAGIC that's the versioning happening, no extra ceremony.

# COMMAND ----------

# TODO: train and register v2 here, ending with:
# V2 = mlflow.register_model(info.model_uri, MODEL_NAME).version

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Beat 1 — compare versions: *which model is better? prove it*
# MAGIC
# MAGIC Also look in the UI: **Experiments** → select both runs → **Compare**.

# COMMAND ----------

runs = mlflow.search_runs(order_by=["start_time DESC"], max_results=5)
display(runs[["tags.mlflow.runName", "metrics.auc", "params.features", "start_time"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Beat 2 — inference from a **previous** version: *the auditor question*
# MAGIC
# MAGIC *"What would these five drivers have been charged under the old model?"*
# MAGIC Load **version 1 specifically** — not latest, not champion — and score.

# COMMAND ----------

sample_drivers = X_test.head(5)

v1 = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/{V1}")   # <- pinned version number
v1_scores = v1.predict(sample_drivers[V1_FEATURES])

# TODO (after your v2 exists): load version 2 the same way and compare per-driver scores
comparison = sample_drivers[["driver_age", "penalty_points", "ncd_years"]].copy()
comparison["v1_claim_prob"] = v1_scores
display(comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Beat 3 — the alias swap: *release without redeploying*
# MAGIC
# MAGIC The scoring cell below only ever asks for `@champion`. Watch it change behaviour
# MAGIC when we repoint the alias — **zero code changes downstream**.

# COMMAND ----------

def score_renewal_book():
    """This is 'production'. Note: it doesn't know or care which version is champion."""
    champion = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion")
    version = client.get_model_version_by_alias(MODEL_NAME, "champion").version
    print(f"scored with champion = version {version}")
    return champion, version

_, v = score_renewal_book()

# COMMAND ----------

# ✏️ EXERCISE: promote your v2, then rerun the cell above. Then ROLL BACK to V1 and rerun again.
client.set_registered_model_alias(MODEL_NAME, "champion", V2)   # promote
# client.set_registered_model_alias(MODEL_NAME, "champion", V1) # rollback — beat 4

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Put it to work — score the renewal book
# MAGIC
# MAGIC Business framing: renewals with high predicted claim risk **and** a big premium
# MAGIC hike are the ones to review before they lapse or bind badly.

# COMMAND ----------

renewals = spark.sql("""
    SELECT r.*, p.driver_age, p.ncd_years, p.penalty_points, p.engine_cc, p.county, p.cover_type
    FROM 2_silver_renewals r JOIN 2_silver_policies p USING (policy_id)
""").toPandas()

feats = pd.get_dummies(renewals[["driver_age", "ncd_years", "penalty_points", "engine_cc", "county", "cover_type"]],
                       columns=["county", "cover_type"], dtype=int).reindex(columns=FEATURE_COLS, fill_value=0)

champion = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion")
used_cols = [c.name for c in champion.metadata.get_input_schema().inputs]
renewals["claim_risk"] = champion.predict(feats[used_cols])

spark.createDataFrame(renewals).write.mode("overwrite").saveAsTable("4_scored_renewals")
display(spark.sql("SELECT * FROM 4_scored_renewals ORDER BY claim_risk DESC LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Checkpoint:** `4_scored_renewals` exists; you promoted v2 and rolled back to v1
# MAGIC without touching the scoring code.
# MAGIC
# MAGIC **Where this goes next (instructor demo):** the same registered model behind a
# MAGIC **serving endpoint** — real-time claim risk at quote time, straight into the
# MAGIC quote funnel. And because the model lives in Unity Catalog, access to it is
# MAGIC governed exactly like access to a table.
# MAGIC
# MAGIC **Discussion:** in your current process, how long does each of the four beats take?
