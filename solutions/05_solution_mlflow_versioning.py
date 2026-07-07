# Databricks notebook source
# MAGIC %md
# MAGIC # SOLUTION — Lab 4: MLflow versioning
# MAGIC
# MAGIC Instructor version: all four beats end-to-end, exercises completed.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "shamrock_bootcamp", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
MODEL_NAME = f"{CATALOG}.{SCHEMA}.shamrock_risk_model"
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import pandas as pd

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

# COMMAND ----------

# Training data
df = spark.sql("""
    SELECT p.driver_age, p.ncd_years, p.penalty_points, p.engine_cc,
           p.county, p.cover_type,
           CASE WHEN c.policy_id IS NULL THEN 0 ELSE 1 END AS had_claim
    FROM 2_silver_policies p
    LEFT JOIN (SELECT DISTINCT policy_id FROM 2_silver_claims) c USING (policy_id)
""").toPandas()

X_all = pd.get_dummies(df.drop(columns="had_claim"), columns=["county", "cover_type"], dtype=int)
y = df["had_claim"]
FEATURE_COLS = list(X_all.columns)
X_train, X_test, y_train, y_test = train_test_split(X_all, y, test_size=0.25, random_state=42)

# COMMAND ----------

def train_and_register(run_name, features, feature_desc):
    with mlflow.start_run(run_name=run_name):
        model = GradientBoostingClassifier(n_estimators=80, max_depth=3, random_state=42)
        model.fit(X_train[features], y_train)
        auc = roc_auc_score(y_test, model.predict_proba(X_test[features])[:, 1])
        mlflow.log_params({"features": feature_desc, "n_estimators": 80})
        mlflow.log_metric("auc", auc)
        info = mlflow.sklearn.log_model(model, name="model", input_example=X_train[features].head(3))
    version = mlflow.register_model(info.model_uri, MODEL_NAME).version
    print(f"{run_name}: AUC {auc:.3f} -> version {version}")
    return version, auc

V1_FEATURES = [c for c in FEATURE_COLS if c.startswith(("driver_age", "ncd_years", "penalty_points", "county_"))]

V1, auc1 = train_and_register("risk_model_v1", V1_FEATURES, "age, ncd, penalty_points, county")
client.set_registered_model_alias(MODEL_NAME, "champion", V1)

# EXERCISE SOLUTION — v2 with all features
V2, auc2 = train_and_register("risk_model_v2", FEATURE_COLS, "all")

# COMMAND ----------

# Beat 1 — compare versions
runs = mlflow.search_runs(order_by=["start_time DESC"], max_results=5)
display(runs[["tags.mlflow.runName", "metrics.auc", "params.features"]])
assert auc2 >= auc1 - 0.02, "v2 should be at least comparable to v1"

# COMMAND ----------

# Beat 2 — inference from a pinned previous version (the auditor question)
sample_drivers = X_test.head(5)
v1_model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/{V1}")
v2_model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/{V2}")

comparison = sample_drivers[["driver_age", "penalty_points", "ncd_years"]].copy()
comparison["v1_claim_prob"] = v1_model.predict(sample_drivers[V1_FEATURES])
comparison["v2_claim_prob"] = v2_model.predict(sample_drivers[FEATURE_COLS])
display(comparison)

# COMMAND ----------

# Beats 3 + 4 — alias swap and rollback; 'production' only ever asks for @champion
def champion_version():
    return client.get_model_version_by_alias(MODEL_NAME, "champion").version

client.set_registered_model_alias(MODEL_NAME, "champion", V2)   # promote
assert str(champion_version()) == str(V2)
print(f"promoted: champion -> v{V2}")

client.set_registered_model_alias(MODEL_NAME, "champion", V1)   # rollback
assert str(champion_version()) == str(V1)
print(f"rolled back: champion -> v{V1}")

client.set_registered_model_alias(MODEL_NAME, "champion", V2)   # end state: v2 live

# COMMAND ----------

# Score the renewal book with whatever is champion
renewals = spark.sql("""
    SELECT r.*, p.driver_age, p.ncd_years, p.penalty_points, p.engine_cc, p.county, p.cover_type
    FROM 2_silver_renewals r JOIN 2_silver_policies p USING (policy_id)
""").toPandas()

feats = pd.get_dummies(
    renewals[["driver_age", "ncd_years", "penalty_points", "engine_cc", "county", "cover_type"]],
    columns=["county", "cover_type"], dtype=int,
).reindex(columns=FEATURE_COLS, fill_value=0)

champion = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion")
used_cols = [c.name for c in champion.metadata.get_input_schema().inputs]
renewals["claim_risk"] = champion.predict(feats[used_cols])

spark.createDataFrame(renewals).write.mode("overwrite").saveAsTable("4_scored_renewals")
display(spark.sql("SELECT * FROM 4_scored_renewals ORDER BY claim_risk DESC LIMIT 10"))
print("Lab 4 solution complete")
