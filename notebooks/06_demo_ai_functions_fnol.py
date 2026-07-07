# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5 — AI on claim documents (30 min, instructor demo)
# MAGIC
# MAGIC **What is this for?** Claims arrive as letters, PDFs and scans. Turning them into
# MAGIC structured fields today means either manual keying or an OCR project. With AI
# MAGIC Functions it's SQL — same governance, same tables, no new infrastructure.
# MAGIC
# MAGIC Flow: `ai_parse_document` (file → structured content) → `ai_extract` (content →
# MAGIC your fields) → `ai_classify` (route it). We demo on synthetic FNOL
# MAGIC (first-notification-of-loss) letters.
# MAGIC
# MAGIC > **Portability note:** requires AI Functions availability on the workspace
# MAGIC > (serverless). If unavailable, this stays a talk-through with screenshots.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md ## 1. Generate a few synthetic FNOL letters (text; PDFs optional)

# COMMAND ----------

fnol_letters = [
    ("FNOL-001", """Dear Shamrock General, I am writing to report an accident involving my car,
registration 231-D-45678, policy POL-0012345. On 14 June 2026 at the Red Cow roundabout in Dublin,
another vehicle collided with my rear bumper. Nobody was injured but the boot no longer closes.
My phone number is 087 123 4567. I got an estimate from my local garage for EUR 2,340.
Yours sincerely, Aoife Murphy"""),
    ("FNOL-002", """To whom it may concern. Policy number POL-0034567. My windscreen was cracked by
a stone on the M8 near Fermoy on 2 July 2026. I would like to arrange repair through your approved
provider. The crack is about 20cm. Regards, Padraig O'Sullivan, 086 765 4321."""),
    ("FNOL-003", """URGENT - my van (policy POL-0056789) was stolen overnight from outside my house in
Galway, some time between 11pm on 30 June and 6am on 1 July 2026. Gardai have been notified, PULSE
incident number GA-2026-88421. The van contained my work tools, roughly EUR 4,000 worth.
Sean Kelly, 085 222 3344."""),
]
spark.createDataFrame(fnol_letters, ["doc_id", "content"]).write.mode("overwrite").saveAsTable("5_fnol_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `ai_extract` — pull the claim fields out
# MAGIC
# MAGIC (With real PDFs/scans in a volume you'd first run `ai_parse_document(content)` —
# MAGIC same pattern, one extra step. That is exactly the function evaluated for the
# MAGIC group Document Understanding workflow.)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE 5_fnol_extracted AS
# MAGIC SELECT
# MAGIC   doc_id,
# MAGIC   ai_extract(content,
# MAGIC     array('policy_number', 'incident_date', 'incident_location', 'contact_phone', 'estimated_cost_eur')
# MAGIC   ) AS fields,
# MAGIC   ai_classify(content, array('vehicle damage', 'windscreen', 'theft', 'injury')) AS claim_type,
# MAGIC   ai_summarize(content, 20) AS summary
# MAGIC FROM 5_fnol_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT doc_id, claim_type, fields.policy_number, fields.incident_date,
# MAGIC        fields.estimated_cost_eur, summary
# MAGIC FROM 5_fnol_extracted

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The point
# MAGIC
# MAGIC - Three letters became **queryable rows** — joinable to `2_silver_policies` on
# MAGIC   the extracted policy number, right now, in this schema.
# MAGIC - No OCR stack, no model hosting, no data leaving governance.
# MAGIC - Cost model: pay per processed page/row as an AI Functions workload — measurable
# MAGIC   in system tables per function, so a pilot on a representative sample gives you a
# MAGIC   real EUR-per-1000-documents figure.
# MAGIC
# MAGIC **Try it after today:** drop 20 real (non-sensitive) documents in a volume, run
# MAGIC `ai_parse_document` + `ai_extract`, and check the measured cost against the manual
# MAGIC keying effort it replaces.
