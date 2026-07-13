# Shamrock General Bootcamp

`wryszka/shamrock-general-bootcamp` — a hands-on Databricks bootcamp built around a synthetic Irish motor insurance book
("Shamrock General", a fictional insurer). One dataset runs through the whole day:
raw extracts → pipeline → dashboards & Genie → ML with MLflow versioning → AI on documents.

Designed for a mixed audience (data scientists + analysts) who *say* they are advanced.
Every lab starts from a working example and has explicit **"what is this for"** framing.

> **About this demo**
> All data is synthetic and generated on the fly. Shamrock General is a fictional
> company. Nothing here is real customer, policy, or claims data.

## Portability

- Plain notebooks only — no DLT, no Asset Bundles, no pip installs, no external data.
- Runs on any workspace including **Databricks Free Edition** (use catalog `main`).
- **Serverless environment: pick the latest version** (v5 at time of writing — check the
  environment panel for newer). v5+ ships mlflow and scikit-learn; older versions (≤4)
  don't, and Lab 4 will fail on them with `No module named 'mlflow'`. Same applies when
  scheduling the Lab 2 job or running these notebooks headless.
- All catalog/schema references flow from widgets in `notebooks/00_config.py`.
- Serving endpoints are optional (instructor demo); all lab inference uses
  `mlflow.pyfunc.load_model` in the notebook.

Quick start: import the repo (Workspace → Git folder), open `00_config.py`, set your
catalog/schema, run `01_data_generator.py`, then follow the labs in order.

## Data model (single schema, numbered tables)

| Asset | Contents |
|---|---|
| Volume `raw/` | `policies.csv`, `claims.csv` — deliberately messy (dupes, bad dates, negative premiums) |
| `1_bronze_policies`, `1_bronze_claims` | built in Lab 2 — raw ingest + metadata |
| `2_silver_policies`, `2_silver_claims` | cleaned, typed, deduped (Lab 2 rebuilds these) |
| `2_silver_quotes` | quote funnel: channel, quoted premium, converted flag |
| `2_silver_renewals` | renewal offers: premium change, renewed/lapsed |
| `3_gold_portfolio_summary` | GWP, exposure, claim frequency by county / age band |
| `3_gold_loss_ratio_monthly` | incurred / earned premium by county and month |
| `4_scored_renewals` | Lab 4 output — renewal book scored by the champion model |

The synthetic data has real signal: young drivers, penalty points, big engines and
Dublin postcodes all genuinely drive claim probability and premium, so models find
real patterns and Genie answers make business sense.

## Agenda

### Full day (theory morning, one self-paced build block in the afternoon)

| Time | Session | Notebook |
|---|---|---|
| 10:00 | **Presentation: the big picture** + live Pricing Workbench demo | — |
| 11:00 | **Presentation: the tech tour** — models & R · data in/out · governance & cost | — |
| 12:00 | Lunch | |
| 13:00 | Setup + the roadmap: all labs briefed once | `00_config`, `01_data_generator` |
| 13:20 | **Self-paced build — Labs 1–4** in order, instructors floating | `02`–`05` |
| — | *Lab 5 — AI on claim documents runs as a mini-demo on request during the block* | `06_demo_ai_functions_fnol` |
| 14:30 | Coffee — grab and go, keep building | |
| 15:45 | Showcase, stretch-track ideas, next steps | |
| 16:00 | Close | |

### Half day (instructor presents)

08:30 kickoff → instructor demos Labs 2–5 as one continuous story on the same data
(45 min pipeline+dashboards+Genie, 45 min MLflow, 15 min AI functions) → 12:00 Q&A →
attendees take this repo home with Free Edition instructions.

## Session detail & "what is this for"

**Lab 1 — Meet your book.** Run cells, `%sql` vs Python on the same table, `display()`
charts, let the Assistant write a query. Exercise: worst loss-ratio county (one join).
*What for:* replaces the CSV-pull-into-Excel loop. Also quietly calibrates real skill.

**Lab 2 — The nightly pipeline.** Messy CSVs in the volume → bronze (as-is + metadata)
→ silver (types fixed, dupes dropped, bad rows quarantined) → two gold tables. Then
schedule it as a daily Workflows job via the UI. *What for:* the gold tables are the
single source of truth everything else in the day uses.

**Lab 3 — Dashboards + Genie.** Build an AI/BI dashboard from a recipe (KPI tiles,
county bar, monthly trend, cover filter). Then create a Genie space on the schema,
add instructions ("loss ratio = incurred/premium", "amounts are EUR") and ask real
questions — including one Genie fumbles until they add a curation instruction.
*What for:* self-service for business users; kills the "can you pull me numbers" queue.

**Lab 4 — MLflow versioning.** Simple sklearn GBM predicting claim probability — the
thing pricing does in R and uploads. Four explicit beats:
1. **Compare versions** — v1 vs v2 metrics in code + UI → "which is better? prove it"
2. **Inference from a previous version** — load `models:/…/1`, score 5 drivers →
   "reproduce last quarter's prices for the auditor"
3. **Alias swap** — repoint `@champion` to v2; downstream scoring cell needs zero code
   change → "release without redeploying"
4. **Rollback** — swap the alias back → "bad model live? fixed in 30 seconds"
Then batch-score the renewal book with `@champion`.
*What for:* this registry + alias workflow is what replaces emailing R scripts to IT.

**Lab 5 — AI on claim documents (demo).** `ai_parse_document` + `ai_extract` turn FNOL
letters into structured claim fields in SQL. *What for:* document understanding without
building an OCR stack.

## Repo layout

```
notebooks/
  00_config.py                # catalog/schema widgets — the only place names live
  01_data_generator.py        # synthetic Shamrock General book (idempotent, seeded)
  02_lab_meet_your_book.py
  03_lab_nightly_pipeline.py
  04_lab_dashboards_genie.py  # recipe + Genie question list (mostly UI-driven)
  05_lab_mlflow_versioning.py
  06_demo_ai_functions_fnol.py
solutions/                    # instructor versions with all TODOs completed
```
