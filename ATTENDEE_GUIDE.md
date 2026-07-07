# Shamrock General Bootcamp — Attendee Guide

Welcome! Today you work for **Shamrock General**, a (fictional) Irish motor insurer.
You'll take one book of business — 50,000 motor policies — from raw files all the way
to dashboards, natural-language analytics, and a governed pricing model. All data is
synthetic; every problem in it is one you'll recognise.

**How this guide works:** every lab starts with *your task* (a business problem, not a
tool instruction), lists *the tools you'll use*, then gives *step-by-step* directions
with **example prompts in bold** you can paste into Genie Code / the Assistant. If the
prompts get you there, great — the AI writing code for you is part of the lesson.
Stuck? Every lab has a full solution in the `solutions/` folder.

---

## What we're learning today — at a glance

One synthetic motor book, five parts, each solving a business problem with a specific
set of Databricks tools:

| Part | The business problem | What you learn | Databricks tools |
|---|---|---|---|
| **1. Meet your book** | Which county is losing us money? | Notebooks: SQL + Python on the same data; AI-assisted analysis | Notebooks, Databricks Assistant / Genie Code |
| **2. The nightly pipeline** | Two analysts, two different numbers | Bronze → silver → gold; cleaning as a contract; scheduling | Delta tables, Workflows (scheduled jobs), Genie Code |
| **3. Self-service analytics** | The Monday report queue | Dashboards built by describing them; Genie rooms + curation | AI/BI Dashboards, Genie spaces |
| **4. Governed model lifecycle** | R scripts emailed to IT | Track experiments; version, compare, pin, alias-swap and roll back models; batch scoring | MLflow, Unity Catalog model registry |
| **5. AI on documents** *(demo)* | Humans retyping claim letters | Parse / extract / classify documents in SQL | AI Functions (`ai_extract`, `ai_classify`, `ai_parse_document`) |

Running underneath everything, all day: **Unity Catalog** (your own schema, permissions,
lineage, a governed model) and **serverless compute** (nothing to size or manage).

**Skills you take home:** query and chart data in a notebook · prompt an AI to write
working code · build and schedule a production pipeline · create a dashboard and a
curated Genie room · register, version, compare, release and roll back an ML model ·
know when SQL AI functions replace an OCR project.

---

## Start here — setup (15 min, do once)

1. Log in to the workspace (URL on screen) and open the folder shown by the
   facilitator (e.g. `Workspace → Shared → shamrock-general-bootcamp`). Working from GitHub
   instead? `New → Git folder` → `https://github.com/wryszka/shamrock-general-bootcamp`.
2. Open `notebooks/00_config` and click **Connect** (top right) → **Serverless**.
3. **Important:** open the **Environment** panel (side icon) and set the environment
   version to the **latest** (v5 or higher). Older versions are missing the ML
   libraries and Lab 4 will fail late in the day — set it now.
4. Run `00_config` top to bottom. It creates **your own schema** (named
   `shamrock_<your name>`) — you can't collide with your neighbour all day.
5. Open and run `01_data_generator` (leave `build_all = no`; takes 2–3 min).

✅ **Checkpoint:** in **Catalog** (left nav) find your schema: it has a `raw` volume
containing `policies.csv` and `claims.csv`, plus tables `2_silver_quotes` and
`2_silver_renewals`. That's your book. Everything today builds on it.

---

## Lab 1 — Where are we losing money? (40 min)

**Your task:** you've just joined Shamrock General as an analyst. The CFO is convinced
*"Dublin is the problem."* Before Friday's meeting, verify it: **which county actually
has the worst loss ratio — claims paid out per euro of premium taken in?**

**You will use:** a notebook (SQL and Python on the same data), and **Genie Code /
the Databricks Assistant** as your pair-analyst.

**Steps:**

1. Open `02_lab_meet_your_book` and run the first cells — they load the raw policy
   and claims files and register them as `policies` and `claims` views.
2. Run the ready-made SQL cell (policies and average premium by county). Click the
   **chart icon** under the result and switch it to a bar chart. You just did in one
   cell what normally takes an export and a pivot table.
3. Now let the AI do one for you. Open the Assistant/Genie Code panel and prompt:
   > **"Using the policies view, show average annual premium by cover_type and driver
   > age band (under 25, 25-39, 40-59, 60+), as a SQL query."**
   Paste the result into the empty cell and run it. Not quite right? Don't fix it by
   hand — tell it what's wrong: **"put age bands as rows and cover types as columns."**
4. The exercise: the loss-ratio question. Try building it yourself first (hints in the
   notebook), or prompt your way there:
   > **"Join the claims view to the policies view on policy_id and calculate the loss
   > ratio per county: total incurred_amount divided by total annual_premium. Exclude
   > claims with negative incurred_amount. Sort worst first."**
5. Sanity-check the answer before you trust it — ask: does the row count look right?
   Why did we exclude negative amounts? (Peek at those rows: `WHERE incurred_amount < 0`.
   They're a legacy-system placeholder — data you'll *clean properly* in Lab 2.)

✅ **Checkpoint:** loss ratios roughly between 0.3 and 0.7, high-risk counties
(Dublin, Kildare, Meath) near the top. So — was the CFO right?

🆘 Stuck: `solutions/02_solution_meet_your_book`.

---

## Lab 2 — One version of the truth, every morning (60 min)

**Your task:** your loss-ratio number and your neighbour's don't match — the raw files
contain duplicates, impossible dates and negative premiums, and each of you handled
them differently. **Build the nightly process that turns raw extracts into one trusted
set of tables, and schedule it so it runs while you sleep.**

**You will use:** a notebook (the pipeline), **Genie Code / the Assistant** to write the
cleaning code, and **Jobs & Pipelines** (via the notebook's Schedule button) to automate it.

**Steps:**

1. Open `03_lab_nightly_pipeline`. Run the **bronze** section — it lands both CSVs
   as-is, everything as text, plus audit columns. Rule of thumb: bronze answers
   *"what did the source system actually send us?"*
2. Run the **silver policies** cell and read it — this is the cleaning contract:
   dedupe, fix types, drop impossible rows. Note `try_to_date`: the file contains
   dates like `31/02/2025`, and this turns them into NULLs instead of crashing the job.
3. Your turn — **silver claims**. Build it yourself from the TODO cell, or prompt:
   > **"Read the table 1_bronze_claims, deduplicate on claim_id, convert claim_date to
   > a date using try_to_date with format yyyy-MM-dd, cast incurred_amount to double,
   > remove rows where incurred_amount is negative, drop the _ingested_at and
   > _source_file columns, and save as table 2_silver_claims."**
4. Run the first **gold** cell (portfolio summary — premium, claim frequency and
   incurred by county / age band / cover type). This is the table your dashboard and
   Genie room will sit on in Lab 3.
5. Build the second gold table (monthly loss ratio). Prompt if you like:
   > **"Create table 3_gold_loss_ratio_monthly: join 2_silver_claims to
   > 2_silver_policies on policy_id, group by county and month of claim_date, and
   > output claim count, injury claim count, total incurred, total premium of claiming
   > policies, and loss_ratio = incurred / premium."**
6. Automate it: click **Schedule** (top right) → *Add schedule* → daily at 02:00,
   serverless. Then open **Jobs & Pipelines** in the left nav, find your job and hit
   **Run now** — watch it go green.

✅ **Checkpoint:** your schema shows `1_bronze_*`, `2_silver_*` and `3_gold_*` tables,
and your job has one successful run. Compare bronze vs silver row counts — you can say
*exactly* how many bad rows the source sent. Who fixes those rows today, and how often?

🆘 Stuck: `solutions/03_solution_nightly_pipeline`.

---

## Lab 3 — Stop being the report queue (60 min)

**Your task:** your claims manager asks you the same five questions every Monday, plus
one new one you can never predict. **Give them a dashboard for the known questions —
and a Genie room where they can ask the unknown ones themselves, in plain English.**

**You will use:** **AI/BI Dashboards** (build the widgets by describing them — this is
Genie Code for dashboards) and a **Genie space** on your schema.

### Part A — the dashboard (25 min)

1. **New → Dashboard**. Add datasets: your `3_gold_portfolio_summary` and
   `3_gold_loss_ratio_monthly`.
2. Build each widget by typing what you want into the dashboard's AI box:
   > **"Counters showing total gross written premium, total policies, and overall
   > claim frequency"**
   > **"Bar chart of claim frequency by county, sorted descending"**
   > **"Line chart of monthly loss ratio over time, one line per county, top 5
   > counties by premium only"**
3. Add a **filter** widget on `cover_type` and wire it to the counters and the bar
   chart. Click it — everything should react.
4. Hit **Publish** and open the published link. *That* is what the claims manager
   gets: no canvas, no SQL, just their numbers.

✅ **Checkpoint A:** published dashboard, filter working.

### Part B — the Genie room (25 min)

5. **New → Genie space**. Add tables: `2_silver_policies`, `2_silver_claims`,
   `2_silver_quotes`, `2_silver_renewals`, `3_gold_portfolio_summary`.
6. Ask it the Monday questions — check **Show code** on each answer and judge the SQL:
   - *Which county has the highest loss ratio?*
   - *What's the average claim cost for drivers under 25?*
   - *How many injury claims did we have and what did they cost?*
   - *Which sales channel converts quotes best?*
7. Now break it. Ask: ***"What is our retention rate?"*** — Genie has to guess what
   retention means, and it will likely guess wrong. This is the important moment.
8. Fix it with curation, not code. In the space settings, add **Instructions**:
   ```
   - Retention rate = share of renewal offers where renewed = true, from 2_silver_renewals.
   - Loss ratio = total incurred_amount / total annual_premium.
   - All monetary amounts are EUR.
   - "Young drivers" means driver_age < 25.
   ```
   Ask the retention question again and compare.
9. Stress-test with the real questions *your* stakeholders ask. Each miss = one more
   instruction. That's the job: teach it the business definitions once, instead of
   writing every query forever.

✅ **Checkpoint B:** a question that failed in step 7 answers correctly now.
**Discussion:** who gets this space on Monday — and what are your first three instructions?

---

## Lab 4 — Retire the R-script upload (75 min)

**Your task:** today, putting a new pricing risk model live means emailing an R script
to IT. Nobody can prove which model priced last quarter, and rolling back a bad one
takes a week. **Ship a claim-risk model the governed way: prove the new version is
better, answer the auditor, release it, and roll it back — in minutes, without emailing
anyone.**

**You will use:** a notebook, **MLflow** (experiment tracking + the Experiments UI),
and the **Unity Catalog model registry** (versions + the `@champion` alias).

**Steps:**

1. Open `05_lab_mlflow_versioning`. Run the setup and training-data cells — the label
   is simply *"did this policy have a claim?"*, built from your own silver tables.
2. Run the **v1** cell: it trains a small model on four features, logs it to MLflow
   and registers it in Unity Catalog as `shamrock_risk_model`. Note the printed
   version number. Then run the alias cell — `@champion` now points at v1.
   Open **Catalog → your schema → Models** and look at what just appeared: your model
   is now a governed asset, like a table.
3. **Exercise — train v2.** Copy the v1 cell; use `FEATURE_COLS` (all features),
   rename the run to `risk_model_v2`, capture the version as `V2`. Or prompt:
   > **"Copy this training cell but use FEATURE_COLS as the feature list, set
   > run_name to risk_model_v2, log features=all, and store the registered version
   > number in a variable called V2."**
4. **Which model is better — prove it.** Run the comparison cell, then open
   **Experiments** (left nav), tick both runs and click **Compare**. This screenshot
   is what you'd show a pricing committee instead of "trust me".
5. **The auditor question.** Run the pinned-version cell: it loads *specifically*
   version 1 (`models:/…/{V1}`) and rescores five drivers. Extend it for v2:
   > **"Load version V2 of the model the same way and add a v2_claim_prob column to
   > the comparison dataframe."**
   Same drivers, two model generations, side by side — that's reproducibility.
6. **Release without redeploying.** Run the `score_renewal_book()` cell — note it only
   ever asks for `@champion`, it has no idea version numbers exist. Now run the
   promote line (`champion → V2`) and rerun the scoring cell. It picked up v2 with
   **zero code changes**.
7. **Rollback.** Pretend v2 misprices young drivers in production. Uncomment the
   rollback line (`champion → V1`), run it, rerun scoring. Thirty seconds, fully
   audited. Put champion back on V2 when done.
8. **Put it to work:** run the final section — it scores your whole renewal book and
   writes `4_scored_renewals`. Business read: renewals with **high claim risk and a
   big premium hike** are the ones to review before they lapse or bind badly.

✅ **Checkpoint:** `4_scored_renewals` exists; you promoted and rolled back without
touching the scoring code; the Experiments UI shows both runs compared.
**Discussion:** how long does each of these four moves take in your current process?

🆘 Stuck: `solutions/05_solution_mlflow_versioning`.

---

## Lab 5 — Claims documents without an OCR project (30 min, demo)

**The business problem:** claims arrive as letters and PDFs; today a human retypes
them. Watch `06_demo_ai_functions_fnol`: three first-notification-of-loss letters
become queryable rows using `ai_extract`, `ai_classify` and `ai_summarize` — plain
SQL, no new infrastructure, joinable to your policy tables immediately.

**Try after today:** put ~20 real (non-sensitive) documents in a volume, run
`ai_parse_document` + `ai_extract` over them, and read the measured cost per document
from system tables. Compare that to the cost of the manual keying it replaces.

---

## Finished early? Pick your track

Same data, your team's problem:

- **Pricing:** technical premium = frequency × severity. Prompt: **"Using
  2_silver_claims and 2_silver_policies, compute average claim frequency and average
  severity by county and age band, and multiply them into a technical premium per
  segment. Compare it to the actual average annual_premium."**
- **Customer Analytics:** who's about to lapse? Rerun the Lab 4 pattern with
  `renewed` as the label on `2_silver_renewals` — register it as a second model.
- **Digital:** where does the quote funnel leak? Ask your Genie room: *"conversion
  rate by channel and month"* — then add whatever instruction it needed.
- **Data & AI:** rerun the Lab 5 demo notebook yourself and add a fourth FNOL letter
  with a tricky edge case (two dates, no policy number) — what does `ai_extract` do?

---

*All data in this bootcamp is synthetic. Shamrock General is a fictional company.*
