# FHIR API Data Ingestion & Analytics — Databricks Solution

## Architecture Overview

```
HAPI FHIR API (https://hapi.fhir.org/baseR4)
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  RAW LAYER    (JSON files, bucketed by load_date)               │
│  dbfs:/fhir_lakehouse/raw/<resource>/<YYYY-MM-DD>/              │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (Delta, partitioned by load_date)                │
│  dbfs:/fhir_lakehouse/bronze/<resource>/                        │
│  • Parsed FHIR fields  • row_hash  • metadata cols              │
│  • MERGE upsert (no duplicates across runs)                     │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (Delta, SCD Type-2)                              │
│  dbfs:/fhir_lakehouse/silver/<resource>/                        │
│  • Cleansed & deduplicated                                      │
│  • scd_start_date / scd_end_date / scd_is_current / scd_version │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (Delta, reporting-optimised)                       │
│  dbfs:/fhir_lakehouse/gold/<table>/                             │
│  • gold_patient_profile      (dimension)                        │
│  • gold_encounter_fact       (fact)                             │
│  • gold_observation_fact     (fact)                             │
│  • gold_condition_fact       (fact)                             │
│  • gold_patient_summary      (KPI, Power BI ready)             │
│  • gold_condition_prevalence (analytics)                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Notebooks

| # | Notebook | Purpose |
|---|----------|---------|
| 00 | `00_config.py` | Shared config, constants, helper functions |
| 01 | `01_raw_ingestion.py` | Fetch FHIR API pages → Raw JSON files |
| 02 | `02_bronze_layer.py` | Raw JSON → Bronze Delta (MERGE upsert) |
| 03 | `03_silver_layer.py` | Bronze → Silver with SCD Type-2 history |
| 04 | `04_gold_layer.py` | Silver → Gold analytical tables |
| 05 | `05_orchestration.py` | Master runner (use for scheduling) |
| 06 | `06_qa_validation.py` | Data quality checks across all layers |

---

## Setup Instructions

### 1. Import Notebooks into Databricks

**Option A — Workspace UI**
1. Open your Databricks workspace
2. Go to **Workspace → Users → your_username**
3. Click **⋮ → Import** and upload each `.py` file

**Option B — Databricks CLI**
```bash
pip install databricks-cli
databricks configure --token   # enter host + token

for nb in *.py; do
  databricks workspace import \
    --language PYTHON \
    --overwrite \
    "/Users/<your_email>/fhir_medallion/${nb%.py}" \
    "$nb"
done
```

### 2. Cluster Requirements

| Setting | Recommended |
|---------|-------------|
| Runtime | DBR 13.x LTS (includes Delta Lake + PySpark) |
| Node type | Single-node (Community Edition OK) |
| Libraries | None needed — all dependencies are built-in |

### 3. Run the Pipeline

**Interactive (for development)**
Open `05_orchestration.py` → click **Run All**.

**Scheduled (production)**
1. Go to **Workflows → Create Job**
2. Task type: **Notebook**
3. Notebook path: `/Users/<you>/fhir_medallion/05_orchestration`
4. Schedule: `0 6 * * *` (daily 06:00 UTC)
5. Notifications: add your email for failure alerts

**Date range override**
Pass widget values when triggering manually or via API:
```json
{ "start_date": "2025-01-01", "end_date": "2025-01-03" }
```

---

## Table Relationships

```
gold_patient_profile
    │  patient_id
    ├──< gold_encounter_fact       (patient_id)
    ├──< gold_observation_fact     (patient_id, encounter_id)
    ├──< gold_condition_fact       (patient_id, encounter_id)
    └──< gold_patient_summary      (patient_id, aggregated KPIs)

gold_condition_prevalence   ← standalone aggregate (condition_code grain)
```

---

## Metadata & Versioning

Every Bronze record contains:
- `extraction_timestamp` — UTC time the API was called
- `api_url_or_params`    — full request URL (including pagination params)
- `load_date`            — the calendar date of the pipeline run
- `row_hash`             — SHA-256 hash of the raw JSON (change detection)

Every Silver record additionally contains:
- `scd_start_date`  — when this version first appeared
- `scd_end_date`    — when it was superseded (NULL = still current)
- `scd_is_current`  — quick filter: `WHERE scd_is_current = true`
- `scd_version`     — monotonically increasing integer per `fhir_id`

---

## Power BI Connection (optional)

1. Open Power BI Desktop → **Get Data → Azure Databricks**
2. Server: `<your-workspace>.azuredatabricks.net`
3. HTTP Path: copy from **SQL Warehouses → Connection details**
4. Choose tables: `gold_patient_summary`, `gold_condition_prevalence`, etc.
5. Build visuals on the Gold layer — no transformation needed

---

## QA Validation

After each pipeline run, execute `06_qa_validation.py`.
It runs ~25 automated checks and prints a PASS/FAIL summary with row counts
for every layer.

---

## Project Structure

```
fhir_medallion/
├── 00_config.py            ← shared config & helpers
├── 01_raw_ingestion.py     ← FHIR API → Raw JSON
├── 02_bronze_layer.py      ← Raw → Bronze Delta
├── 03_silver_layer.py      ← Bronze → Silver + SCD2
├── 04_gold_layer.py        ← Silver → Gold views
├── 05_orchestration.py     ← master runner / scheduler entry point
├── 06_qa_validation.py     ← data quality checks
└── README.md               ← this file
```
