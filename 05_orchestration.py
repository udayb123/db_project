# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Orchestration: Full Pipeline Runner
# MAGIC
# MAGIC This is the **master notebook** that runs the complete FHIR Medallion pipeline
# MAGIC in the correct dependency order:
# MAGIC
# MAGIC ```
# MAGIC 00_config  →  01_raw_ingestion  →  02_bronze_layer
# MAGIC                                          ↓
# MAGIC                               03_silver_layer  →  04_gold_layer
# MAGIC ```
# MAGIC
# MAGIC ### How to schedule this in Databricks
# MAGIC 1. Go to **Workflows → Create Job**
# MAGIC 2. Add a single task: **Notebook** → `/path/to/05_orchestration`
# MAGIC 3. Set a cluster (single-node is fine for HAPI FHIR scale)
# MAGIC 4. Set a schedule: e.g., `0 6 * * *` (daily 06:00 UTC)
# MAGIC 5. Enable **email on failure** under Notifications
# MAGIC
# MAGIC Alternatively, configure each notebook as a separate **Workflow task**
# MAGIC with `depends_on` to get per-step retry and monitoring.

# COMMAND ----------

# ─── Widget support: allow caller to pass a date range ────────────────────────
dbutils.widgets.text("start_date", "", "Override start date (YYYY-MM-DD, optional)")
dbutils.widgets.text("end_date",   "", "Override end date   (YYYY-MM-DD, optional)")

start_override = dbutils.widgets.get("start_date")
end_override   = dbutils.widgets.get("end_date")

# ─── Notebook paths (relative to this file) ───────────────────────────────────
NB_CONFIG  = "./00_config"
NB_RAW     = "./01_raw_ingestion"
NB_BRONZE  = "./02_bronze_layer"
NB_SILVER  = "./03_silver_layer"
NB_GOLD    = "./04_gold_layer"

TIMEOUT_SECONDS = 3600   # 60-minute guard-rail per step

# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE RUNNER
# ─────────────────────────────────────────────────────────────────────────────

import time

pipeline_start = time.time()
results = {}

def run_step(name: str, notebook_path: str, params: dict = None):
    """Run a notebook step and record success/failure."""
    print(f"\n{'─'*60}")
    print(f"  STEP: {name}")
    print(f"  Path: {notebook_path}")
    print(f"{'─'*60}")
    step_start = time.time()
    try:
        dbutils.notebook.run(notebook_path, TIMEOUT_SECONDS, params or {})
        elapsed = round(time.time() - step_start, 1)
        results[name] = {"status": "SUCCESS", "elapsed_s": elapsed}
        print(f"  ✅  {name} — done in {elapsed}s")
    except Exception as exc:
        elapsed = round(time.time() - step_start, 1)
        results[name] = {"status": "FAILED", "error": str(exc), "elapsed_s": elapsed}
        print(f"  ❌  {name} FAILED: {exc}")
        raise   # propagate so the Workflow job marks the run as failed


# Override params forwarded to raw ingestion notebook
raw_params = {}
if start_override:
    raw_params["start_date"] = start_override
if end_override:
    raw_params["end_date"] = end_override

# ─── Execute steps in order ───────────────────────────────────────────────────

print("=" * 60)
print("  FHIR MEDALLION PIPELINE — START")
print("=" * 60)

run_step("01 Raw Ingestion", NB_RAW,    params=raw_params)
run_step("02 Bronze Layer",  NB_BRONZE)
run_step("03 Silver Layer",  NB_SILVER)
run_step("04 Gold Layer",    NB_GOLD)

# ─── Final summary ────────────────────────────────────────────────────────────

total_elapsed = round(time.time() - pipeline_start, 1)
print("\n" + "=" * 60)
print(f"  PIPELINE COMPLETE  (total: {total_elapsed}s)")
print("=" * 60)
for step, info in results.items():
    icon = "✅" if info["status"] == "SUCCESS" else "❌"
    print(f"  {icon}  {step:<25} {info['status']}  ({info['elapsed_s']}s)")

all_ok = all(v["status"] == "SUCCESS" for v in results.values())
if not all_ok:
    raise RuntimeError("One or more pipeline steps failed — see log above.")
