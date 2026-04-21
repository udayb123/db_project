# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Raw Layer: FHIR API Ingestion
# MAGIC
# MAGIC **What this notebook does**
# MAGIC - Calls the HAPI FHIR API for each resource type (Patient, Encounter, Observation, Condition)
# MAGIC - Handles pagination up to `MAX_PAGES` per resource per execution day
# MAGIC - Saves the raw JSON bundle responses as-is to `raw/<resource>/<load_date>/`
# MAGIC - Adds metadata columns (`extraction_timestamp`, `api_url_or_params`, `load_date`)
# MAGIC
# MAGIC **Run after:** `00_config`

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Override date range via widgets
# MAGIC Uncomment the lines below when running the notebook interactively.

# COMMAND ----------

# dbutils.widgets.text("start_date", START_DATE, "Start Date (YYYY-MM-DD)")
# dbutils.widgets.text("end_date",   END_DATE,   "End Date   (YYYY-MM-DD)")
# START_DATE = dbutils.widgets.get("start_date")
# END_DATE   = dbutils.widgets.get("end_date")

# COMMAND ----------

ensure_paths()

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Fetch raw JSON bundles and persist to Raw layer
# ─────────────────────────────────────────────────────────────────────────────

def ingest_resource_to_raw(resource: str) -> int:
    """
    Fetch all pages for a FHIR resource and write the raw JSON bundles
    to the Raw layer bucketed by today's load date.

    Returns total number of FHIR entries saved.
    """
    load_date_str = date.today().isoformat()
    out_path = f"{RAW_PATH}/{resource.lower()}/{load_date_str}"
    extraction_ts = datetime.utcnow().isoformat()

    print(f"\n{'='*60}")
    print(f"  Ingesting: {resource}")
    print(f"  Output   : {out_path}")
    print(f"{'='*60}")

    bundles = paginate_fhir(resource)
    total_entries = 0
    all_rows = []

    for page_idx, (bundle, req_url) in enumerate(bundles):
        entries = bundle.get("entry", [])
        total_entries += len(entries)
        print(f"  Page {page_idx+1:02d} | entries={len(entries)} | url={req_url[:80]}…")

        for entry in entries:
            resource_json = json.dumps(entry.get("resource", entry),
                                       ensure_ascii=False)
            all_rows.append({
                "raw_json":             resource_json,
                "bundle_id":            bundle.get("id", ""),
                "page_number":          page_idx + 1,
                "extraction_timestamp": extraction_ts,
                "api_url_or_params":    req_url,
                "load_date":            load_date_str,
                "resource_type":        resource,
            })

    if not all_rows:
        print(f"  ⚠️  No entries returned for {resource}. Skipping write.")
        log_pipeline_event(resource, "raw", 0, "WARNING", "No entries from API")
        return 0

    # Write as JSON files partitioned by load_date (already in the path)
    raw_schema = T.StructType([
        T.StructField("raw_json",             T.StringType()),
        T.StructField("bundle_id",            T.StringType()),
        T.StructField("page_number",          T.IntegerType()),
        T.StructField("extraction_timestamp", T.StringType()),
        T.StructField("api_url_or_params",    T.StringType()),
        T.StructField("load_date",            T.StringType()),
        T.StructField("resource_type",        T.StringType()),
    ])

    df = spark.createDataFrame(all_rows, schema=raw_schema)

    (df.coalesce(1)
       .write
       .mode("overwrite")      # idempotent: re-running the same day is safe
       .json(out_path))

    log_pipeline_event(resource, "raw", total_entries, "SUCCESS",
                       f"pages={len(bundles)}")
    print(f"  ✅  Saved {total_entries} entries → {out_path}")
    return total_entries


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Orchestrate in dependency order
# ─────────────────────────────────────────────────────────────────────────────

summary = {}
for resource_name in FHIR_RESOURCES:
    try:
        n = ingest_resource_to_raw(resource_name)
        summary[resource_name] = {"status": "OK", "entries": n}
    except Exception as e:
        print(f"  ❌  {resource_name} FAILED: {e}")
        summary[resource_name] = {"status": "ERROR", "error": str(e)}
        log_pipeline_event(resource_name, "raw", 0, "ERROR", str(e))

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Summary
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("RAW LAYER INGESTION SUMMARY")
print("="*60)
for r, info in summary.items():
    status_icon = "✅" if info["status"] == "OK" else "❌"
    if info["status"] == "OK":
        print(f"  {status_icon}  {r:<15} entries={info['entries']}")
    else:
        print(f"  {status_icon}  {r:<15} ERROR: {info['error']}")
