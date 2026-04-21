# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — QA & Validation
# MAGIC
# MAGIC Run this notebook **after** the pipeline completes to verify data quality
# MAGIC at every layer.  Each check prints PASS / FAIL with counts.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

CHECKS = []   # accumulate (layer, check, result, detail)

def check(layer, name, condition: bool, detail=""):
    icon   = "✅ PASS" if condition else "❌ FAIL"
    CHECKS.append((layer, name, icon, detail))
    print(f"  [{layer:<8}]  {icon}  {name}  {detail}")


# ─────────────────────────────────────────────────────────────────────────────
# RAW LAYER CHECKS
# ─────────────────────────────────────────────────────────────────────────────
print("\n── RAW LAYER ──────────────────────────────────────────────")
for res in FHIR_RESOURCES:
    path = f"{RAW_PATH}/{res.lower()}"
    try:
        dbu = get_dbutils()
        files = dbu.fs.ls(path)
        check("RAW", f"{res} directory exists", len(files) > 0,
              f"({len(files)} date partition(s))")
    except Exception as e:
        check("RAW", f"{res} directory exists", False, str(e))

# ─────────────────────────────────────────────────────────────────────────────
# BRONZE LAYER CHECKS
# ─────────────────────────────────────────────────────────────────────────────
print("\n── BRONZE LAYER ────────────────────────────────────────────")
for res in FHIR_RESOURCES:
    path = f"{BRONZE_PATH}/{res.lower()}"
    try:
        df = spark.read.format("delta").load(path)
        cnt = df.count()
        null_ids  = df.filter(F.col("fhir_id").isNull()).count()
        null_hash = df.filter(F.col("row_hash").isNull()).count()
        null_ts   = df.filter(F.col("extraction_timestamp").isNull()).count()
        dupes     = cnt - df.dropDuplicates(["fhir_id", "load_date"]).count()

        check("BRONZE", f"{res} has rows",          cnt > 0,        f"rows={cnt}")
        check("BRONZE", f"{res} no null fhir_id",   null_ids == 0,  f"nulls={null_ids}")
        check("BRONZE", f"{res} no null row_hash",  null_hash == 0, f"nulls={null_hash}")
        check("BRONZE", f"{res} no null timestamp", null_ts == 0,   f"nulls={null_ts}")
        check("BRONZE", f"{res} no dupes per day",  dupes == 0,     f"dupes={dupes}")
    except Exception as e:
        check("BRONZE", f"{res} readable", False, str(e))

# ─────────────────────────────────────────────────────────────────────────────
# SILVER LAYER CHECKS
# ─────────────────────────────────────────────────────────────────────────────
print("\n── SILVER LAYER ────────────────────────────────────────────")
for res in FHIR_RESOURCES:
    path = f"{SILVER_PATH}/{res.lower()}"
    try:
        df      = spark.read.format("delta").load(path)
        total   = df.count()
        current = df.filter(F.col("scd_is_current") == True).count()   # noqa
        hist    = total - current
        multi   = (df.filter(F.col("scd_is_current") == True)
                     .groupBy("fhir_id")
                     .count()
                     .filter(F.col("count") > 1)
                     .count())

        check("SILVER", f"{res} has rows",             total > 0,   f"total={total}")
        check("SILVER", f"{res} current records",      current > 0, f"current={current}")
        check("SILVER", f"{res} historical rows",      True,        f"history={hist}")
        check("SILVER", f"{res} 1 current per id",     multi == 0,  f"duplicates={multi}")
        check("SILVER", f"{res} scd_version ≥ 1",
              df.filter(F.col("scd_version") < 1).count() == 0,
              "all versions ≥ 1")
    except Exception as e:
        check("SILVER", f"{res} readable", False, str(e))

# ─────────────────────────────────────────────────────────────────────────────
# GOLD LAYER CHECKS
# ─────────────────────────────────────────────────────────────────────────────
print("\n── GOLD LAYER ──────────────────────────────────────────────")
gold_tables_required = [
    "gold_patient_profile",
    "gold_encounter_fact",
    "gold_observation_fact",
    "gold_condition_fact",
    "gold_patient_summary",
    "gold_condition_prevalence",
]
for tbl in gold_tables_required:
    path = f"{GOLD_PATH}/{tbl}"
    try:
        df  = spark.read.format("delta").load(path)
        cnt = df.count()
        check("GOLD", f"{tbl}", cnt > 0, f"rows={cnt}")
    except Exception as e:
        check("GOLD", tbl, False, str(e))

# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE LOG CHECK
# ─────────────────────────────────────────────────────────────────────────────
print("\n── PIPELINE METADATA ───────────────────────────────────────")
try:
    log_df = spark.read.format("delta").load(LOG_PATH)
    log_cnt = log_df.count()
    errors  = log_df.filter(F.col("status") == "ERROR").count()
    check("META", "Pipeline log populated", log_cnt > 0, f"events={log_cnt}")
    check("META", "No ERROR events",        errors == 0, f"errors={errors}")
    display(log_df.orderBy(F.col("log_ts").desc()).limit(20))
except Exception as e:
    check("META", "Pipeline log readable", False, str(e))

# ─────────────────────────────────────────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("QA SUMMARY")
print("="*60)
passed = sum(1 for _, _, r, _ in CHECKS if "PASS" in r)
failed = sum(1 for _, _, r, _ in CHECKS if "FAIL" in r)
print(f"  PASSED : {passed}")
print(f"  FAILED : {failed}")
print(f"  TOTAL  : {len(CHECKS)}")
if failed > 0:
    print("\nFailed checks:")
    for layer, name, result, detail in CHECKS:
        if "FAIL" in result:
            print(f"  [{layer}] {name}  {detail}")
