# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver Layer: Cleanse + SCD Type-2 Versioning
# MAGIC
# MAGIC **What this notebook does**
# MAGIC - Reads Bronze Delta tables
# MAGIC - Cleanses: deduplication, type casting, null handling, standardisation
# MAGIC - Implements **SCD Type-2** to track historical changes per FHIR resource
# MAGIC   - `scd_start_date`  — when this version became effective
# MAGIC   - `scd_end_date`    — when it was superseded (NULL = current record)
# MAGIC   - `scd_is_current`  — boolean flag for easy filtering
# MAGIC   - `scd_version`     — monotonically increasing version counter per `fhir_id`
# MAGIC - Writes to `silver/<resource>/`
# MAGIC
# MAGIC **Run after:** `02_bronze_layer`

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────────
# SCD Type-2 MERGE LOGIC 
# ─────────────────────────────────────────────────────────────────────────────

def apply_scd2(bronze_df, table_name: str, pk_col: str = "fhir_id"):
    """
    Apply SCD Type-2 logic:

    1. Load the existing silver table (if any).
    2. For every incoming bronze record:
       a. If `fhir_id` is new → INSERT with scd_version=1, scd_is_current=True.
       b. If `fhir_id` exists AND `row_hash` is different:
          - Close the old current row (set scd_end_date, scd_is_current=False).
          - INSERT a new row with incremented scd_version.
       c. If `fhir_id` exists AND `row_hash` is the same → no change.

    Writes merged result back to silver/<table_name>/ as a full Delta overwrite
    (this keeps the file layout clean and avoids small-file accumulation on DBFS).
    """
    silver_path = f"{SILVER_PATH}/{table_name}"
    now_ts      = datetime.utcnow()

    # Deduplicate bronze: keep the latest extraction per fhir_id
    w = Window.partitionBy(pk_col).orderBy(F.col("extraction_timestamp").desc())
    incoming = (bronze_df
                .withColumn("_rn", F.row_number().over(w))
                .filter(F.col("_rn") == 1)
                .drop("_rn"))

    if not DeltaTable.isDeltaTable(spark, silver_path):
        # ── FIRST LOAD ────────────────────────────────────────────────────────
        silver_df = (incoming
                     .withColumn("scd_start_date",  F.lit(now_ts))
                     .withColumn("scd_end_date",     F.lit(None).cast(T.TimestampType()))
                     .withColumn("scd_is_current",   F.lit(True))
                     .withColumn("scd_version",      F.lit(1)))

        (silver_df.write
                  .format("delta")
                  .mode("overwrite")
                  .option("overwriteSchema", "true")
                  .partitionBy("load_date")
                  .save(silver_path))

        return silver_df.count()

    # ── INCREMENTAL LOAD — full SCD2 merge ───────────────────────────────────
    existing = spark.read.format("delta").load(silver_path)

    # Identify changed rows (different hash) among current records
    current = existing.filter(F.col("scd_is_current") == True)   # noqa: E712

    changed = (incoming.alias("new")
               .join(current.select(pk_col, "row_hash").alias("cur"),
                     on=pk_col, how="inner")
               .filter(F.col("new.row_hash") != F.col("cur.row_hash"))
               .select("new.*"))

    new_ids = (incoming.alias("new")
               .join(existing.select(pk_col).distinct().alias("ex"),
                     on=pk_col, how="left_anti")
               .select("new.*"))

    # Max version per id (to increment correctly)
    max_ver = (existing.groupBy(pk_col)
               .agg(F.max("scd_version").alias("max_ver")))

    # Build new version rows for changed + brand-new records
    new_versions = (
        changed.union(new_ids)
               .join(max_ver, on=pk_col, how="left")
               .withColumn("scd_version",
                            F.coalesce(F.col("max_ver"), F.lit(0)) + 1)
               .drop("max_ver")
               .withColumn("scd_start_date",  F.lit(now_ts))
               .withColumn("scd_end_date",     F.lit(None).cast(T.TimestampType()))
               .withColumn("scd_is_current",   F.lit(True))
    )

    # IDs whose current row needs to be expired
    ids_to_expire = changed.select(pk_col).distinct()

    expired = (existing
               .join(ids_to_expire.alias("chg"), on=pk_col, how="inner")
               .filter(F.col("scd_is_current") == True)   # noqa: E712
               .withColumn("scd_end_date",    F.lit(now_ts))
               .withColumn("scd_is_current",  F.lit(False)))

    # Unchanged current + all historical rows remain as-is
    unchanged = (existing
                 .join(ids_to_expire.alias("chg"), on=pk_col, how="left_anti"))

    # Stitch everything together and rewrite (overwrite partition-safe)
    final = unchanged.union(expired).union(new_versions)

    (final.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .partitionBy("load_date")
          .save(silver_path))

    total     = final.count()
    new_cnt   = new_versions.count()
    chg_cnt   = changed.count()
    print(f"    SCD2 | new={new_cnt}  changed={chg_cnt}  total_rows={total}")
    return total


# ─────────────────────────────────────────────────────────────────────────────
# CLEANSE FUNCTIONS  (per resource)
# ─────────────────────────────────────────────────────────────────────────────

def cleanse_patient(df):
    return (df
        .withColumn("birth_date",  F.to_date("birth_date", "yyyy-MM-dd"))
        .withColumn("gender",      F.lower(F.trim(F.col("gender"))))
        .withColumn("active",      F.coalesce(F.col("active"), F.lit(True)))
        .withColumn("family_name", F.initcap(F.trim(F.col("family_name"))))
        .dropDuplicates(["fhir_id"])
    )


def cleanse_encounter(df):
    return (df
        .withColumn("period_start",  F.to_timestamp("period_start"))
        .withColumn("period_end",    F.to_timestamp("period_end"))
        .withColumn("status",        F.lower(F.trim(F.col("status"))))
        .withColumn("patient_id",
                    F.regexp_extract("patient_ref", r"Patient/(.+)", 1))
        .dropDuplicates(["fhir_id"])
    )


def cleanse_observation(df):
    return (df
        .withColumn("effective_dt",    F.to_timestamp("effective_dt"))
        .withColumn("status",          F.lower(F.trim(F.col("status"))))
        .withColumn("patient_id",
                    F.regexp_extract("patient_ref", r"Patient/(.+)", 1))
        .withColumn("encounter_id",
                    F.regexp_extract("encounter_ref", r"Encounter/(.+)", 1))
        .dropDuplicates(["fhir_id"])
    )


def cleanse_condition(df):
    return (df
        .withColumn("onset_datetime",  F.to_timestamp("onset_datetime"))
        .withColumn("recorded_date",   F.to_date("recorded_date", "yyyy-MM-dd"))
        .withColumn("clinical_status", F.lower(F.trim(F.col("clinical_status"))))
        .withColumn("patient_id",
                    F.regexp_extract("patient_ref", r"Patient/(.+)", 1))
        .withColumn("encounter_id",
                    F.regexp_extract("encounter_ref", r"Encounter/(.+)", 1))
        .dropDuplicates(["fhir_id"])
    )


CLEANSE_FNS = {
    "Patient":     cleanse_patient,
    "Encounter":   cleanse_encounter,
    "Observation": cleanse_observation,
    "Condition":   cleanse_condition,
}

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

print("="*60)
print("SILVER LAYER PROCESSING  (Cleanse + SCD Type-2)")
print("="*60)

for resource_name in FHIR_RESOURCES:
    bronze_path = f"{BRONZE_PATH}/{resource_name.lower()}"
    print(f"\n▶  {resource_name}")

    try:
        bronze_df = spark.read.format("delta").load(bronze_path)
        cleansed  = CLEANSE_FNS[resource_name](bronze_df)
        count     = apply_scd2(cleansed, resource_name.lower())
        print(f"   ✅  {count} total rows (including history) → silver/{resource_name.lower()}")
        log_pipeline_event(resource_name, "silver", count, "SUCCESS")
    except Exception as e:
        import traceback
        print(f"   ❌  {e}")
        traceback.print_exc()
        log_pipeline_event(resource_name, "silver", 0, "ERROR", str(e))

print("\n✅  Silver layer complete.")
