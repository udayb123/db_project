# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze Layer: Raw JSON → Delta Tables
# MAGIC
# MAGIC **What this notebook does**
# MAGIC - Reads the raw JSON files written by notebook 01
# MAGIC - Flattens the top-level FHIR resource fields (keeps nested as struct/array)
# MAGIC - Adds metadata: `extraction_timestamp`, `api_url_or_params`, `load_date`, `row_hash`
# MAGIC - Writes to Delta tables at `bronze/<resource>/`
# MAGIC - Uses `MERGE` so re-runs are idempotent (no duplicates)
# MAGIC
# MAGIC **Run after:** `01_raw_ingestion`

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────────
# HELPER — read all raw JSON for a resource across all available load dates
# ─────────────────────────────────────────────────────────────────────────────

def read_raw_for_resource(resource: str):
    """
    Read every date partition under raw/<resource>/ and return a single
    DataFrame with the raw_json column plus metadata columns.
    """
    raw_resource_path = f"{RAW_PATH}/{resource.lower()}/*"
    try:
        df = (spark.read
                   .option("multiLine", "true")
                   .json(raw_resource_path))

        # If the file itself has a raw_json string column, return as-is
        # (written by notebook 01); otherwise the whole file is the resource.
        if "raw_json" in df.columns:
            return df
        else:
            # Fallback: whole record is the resource
            return df.withColumn("raw_json",       F.to_json(F.struct("*"))) \
                     .withColumn("extraction_timestamp", F.current_timestamp()) \
                     .withColumn("api_url_or_params",    F.lit(FHIR_BASE_URL)) \
                     .withColumn("load_date",            F.current_date()) \
                     .withColumn("resource_type",        F.lit(resource))
    except Exception as exc:
        print(f"  ⚠️  Could not read raw data for {resource}: {exc}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Per-resource bronze writers
# ─────────────────────────────────────────────────────────────────────────────

def _add_metadata(df, resource: str):
    """Attach standard metadata columns and a row hash."""
    return (df
        .withColumn("resource_type",
                    F.coalesce(F.col("resource_type"), F.lit(resource)))
        .withColumn("extraction_timestamp",
                    F.to_timestamp(F.col("extraction_timestamp")))
        .withColumn("load_date",
                    F.to_date(F.col("load_date")))
        .withColumn("row_hash",
                    F.sha2(F.col("raw_json"), 256))
    )


def write_bronze_patient(raw_df):
    """Parse Patient resource and write to bronze/patient Delta table."""
    parsed = (raw_df
        .withColumn("res",         F.from_json(F.col("raw_json"), T.MapType(T.StringType(), T.StringType())))
        .withColumn("fhir_id",     F.get_json_object(F.col("raw_json"), "$.id"))
        .withColumn("gender",      F.get_json_object(F.col("raw_json"), "$.gender"))
        .withColumn("birth_date",  F.get_json_object(F.col("raw_json"), "$.birthDate"))
        .withColumn("active",      F.get_json_object(F.col("raw_json"), "$.active").cast("boolean"))
        .withColumn("family_name", F.get_json_object(F.col("raw_json"), "$.name[0].family"))
        .withColumn("given_names", F.get_json_object(F.col("raw_json"), "$.name[0].given"))
        .withColumn("language",    F.get_json_object(F.col("raw_json"), "$.communication[0].language.coding[0].code"))
        .filter(F.col("fhir_id").isNotNull())
        .select("fhir_id", "gender", "birth_date", "active",
                "family_name", "given_names", "language",
                "raw_json", "extraction_timestamp",
                "api_url_or_params", "load_date", "resource_type")
    )
    parsed = _add_metadata(parsed, "Patient")
    _merge_to_bronze(parsed, "patient", "fhir_id")
    return parsed.count()


def write_bronze_encounter(raw_df):
    """Parse Encounter resource and write to bronze/encounter Delta table."""
    parsed = (raw_df
        .withColumn("fhir_id",        F.get_json_object(F.col("raw_json"), "$.id"))
        .withColumn("status",         F.get_json_object(F.col("raw_json"), "$.status"))
        .withColumn("class_code",     F.get_json_object(F.col("raw_json"), "$.class.code"))
        .withColumn("patient_ref",    F.get_json_object(F.col("raw_json"), "$.subject.reference"))
        .withColumn("period_start",   F.get_json_object(F.col("raw_json"), "$.period.start"))
        .withColumn("period_end",     F.get_json_object(F.col("raw_json"), "$.period.end"))
        .withColumn("type_display",   F.get_json_object(F.col("raw_json"), "$.type[0].coding[0].display"))
        .filter(F.col("fhir_id").isNotNull())
        .select("fhir_id", "status", "class_code", "patient_ref",
                "period_start", "period_end", "type_display",
                "raw_json", "extraction_timestamp",
                "api_url_or_params", "load_date", "resource_type")
    )
    parsed = _add_metadata(parsed, "Encounter")
    _merge_to_bronze(parsed, "encounter", "fhir_id")
    return parsed.count()


def write_bronze_observation(raw_df):
    """Parse Observation resource and write to bronze/observation Delta table."""
    parsed = (raw_df
        .withColumn("fhir_id",        F.get_json_object(F.col("raw_json"), "$.id"))
        .withColumn("status",         F.get_json_object(F.col("raw_json"), "$.status"))
        .withColumn("code_system",    F.get_json_object(F.col("raw_json"), "$.code.coding[0].system"))
        .withColumn("code_value",     F.get_json_object(F.col("raw_json"), "$.code.coding[0].code"))
        .withColumn("code_display",   F.get_json_object(F.col("raw_json"), "$.code.coding[0].display"))
        .withColumn("patient_ref",    F.get_json_object(F.col("raw_json"), "$.subject.reference"))
        .withColumn("encounter_ref",  F.get_json_object(F.col("raw_json"), "$.encounter.reference"))
        .withColumn("effective_dt",   F.get_json_object(F.col("raw_json"), "$.effectiveDateTime"))
        .withColumn("value_quantity", F.get_json_object(F.col("raw_json"), "$.valueQuantity.value").cast("double"))
        .withColumn("value_unit",     F.get_json_object(F.col("raw_json"), "$.valueQuantity.unit"))
        .withColumn("value_string",   F.get_json_object(F.col("raw_json"), "$.valueString"))
        .filter(F.col("fhir_id").isNotNull())
        .select("fhir_id", "status", "code_system", "code_value", "code_display",
                "patient_ref", "encounter_ref", "effective_dt",
                "value_quantity", "value_unit", "value_string",
                "raw_json", "extraction_timestamp",
                "api_url_or_params", "load_date", "resource_type")
    )
    parsed = _add_metadata(parsed, "Observation")
    _merge_to_bronze(parsed, "observation", "fhir_id")
    return parsed.count()


def write_bronze_condition(raw_df):
    """Parse Condition resource and write to bronze/condition Delta table."""
    parsed = (raw_df
        .withColumn("fhir_id",          F.get_json_object(F.col("raw_json"), "$.id"))
        .withColumn("clinical_status",  F.get_json_object(F.col("raw_json"), "$.clinicalStatus.coding[0].code"))
        .withColumn("verification_status", F.get_json_object(F.col("raw_json"), "$.verificationStatus.coding[0].code"))
        .withColumn("code_system",      F.get_json_object(F.col("raw_json"), "$.code.coding[0].system"))
        .withColumn("code_value",       F.get_json_object(F.col("raw_json"), "$.code.coding[0].code"))
        .withColumn("code_display",     F.get_json_object(F.col("raw_json"), "$.code.coding[0].display"))
        .withColumn("patient_ref",      F.get_json_object(F.col("raw_json"), "$.subject.reference"))
        .withColumn("encounter_ref",    F.get_json_object(F.col("raw_json"), "$.encounter.reference"))
        .withColumn("onset_datetime",   F.get_json_object(F.col("raw_json"), "$.onsetDateTime"))
        .withColumn("recorded_date",    F.get_json_object(F.col("raw_json"), "$.recordedDate"))
        .filter(F.col("fhir_id").isNotNull())
        .select("fhir_id", "clinical_status", "verification_status",
                "code_system", "code_value", "code_display",
                "patient_ref", "encounter_ref", "onset_datetime", "recorded_date",
                "raw_json", "extraction_timestamp",
                "api_url_or_params", "load_date", "resource_type")
    )
    parsed = _add_metadata(parsed, "Condition")
    _merge_to_bronze(parsed, "condition", "fhir_id")
    return parsed.count()


# ─────────────────────────────────────────────────────────────────────────────
# Generic MERGE helper
# ─────────────────────────────────────────────────────────────────────────────

def _merge_to_bronze(incoming_df, table_name: str, pk_col: str):
    """
    Upsert into bronze Delta table.
    - New records  → INSERT
    - Existing records with same row_hash → no-op
    - Existing records with different row_hash → UPDATE (new version)
    """
    target_path = f"{BRONZE_PATH}/{table_name}"

    if DeltaTable.isDeltaTable(spark, target_path):
        dt = DeltaTable.forPath(spark, target_path)
        (dt.alias("target")
           .merge(incoming_df.alias("src"),
                  f"target.{pk_col} = src.{pk_col}")
           .whenMatchedUpdate(
               condition="target.row_hash <> src.row_hash",
               set={c: f"src.{c}" for c in incoming_df.columns}
           )
           .whenNotMatchedInsertAll()
           .execute())
    else:
        # First load — create the Delta table
        (incoming_df.write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .partitionBy("load_date")
                    .save(target_path))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ORCHESTRATION
# ─────────────────────────────────────────────────────────────────────────────

BRONZE_WRITERS = {
    "Patient":     write_bronze_patient,
    "Encounter":   write_bronze_encounter,
    "Observation": write_bronze_observation,
    "Condition":   write_bronze_condition,
}

print("="*60)
print("BRONZE LAYER PROCESSING")
print("="*60)

for resource_name in FHIR_RESOURCES:
    print(f"\n▶  {resource_name}")
    raw_df = read_raw_for_resource(resource_name)
    if raw_df is None:
        print("   Skipped (no raw data).")
        continue
    try:
        count = BRONZE_WRITERS[resource_name](raw_df)
        print(f"   ✅  {count} records → {BRONZE_PATH}/{resource_name.lower()}")
        log_pipeline_event(resource_name, "bronze", count, "SUCCESS")
    except Exception as e:
        print(f"   ❌  {e}")
        log_pipeline_event(resource_name, "bronze", 0, "ERROR", str(e))

print("\n✅  Bronze layer complete.")
