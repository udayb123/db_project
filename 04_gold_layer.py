# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold Layer: Analytical Views & Aggregates
# MAGIC
# MAGIC **What this notebook does**
# MAGIC - Reads current (scd_is_current = True) Silver records
# MAGIC - Creates denormalised, reporting-ready Gold Delta tables:
# MAGIC   - `gold_patient_profile`       — Patient dimension with demographics
# MAGIC   - `gold_encounter_fact`        — Encounter fact linked to Patient
# MAGIC   - `gold_observation_fact`      — Observation fact linked to Patient + Encounter
# MAGIC   - `gold_condition_fact`        — Condition fact linked to Patient + Encounter
# MAGIC   - `gold_patient_summary`       — Aggregated patient-level KPIs (for dashboards)
# MAGIC   - `gold_condition_prevalence`  — Condition code frequency (analytics)
# MAGIC - Registers each table as a Spark SQL view for direct SQL querying
# MAGIC
# MAGIC **Run after:** `03_silver_layer`

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────────
# LOAD CURRENT SILVER RECORDS
# ─────────────────────────────────────────────────────────────────────────────

def load_current_silver(resource: str):
    """Return only the current (latest) version of each FHIR resource."""
    path = f"{SILVER_PATH}/{resource.lower()}"
    return (spark.read
                 .format("delta")
                 .load(path)
                 .filter(F.col("scd_is_current") == True))   # noqa: E712


def write_gold(df, table_name: str):
    """Overwrite a Gold Delta table and register as a Spark SQL temp view."""
    gold_path = f"{GOLD_PATH}/{table_name}"
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(gold_path))
    # Register view so analysts can query with SQL in the same session
    spark.read.format("delta").load(gold_path).createOrReplaceTempView(table_name)
    count = df.count()
    print(f"   ✅  {table_name:<35} {count:>6} rows → {gold_path}")
    return count


# ─────────────────────────────────────────────────────────────────────────────
# LOAD SILVER TABLES
# ─────────────────────────────────────────────────────────────────────────────

try:
    silver_patient     = load_current_silver("Patient")
    silver_encounter   = load_current_silver("Encounter")
    silver_observation = load_current_silver("Observation")
    silver_condition   = load_current_silver("Condition")
except Exception as e:
    raise RuntimeError(
        "Silver tables are missing — run 03_silver_layer first."
    ) from e

# ─────────────────────────────────────────────────────────────────────────────
# GOLD TABLE 1: Patient Dimension
# ─────────────────────────────────────────────────────────────────────────────

gold_patient = (silver_patient
    .select(
        F.col("fhir_id").alias("patient_id"),
        "family_name",
        "given_names",
        "gender",
        F.col("birth_date").alias("date_of_birth"),
        F.floor(F.datediff(F.current_date(), F.col("birth_date")) / 365.25)
         .alias("age_years"),
        "active",
        "language",
        "extraction_timestamp",
        "load_date",
        "scd_version",
    )
)

# ─────────────────────────────────────────────────────────────────────────────
# GOLD TABLE 2: Encounter Fact
# ─────────────────────────────────────────────────────────────────────────────

gold_encounter = (silver_encounter
    .select(
        F.col("fhir_id").alias("encounter_id"),
        F.col("patient_id"),
        F.col("status").alias("encounter_status"),
        F.col("class_code").alias("encounter_class"),
        F.col("type_display").alias("encounter_type"),
        F.col("period_start").alias("encounter_start"),
        F.col("period_end").alias("encounter_end"),
        F.round(
            (F.unix_timestamp("period_end") - F.unix_timestamp("period_start")) / 3600, 2
        ).alias("duration_hours"),
        "load_date",
    )
    .join(gold_patient.select("patient_id", "gender", "age_years", "family_name"),
          on="patient_id", how="left")
)

# ─────────────────────────────────────────────────────────────────────────────
# GOLD TABLE 3: Observation Fact
# ─────────────────────────────────────────────────────────────────────────────

gold_observation = (silver_observation
    .select(
        F.col("fhir_id").alias("observation_id"),
        F.col("patient_id"),
        F.col("encounter_id"),
        F.col("status").alias("obs_status"),
        F.col("code_system").alias("obs_code_system"),
        F.col("code_value").alias("obs_code"),
        F.col("code_display").alias("obs_name"),
        F.col("effective_dt").alias("obs_datetime"),
        F.col("value_quantity").alias("numeric_value"),
        F.col("value_unit").alias("unit"),
        F.col("value_string").alias("string_value"),
        "load_date",
    )
)

# ─────────────────────────────────────────────────────────────────────────────
# GOLD TABLE 4: Condition Fact
# ─────────────────────────────────────────────────────────────────────────────

gold_condition = (silver_condition
    .select(
        F.col("fhir_id").alias("condition_id"),
        F.col("patient_id"),
        F.col("encounter_id"),
        F.col("clinical_status"),
        F.col("verification_status"),
        F.col("code_system").alias("condition_code_system"),
        F.col("code_value").alias("condition_code"),
        F.col("code_display").alias("condition_name"),
        F.col("onset_datetime"),
        F.col("recorded_date"),
        "load_date",
    )
)

# ─────────────────────────────────────────────────────────────────────────────
# GOLD TABLE 5: Patient Summary (KPI table — Power BI ready)
# ─────────────────────────────────────────────────────────────────────────────

enc_counts = (gold_encounter
    .groupBy("patient_id")
    .agg(F.count("encounter_id").alias("total_encounters"),
         F.min("encounter_start").alias("first_encounter_date"),
         F.max("encounter_start").alias("latest_encounter_date")))

obs_counts = (gold_observation
    .groupBy("patient_id")
    .agg(F.count("observation_id").alias("total_observations")))

cond_counts = (gold_condition
    .filter(F.col("clinical_status") == "active")
    .groupBy("patient_id")
    .agg(F.count("condition_id").alias("active_conditions"),
         F.collect_list("condition_name").alias("condition_list")))

gold_patient_summary = (gold_patient
    .join(enc_counts,  on="patient_id", how="left")
    .join(obs_counts,  on="patient_id", how="left")
    .join(cond_counts, on="patient_id", how="left")
    .withColumn("total_encounters",   F.coalesce(F.col("total_encounters"),   F.lit(0)))
    .withColumn("total_observations", F.coalesce(F.col("total_observations"), F.lit(0)))
    .withColumn("active_conditions",  F.coalesce(F.col("active_conditions"),  F.lit(0)))
)

# ─────────────────────────────────────────────────────────────────────────────
# GOLD TABLE 6: Condition Prevalence (analytics / drill-down)
# ─────────────────────────────────────────────────────────────────────────────

gold_condition_prevalence = (gold_condition
    .groupBy("condition_code", "condition_name", "condition_code_system")
    .agg(
        F.count("condition_id").alias("total_cases"),
        F.countDistinct("patient_id").alias("unique_patients"),
        F.sum(F.when(F.col("clinical_status") == "active", 1).otherwise(0))
         .alias("active_cases"),
        F.min("onset_datetime").alias("earliest_onset"),
        F.max("onset_datetime").alias("latest_onset"),
    )
    .orderBy(F.col("total_cases").desc())
)

# ─────────────────────────────────────────────────────────────────────────────
# WRITE ALL GOLD TABLES
# ─────────────────────────────────────────────────────────────────────────────

print("="*60)
print("GOLD LAYER PROCESSING")
print("="*60)

gold_tables = {
    "gold_patient_profile":      gold_patient,
    "gold_encounter_fact":       gold_encounter,
    "gold_observation_fact":     gold_observation,
    "gold_condition_fact":       gold_condition,
    "gold_patient_summary":      gold_patient_summary,
    "gold_condition_prevalence": gold_condition_prevalence,
}

for tbl_name, df in gold_tables.items():
    try:
        cnt = write_gold(df, tbl_name)
        log_pipeline_event(tbl_name, "gold", cnt, "SUCCESS")
    except Exception as e:
        print(f"   ❌  {tbl_name}: {e}")
        log_pipeline_event(tbl_name, "gold", 0, "ERROR", str(e))

print("\n✅  Gold layer complete.")

# ─────────────────────────────────────────────────────────────────────────────
# QUICK VALIDATION QUERIES
# ─────────────────────────────────────────────────────────────────────────────

# MAGIC %md
# MAGIC ## Sample SQL Queries (run these cells interactively)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 most prevalent conditions
# MAGIC SELECT condition_name, total_cases, unique_patients, active_cases
# MAGIC FROM gold_condition_prevalence
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gender distribution
# MAGIC SELECT gender,
# MAGIC        COUNT(*)               AS patient_count,
# MAGIC        AVG(age_years)         AS avg_age,
# MAGIC        SUM(total_encounters)  AS total_encounters
# MAGIC FROM gold_patient_summary
# MAGIC GROUP BY gender
# MAGIC ORDER BY patient_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Encounter duration distribution
# MAGIC SELECT encounter_class,
# MAGIC        COUNT(*)              AS total,
# MAGIC        ROUND(AVG(duration_hours), 2) AS avg_hours,
# MAGIC        MAX(duration_hours)   AS max_hours
# MAGIC FROM gold_encounter_fact
# MAGIC WHERE duration_hours IS NOT NULL
# MAGIC GROUP BY encounter_class
# MAGIC ORDER BY total DESC
