# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Configuration & Shared Utilities
# MAGIC This notebook defines all environment settings, paths, and helper functions
# MAGIC used across the FHIR Medallion pipeline. Run it via `%run ./00_config`
# MAGIC in every downstream notebook.

# COMMAND ----------

import requests, json, time, hashlib
from datetime import datetime, timedelta, date
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ─────────────────────────────────────────────
# 1.  TUNEABLE PARAMETERS  
# ─────────────────────────────────────────────
FHIR_BASE_URL   = "https://hapi.fhir.org/baseR4"
PAGE_COUNT       = 20          # _count per page  (max 50 for hapi)
MAX_PAGES        = 5           # safety cap per resource per day
REQUEST_TIMEOUT  = 30          # seconds
RETRY_ATTEMPTS   = 3
RETRY_BACKOFF    = 2           # seconds between retries

# Resources to ingest — order matters (Patient first, then dependents)
FHIR_RESOURCES  = ["Patient", "Encounter", "Observation", "Condition"]

# Date range: last 3 days (override via Databricks widgets if needed)
# Format: YYYY-MM-DD
END_DATE   = date.today().isoformat()
START_DATE = (date.today() - timedelta(days=2)).isoformat()

# ─────────────────────────────────────────────
# 2.  STORAGE PATHS  (DBFS — works on Community / trial clusters)
# ─────────────────────────────────────────────
BASE_PATH   = "dbfs:/fhir_lakehouse"
RAW_PATH    = f"{BASE_PATH}/raw"
BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"
GOLD_PATH   = f"{BASE_PATH}/gold"
LOG_PATH    = f"{BASE_PATH}/metadata/pipeline_logs"

# ─────────────────────────────────────────────
# 3.  SCHEMA DEFINITIONS  (used across layers)
# ─────────────────────────────────────────────

# Metadata added to every bronze record
METADATA_SCHEMA = T.StructType([
    T.StructField("extraction_timestamp", T.TimestampType(), False),
    T.StructField("api_url_or_params",    T.StringType(),    False),
    T.StructField("load_date",            T.DateType(),      False),
    T.StructField("resource_type",        T.StringType(),    False),
    T.StructField("row_hash",             T.StringType(),    False),
])

# SCD Type-2 tracking columns added at Silver
SCD2_SCHEMA = T.StructType([
    T.StructField("scd_start_date",  T.TimestampType(), False),
    T.StructField("scd_end_date",    T.TimestampType(), True),   # NULL = current
    T.StructField("scd_is_current",  T.BooleanType(),   False),
    T.StructField("scd_version",     T.IntegerType(),   False),
])

# ─────────────────────────────────────────────
# 4.  HELPER FUNCTIONS
# ─────────────────────────────────────────────

def get_dbutils():
    """Safe dbutils access works in notebooks and jobs."""
    try:
        return dbutils          # noqa: F821  (injected by Databricks)
    except NameError:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)   # noqa: F821


def ensure_paths():
    """Create base directory structure if it doesn't exist."""
    dbu = get_dbutils()
    for p in [RAW_PATH, BRONZE_PATH, SILVER_PATH, GOLD_PATH, LOG_PATH]:
        try:
            dbu.fs.mkdirs(p)
        except Exception:
            pass   # already exists


def fetch_fhir_page(resource: str, params: dict) -> dict:
    """
    Fetch a single FHIR page with retry logic.
    Returns the parsed JSON bundle or raises after exhausting retries.
    """
    url = f"{FHIR_BASE_URL}/{resource}"
    headers = {"Accept": "application/fhir+json"}

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, params=params, headers=headers,
                                timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json(), resp.url
        except requests.RequestException as exc:
            if attempt == RETRY_ATTEMPTS:
                raise RuntimeError(
                    f"[FHIR] Failed {resource} after {RETRY_ATTEMPTS} attempts: {exc}"
                ) from exc
            time.sleep(RETRY_BACKOFF * attempt)


def paginate_fhir(resource: str, extra_params: dict = None) -> list:
    """
    Walk through all pages for a FHIR resource between START_DATE and END_DATE.
    Returns list of (bundle_dict, request_url) tuples.
    """
    params = {
        "_count":          PAGE_COUNT,
        "_lastUpdated":    f"ge{START_DATE}",
        "_sort":           "-_lastUpdated",
    }
    if extra_params:
        params.update(extra_params)

    bundles = []
    page_num = 0

    while page_num < MAX_PAGES:
        bundle, req_url = fetch_fhir_page(resource, params)
        bundles.append((bundle, req_url))
        page_num += 1

        # Follow 'next' link if present
        next_url = None
        for link in bundle.get("link", []):
            if link.get("relation") == "next":
                next_url = link["url"]
                break

        if not next_url or not bundle.get("entry"):
            break

        # Parse params from the next URL for the next iteration
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(next_url)
        params = {k: v[0] for k, v in parse_qs(parsed.query).items()}

    return bundles


def compute_row_hash(row_json: str) -> str:
    """SHA-256 hash of a JSON string — used for change detection."""
    return hashlib.sha256(row_json.encode()).hexdigest()


def log_pipeline_event(resource: str, layer: str, records: int,
                        status: str, message: str = ""):
    """Append a single-row log entry to the pipeline log Delta table."""
    schema = T.StructType([
        T.StructField("log_ts",        T.TimestampType()),
        T.StructField("resource",      T.StringType()),
        T.StructField("layer",         T.StringType()),
        T.StructField("records_count", T.LongType()),
        T.StructField("status",        T.StringType()),
        T.StructField("message",       T.StringType()),
    ])
    row = [(datetime.utcnow(), resource, layer, records, status, message)]
    log_df = spark.createDataFrame(row, schema)   # noqa: F821
    (log_df.write
           .format("delta")
           .mode("append")
           .option("mergeSchema", "true")
           .save(LOG_PATH))


print("✅  Config loaded — paths, helpers, and schemas are ready.")
print(f"    Date range : {START_DATE}  →  {END_DATE}")
print(f"    Resources  : {FHIR_RESOURCES}")
print(f"    Base path  : {BASE_PATH}")
