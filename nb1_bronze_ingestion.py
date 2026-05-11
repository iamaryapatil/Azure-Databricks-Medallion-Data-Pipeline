# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 1 — Bronze Layer: Raw Ingestion
# MAGIC ***** Visa Analytics Pipeline**
# MAGIC - Resolves latest XLSX URLs from data.gov.au CKAN API
# MAGIC - Downloads each XLSX in-memory (streaming, with retry)
# MAGIC - Extracts and parses pivot cache XMLs
# MAGIC - Writes raw Delta tables to ADLS Bronze layer

# COMMAND ----------

# CELL 1 — Install dependencies
%pip install requests

# COMMAND ----------

# CELL 2 — Imports
import requests
import zipfile
import os
import xml.etree.ElementTree as ET
import csv
import time
import io
from typing import List, Optional, Dict
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# CELL 3 — Parameters (ADF will inject date_partition at runtime)
dbutils.widgets.text("date_partition", datetime.today().strftime("%Y%m%d"))
DATE_PARTITION = dbutils.widgets.get("date_partition")
print(f"Running for date partition: {DATE_PARTITION}")

# COMMAND ----------

# CELL 4 — ADLS Mount Configuration

# Mount ADLS once — skip if already mounted
already_mounted = any(m.mountPoint == MOUNT_POINT for m in dbutils.fs.mounts())
if not already_mounted:
    dbutils.fs.mount(
        source=f"wasbs://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
        mount_point=MOUNT_POINT,
        extra_configs={
            f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.blob.core.windows.net": STORAGE_ACCOUNT_KEY
        }
    )
    print(f"Mounted ADLS at {MOUNT_POINT}")
else:
    print(f"Already mounted at {MOUNT_POINT}")

BRONZE_PATH = f"{MOUNT_POINT}/bronze"

# COMMAND ----------

# CELL 4 — ADLS Direct Access Configuration (Unity Catalog compatible)

# Unity Catalog workspaces don't support dbutils.fs.mount()
# Instead we configure direct access via Spark config — no mount needed
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    STORAGE_ACCOUNT_KEY
)

# Use abfss:// protocol instead of /mnt/ paths
BRONZE_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze"
SILVER_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver"
GOLD_PATH   = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/gold"

# Quick connectivity test
try:
    dbutils.fs.ls(f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/")
    print("ADLS connection successful")
except Exception as e:
    print(f"Connection failed: {e}")

# COMMAND ----------

# CELL 5 — CKAN Helpers
CKAN_BASE = "https://data.gov.au/data/api/3/action/package_show?id={dataset_id}"

def _pick_latest(resources: list) -> Optional[dict]:
    if not resources:
        return None
    def sort_key(r):
        return (r.get("last_modified") or r.get("created") or "")
    return sorted(resources, key=sort_key, reverse=True)[0]

def get_xlsx_url(dataset_id: str, contains: Optional[str] = None) -> str:
    api_url = CKAN_BASE.format(dataset_id=dataset_id)
    resp = requests.get(api_url, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("success"):
        raise RuntimeError(f"CKAN API returned unsuccessful for dataset {dataset_id}")
    result         = payload.get("result", {})
    xlsx_resources = result.get("resources", [])

    chosen = None
    if contains:
        c = contains.lower()
        def matches(r):
            return any(c in (r.get(k) or "").lower() for k in ("name", "description", "url"))
        preferred = [r for r in xlsx_resources if matches(r)]
        if preferred:
            chosen = _pick_latest(preferred)
    if not chosen:
        chosen = _pick_latest(xlsx_resources)

    if not chosen or not chosen.get("url"):
        raise RuntimeError(f"No XLSX resource found for dataset {dataset_id}")
    return chosen["url"]

# COMMAND ----------

# CELL 6 — In-memory Download with retry
def download_excel_bytes(url: str, max_retries: int = 3, backoff: float = 1.5) -> bytes:
    attempt  = 0
    last_err = None
    while attempt < max_retries:
        try:
            with requests.get(url, timeout=120, stream=True) as r:
                r.raise_for_status()
                buf = io.BytesIO()
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        buf.write(chunk)
                buf.seek(0)
                return buf.getvalue()
        except Exception as e:
            attempt += 1
            last_err = e
            wait = backoff ** attempt
            print(f"Download failed (attempt {attempt}/{max_retries}): {e}. Retrying in {wait:.1f}s...")
            time.sleep(wait)
    raise RuntimeError(f"Failed to download after {max_retries} attempts: {last_err}")

# COMMAND ----------

# CELL 7 — Pivot XML Extraction
def extract_pivot_xmls_in_memory(xlsx_bytes: bytes):
    with zipfile.ZipFile(io.BytesIO(xlsx_bytes), 'r') as z:
        pivot_defs = [f for f in z.namelist() if f.startswith('xl/pivotCache') and f.endswith('Definition1.xml')]
        pivot_recs = [f for f in z.namelist() if f.startswith('xl/pivotCache') and f.endswith('Records1.xml')]
        if not pivot_defs or not pivot_recs:
            raise Exception("Could not find pivot cache XML files.")
        def_xml = z.read(pivot_defs[0])
        rec_xml = z.read(pivot_recs[0])
        print("Extracted pivot cache files in-memory.")
        return def_xml, rec_xml

def parse_cache_definition_from_bytes(xml_bytes: bytes):
    ns   = {'main': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
    root = ET.fromstring(xml_bytes)
    fields       = []
    shared_items = []
    for field in root.findall('.//main:cacheField', ns):
        name = field.attrib.get('name')
        fields.append(name)
        items  = []
        shared = field.find('main:sharedItems', ns)
        if shared is not None:
            for item in shared:
                tag = item.tag.split('}')[-1]
                if tag == 's':
                    items.append(item.attrib.get('v', item.text))
                elif tag == 'n':
                    items.append(item.attrib.get('v'))
                elif tag == 'm':
                    items.append('')
                elif tag == 'd':
                    items.append(item.attrib.get('v'))
                else:
                    items.append(item.text)
        else:
            items = None
        shared_items.append(items or [])
    return fields, shared_items

def get_record_count(xml_bytes: bytes) -> int:
    for event, elem in ET.iterparse(io.BytesIO(xml_bytes), events=("start",)):
        if elem.tag.endswith("pivotCacheRecords"):
            return int(elem.attrib.get("count", 0))
    return 0

def parse_cache_records_from_bytes(xml_bytes: bytes, shared_items: list) -> list:
    data = []
    for event, elem in ET.iterparse(io.BytesIO(xml_bytes), events=('end',)):
        if elem.tag.endswith('r'):
            values = []
            cells  = list(elem)
            for i in range(len(shared_items)):
                if i >= len(cells):
                    values.append('')
                    continue
                cell = cells[i]
                v    = cell.attrib.get('v')
                if shared_items[i]:
                    if v is not None:
                        try:
                            idx = int(v)
                            values.append(shared_items[i][idx])
                        except (IndexError, ValueError):
                            values.append('')
                    else:
                        values.append('')
                else:
                    values.append(v if v is not None else '')
            data.append(values)
            elem.clear()
    return data

# COMMAND ----------

# CELL 8 — Full ingestion pipeline per dataset
import re
def ingest_dataset_to_bronze(dataset_name, dataset_id, contains, bronze_path, date_partition):
    print(f"\n{'='*60}")
    print(f"Processing: {dataset_name}")
    print(f"Dataset ID: {dataset_id} | Filter: {contains or '(none)'}")

    url = get_xlsx_url(dataset_id, contains=contains)
    print(f"Resolved URL: {url}")

    print("Downloading XLSX (streaming)...")
    xlsx_bytes = download_excel_bytes(url)

    def_xml, rec_xml     = extract_pivot_xmls_in_memory(xlsx_bytes)
    fields, shared_items = parse_cache_definition_from_bytes(def_xml)
    print("Parsing records...")
    data                 = parse_cache_records_from_bytes(rec_xml, shared_items)
    expected_rows        = get_record_count(rec_xml)

    if len(data) != expected_rows:
        print(f"WARNING: Row count mismatch. Expected: {expected_rows}, Got: {len(data)}")
    else:
        print(f"Row count validated: {len(data)} rows")

    def clean_col_name(name):
        if name is None:
            name = "unknown"
        name = name.strip()
        name = re.sub(r"[\s\-\(\)\/]+", "_", name)   # spaces, brackets → underscore
        name = re.sub(r"[^a-zA-Z0-9_]", "", name)     # remove remaining special chars
        name = re.sub(r"_+", "_", name)               # collapse multiple underscores
        name = name.strip("_").lower()
        return name

    clean_fields = [clean_col_name(f) for f in fields]
    pdf = pd.DataFrame(data, columns=clean_fields)
    sdf = spark.createDataFrame(pdf)

    sdf = (sdf
           .withColumn("_ingestion_timestamp", current_timestamp())
           .withColumn("_source_url",          lit(url))
           .withColumn("_dataset_name",        lit(dataset_name))
           .withColumn("_date_partition",      lit(date_partition)))

    delta_path = f"{bronze_path}/{dataset_name}"
    (sdf.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(delta_path))

    print(f"Written to Bronze Delta: {delta_path}")
    return len(data)

# COMMAND ----------

# CELL 9 — Dataset Registry
DATASETS = [
    {"name": "student_visa_granted",
     "dataset_id": "324aa4f7-46bb-4d56-bc2d-772333a2317e",
     "contains": "granted"},

    {"name": "temporary_graduate_visa_lodged",
     "dataset_id": "c957d829-4f9b-4213-a0c2-8cbeb9a03ffb",
     "contains": "lodged"},

    {"name": "temporary_graduate_visa_granted",
     "dataset_id": "c957d829-4f9b-4213-a0c2-8cbeb9a03ffb",
     "contains": "granted"},

    {"name": "overseas_arrivals_and_departures",
     "dataset_id": "5a0ab398-c897-4ae3-986d-f94452a165d7",
     "contains": "departures"},

    {"name": "temporary_work_visa_granted_skilled",
     "dataset_id": "2515b21d-0dba-4810-afd4-ac8dd92e873e",
     "contains": "granted"},

    {"name": "temporary_work_visa_holders_skilled",
     "dataset_id": "2515b21d-0dba-4810-afd4-ac8dd92e873e",
     "contains": "holder"},

    {"name": "working_holiday",
     "dataset_id": "602f74a0-a588-4dea-ae28-0fe123cbb182",
     "contains": "working holiday"},

    {"name": "temporary_visa_holders",
     "dataset_id": "ab245863-4dea-4661-a334-71ee15937130",
     "contains": "holders"},

    {"name": "visitor_visas",
     "dataset_id": "903e4782-8d6d-438d-9c40-9883cf91606c",
     "contains": "visitor"},
]

# COMMAND ----------

# CELL 10 — Run ingestion for all 9 datasets
results = []
for d in DATASETS:
    try:
        row_count = ingest_dataset_to_bronze(
            dataset_name   = d["name"],
            dataset_id     = d["dataset_id"],
            contains       = d.get("contains"),
            bronze_path    = BRONZE_PATH,
            date_partition = DATE_PARTITION
        )
        results.append((d["name"], row_count, "OK"))
    except Exception as e:
        print(f"ERROR processing {d['name']}: {e}")
        results.append((d["name"], 0, f"ERROR: {e}"))

# COMMAND ----------

ingest_dataset_to_bronze(
    dataset_name   = "temporary_work_visa_granted_skilled",
    dataset_id     = "2515b21d-0dba-4810-afd4-ac8dd92e873e",
    contains       = "granted",
    bronze_path    = BRONZE_PATH,
    date_partition = DATE_PARTITION
)

# COMMAND ----------

# CELL 11 — Summary + Verification
print("\n" + "="*60)
print("BRONZE INGESTION SUMMARY")
print("="*60)
for name, rows, status in results:
    print(f"  {status:6} | {name:45} | {rows:>8} rows")

print("\nVerifying Delta tables in Bronze layer:")
for d in DATASETS:
    path = f"{BRONZE_PATH}/{d['name']}"
    try:
        df = spark.read.format("delta").load(path)
        print(f"  OK {d['name']}: {df.count()} total rows")
    except Exception as e:
        print(f"  FAIL {d['name']}: {e}")

print(f"\nBronze ingestion complete for partition: {DATE_PARTITION}")

# COMMAND ----------

BRONZE_PATH = "abfss://medallion@***adls.dfs.core.windows.net/bronze"

datasets = [
    "student_visa_granted",
    "temporary_graduate_visa_lodged",
    "temporary_graduate_visa_granted",
    "overseas_arrivals_and_departures",
    "temporary_work_visa_granted_skilled",
    "temporary_work_visa_holders_skilled",
    "working_holiday",
    "temporary_visa_holders",
    "visitor_visas",
]


print("Verifying Delta tables in Bronze layer:")
for name in datasets:
    path = f"{BRONZE_PATH}/{name}"
    try:
        df = spark.read.format("delta").load(path)
        print(f"  OK {name}: {df.count():,} total rows")
    except Exception as e:
        print(f"  FAIL {name}: {e}")