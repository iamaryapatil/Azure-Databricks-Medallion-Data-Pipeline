# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 2 — Silver Layer: Schema Standardisation & Data Quality
# MAGIC ***** Visa Analytics Pipeline**
# MAGIC - Reads all 9 Bronze Delta tables
# MAGIC - Standardises column names, data types, nulls
# MAGIC - Runs data quality checks
# MAGIC - Special pivot reshape for overseas_arrivals_and_departures
# MAGIC - Writes clean Delta tables to ADLS Silver layer

# COMMAND ----------

# CELL 1 — Imports
import re
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, to_date,
    when, lit, current_timestamp, regexp_replace,
    date_format
)
from pyspark.sql.types import StringType, IntegerType, DoubleType
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("date_partition", datetime.today().strftime("%Y%m%d"))
DATE_PARTITION = dbutils.widgets.get("date_partition")
print(f"Running Silver transforms for partition: {DATE_PARTITION}")



MOUNT_POINT = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
BRONZE_PATH = f"{MOUNT_POINT}/bronze"
SILVER_PATH = f"{MOUNT_POINT}/silver"
GOLD_PATH   = f"{MOUNT_POINT}/gold"

# COMMAND ----------

# CELL 3 — Helper Functions
def standardise_column_names(df):
    new_cols = []
    for c in df.columns:
        new = c.strip()
        new = re.sub(r"[\s\-\(\)\/]+", "_", new)
        new = re.sub(r"[^a-zA-Z0-9_]", "", new)
        new = re.sub(r"_+", "_", new)
        new = new.strip("_").lower()
        new_cols.append(new)
    return df.toDF(*new_cols)

def run_dq_checks(df, dataset_name, key_columns):
    print(f"\n  DQ Report: {dataset_name}")
    total = df.count()
    print(f"  Total rows: {total:,}")
    for col_name in key_columns:
        if col_name in df.columns:
            nulls  = df.filter(col(col_name).isNull() | (trim(col(col_name).cast(StringType())) == "")).count()
            pct    = round(nulls / total * 100, 2) if total > 0 else 0
            status = "OK" if nulls == 0 else "WARN"
            print(f"  {status} Nulls in '{col_name}': {nulls} ({pct}%)")
    valid_keys = [k for k in key_columns if k in df.columns]
    if valid_keys:
        dupes  = total - df.dropDuplicates(valid_keys).count()
        status = "OK" if dupes == 0 else "WARN"
        print(f"  {status} Duplicate rows: {dupes}")
    return df

def drop_bronze_metadata(df):
    meta = ["_ingestion_timestamp", "_source_url", "_dataset_name", "_date_partition"]
    return df.drop(*[c for c in meta if c in df.columns])

def write_silver(df, table_name, date_partition):
    df = (df
          .withColumn("_silver_timestamp", current_timestamp())
          .withColumn("_date_partition",   lit(date_partition)))
    path = f"{SILVER_PATH}/{table_name}"
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(path))
    print(f"  Written to Silver: {path} ({df.count():,} rows)")

# COMMAND ----------

# EXPLORE: student_visa_granted
df_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/student_visa_granted")
df_raw = drop_bronze_metadata(df_raw)
df_raw = standardise_column_names(df_raw)

# Schema
print("=== SCHEMA ===")
df_raw.printSchema()

# First 5 rows
print("\n=== FIRST 5 ROWS ===")
df_raw.show(5, truncate=False)

# Row count
print(f"\n=== ROW COUNT ===")
print(f"Total rows: {df_raw.count():,}")

# Null counts for every column
print("\n=== NULL COUNTS PER COLUMN ===")
from pyspark.sql.functions import count, when, isnan
null_counts = df_raw.select([
    count(when(col(c).isNull() | (trim(col(c).cast(StringType())) == ""), c)).alias(c)
    for c in df_raw.columns
])
null_counts.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, regexp_replace
# CELL 4 — Silver: student_visa_granted
print("--- student_visa_granted ---")

df = spark.read.format("delta").load(f"{BRONZE_PATH}/student_visa_granted")
df = drop_bronze_metadata(df)
df = standardise_column_names(df)

df = (df
      .withColumn("financial_year_of_visa_grant", trim(col("financial_year_of_visa_grant")))
      .withColumn("financial_year_quarter",regexp_extract(trim(col("financial_year_quarter")), r"(Q\d)", 1))
      .withColumn("month", trim(col("month")))
      .withColumn("client_location", trim(col("client_location")))
      .withColumn("lodgement_channel", trim(col("lodgement_channel")))
      .withColumn("sector", trim(col("sector")))
      .withColumn("applicant_type", trim(col("applicant_type")))
      .withColumn("education_provider_registered_state", upper(trim(col("education_provider_registered_state"))))
      .withColumn("gender", trim(col("gender")))
      .withColumn("citizenship_country", trim(col("citizenship_country")))
      .withColumnRenamed("age_group", "age_group_in_years")
      .withColumn("age_group_in_years",regexp_replace(col("age_group_in_years"), r"\s*to\s*", "-"))
      .withColumn("age_group_in_years",regexp_replace(col("age_group_in_years"), r"\s*years", ""))
      .withColumn("last_visa_held_visa_category", trim(col("last_visa_held_visa_category")))

      # visualisation-friendly helper columns
      .withColumnRenamed("total", "total_grants")
      .withColumn("total_grants", col("total_grants").cast(IntegerType()))
      .withColumn("month_number", regexp_extract(col("month"), r"M(\d{2})", 1).cast(IntegerType()))
      .withColumn("month_name", regexp_extract(col("month"), r"[A-Za-z]{3}$", 0))
)

# Preview transformed schema
print("\n=== TRANSFORMED SCHEMA ===")
df.printSchema()

# Preview sample rows
print("\n=== SAMPLE TRANSFORMED DATA ===")
display(df.limit(5))

run_dq_checks(
    df,
    "student_visa_granted",
    ["financial_year_of_visa_grant", "month", "citizenship_country"]
)

write_silver(df, "student_visa_granted", DATE_PARTITION)

# COMMAND ----------

print("--- Inspecting temporary_graduate_visa_lodged (Bronze) ---")

df_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/temporary_graduate_visa_lodged")
df_raw = drop_bronze_metadata(df_raw)
df_raw = standardise_column_names(df_raw)

print("\n=== SCHEMA ===")
df_raw.printSchema()

print("\n=== SAMPLE DATA ===")
df_raw.show(5, truncate=False)

print("\n=== ROW COUNT ===")
print(f"Total rows: {df_raw.count():,}")

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, regexp_replace

# CELL — Silver: temporary_graduate_visa_lodged
print("--- temporary_graduate_visa_lodged ---")

df = spark.read.format("delta").load(f"{BRONZE_PATH}/temporary_graduate_visa_lodged")
df = drop_bronze_metadata(df)
df = standardise_column_names(df)

df = (df
      .withColumn("financial_year_of_visa_lodged",
                  trim(col("financial_year_of_visa_lodged")))

      .withColumn("financial_year_quarter",
                  regexp_extract(trim(col("financial_year_quarter")), r"(Q\d)", 1))

      .withColumn("month", trim(col("month")))
      .withColumn("visa_subclass", trim(col("visa_subclass")))
      .withColumn("applicant_type", trim(col("applicant_type")))
      .withColumn("gender", trim(col("gender")))
      .withColumn("citizenship_country", trim(col("citizenship_country")))
      .withColumn("visa_type", trim(col("visa_type")))
      .withColumn("visa_sub_type", trim(col("visa_sub_type")))

      # rename measure column
      .withColumnRenamed("total", "total_lodged")
      .withColumn("total_lodged", col("total_lodged").cast(IntegerType()))

      # visualisation helpers
      .withColumn("month_number",
                  regexp_extract(col("month"), r"(\d{2})", 1).cast(IntegerType()))

      .withColumn("month_name",
                  regexp_extract(col("month"), r"[A-Z]{3}", 0))
)

# Preview transformed schema
print("\n=== TRANSFORMED SCHEMA ===")
df.printSchema()

# Preview sample rows
print("\n=== SAMPLE TRANSFORMED DATA ===")
display(df.limit(5))

run_dq_checks(
    df,
    "temporary_graduate_visa_lodged",
    ["financial_year_of_visa_lodged", "month", "citizenship_country"]
)

write_silver(df, "temporary_graduate_visa_lodged", DATE_PARTITION)

# COMMAND ----------

print("--- Inspecting temporary_graduate_visa_granted (Bronze) ---")

df_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/temporary_graduate_visa_granted")
df_raw = drop_bronze_metadata(df_raw)
df_raw = standardise_column_names(df_raw)

print("\n=== SCHEMA ===")
df_raw.printSchema()

print("\n=== SAMPLE DATA ===")
df_raw.show(5, truncate=False)

print("\n=== ROW COUNT ===")
print(f"Total rows: {df_raw.count():,}")

# COMMAND ----------

from pyspark.sql.functions import regexp_extract

# CELL — Silver: temporary_graduate_visa_granted
print("--- temporary_graduate_visa_granted ---")

df = spark.read.format("delta").load(f"{BRONZE_PATH}/temporary_graduate_visa_granted")
df = drop_bronze_metadata(df)
df = standardise_column_names(df)

df = (df
      .withColumn("financial_year_of_visa_grant",
                  trim(col("financial_year_of_visa_grant")))

      .withColumn("financial_year_quarter",
                  regexp_extract(trim(col("financial_year_quarter")), r"(Q\d)", 1))

      .withColumn("month", trim(col("month")))
      .withColumn("visa_subclass", trim(col("visa_subclass")))
      .withColumn("applicant_type", trim(col("applicant_type")))
      .withColumn("gender", trim(col("gender")))
      .withColumn("citizenship_country", trim(col("citizenship_country")))
      .withColumn("visa_type", trim(col("visa_type")))
      .withColumn("visa_sub_type", trim(col("visa_sub_type")))

      # rename measure column
      .withColumnRenamed("total", "total_granted")
      .withColumn("total_granted", col("total_granted").cast(IntegerType()))

      # visualisation helpers
      .withColumn("month_number",
                  regexp_extract(col("month"), r"(\d{2})", 1).cast(IntegerType()))

      .withColumn("month_name",
                  regexp_extract(col("month"), r"[A-Z]{3}", 0))
)

# Preview transformed schema
print("\n=== TRANSFORMED SCHEMA ===")
df.printSchema()

# Preview sample rows
print("\n=== SAMPLE TRANSFORMED DATA ===")
display(df.limit(5))

run_dq_checks(
    df,
    "temporary_graduate_visa_granted",
    ["financial_year_of_visa_grant", "month", "citizenship_country"]
)

write_silver(df, "temporary_graduate_visa_granted", DATE_PARTITION)

# COMMAND ----------

print("--- Inspecting overseas_arrivals_and_departuresanted (Bronze) ---")

df_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/overseas_arrivals_and_departures")
df_raw = drop_bronze_metadata(df_raw)
df_raw = standardise_column_names(df_raw)

print("\n=== SCHEMA ===")
df_raw.printSchema()

print("\n=== SAMPLE DATA ===")
df_raw.show(5, truncate=False)

print("\n=== ROW COUNT ===")
print(f"Total rows: {df_raw.count():,}")

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, regexp_replace, to_timestamp, to_date, date_format

# CELL — Silver: overseas_arrivals_and_departures
print("--- overseas_arrivals_and_departures ---")

df = spark.read.format("delta").load(f"{BRONZE_PATH}/overseas_arrivals_and_departures")
df = drop_bronze_metadata(df)
df = standardise_column_names(df)

df = (df
      .withColumn("financial_year", trim(col("financial_year")))

      # keep raw month string, but create a proper date field for visuals
      .withColumn("month", trim(col("month")))
      .withColumn("month_timestamp", to_timestamp(col("month"), "yyyy-MM-dd'T'HH:mm:ss"))
      .withColumn("month_date", to_date(col("month_timestamp")))

      .withColumn("travel_mode", trim(col("travel_mode")))
      .withColumn("move_direction", trim(col("move_direction")))
      .withColumnRenamed("intended_length_of_stay", "intended_length_of_stay_in_months")
      .withColumn("intended_length_of_stay_in_months",trim(col("intended_length_of_stay_in_months")))
      .withColumn("intended_length_of_stay_in_months",regexp_replace(col("intended_length_of_stay_in_months"), r"\s*MTHS?", ""))
      .withColumn("gender", trim(col("gender")))

      .withColumnRenamed("age_group", "age_group_in_years")
      .withColumn("age_group_in_years", trim(col("age_group_in_years")))
      .withColumn("age_group_in_years", regexp_replace(col("age_group_in_years"), r"\s*-\s*", "-"))

      .withColumn("country_of_citizenship", trim(col("country_of_citizenship")))
      .withColumn("country_of_dis_embarkation", trim(col("country_of_dis_embarkation")))
      .withColumn("country_of_intended_residence", trim(col("country_of_intended_residence")))
      .withColumn("state_territory_of_intended_residence", upper(trim(col("state_territory_of_intended_residence"))))
      .withColumn("main_reason_for_travel", trim(col("main_reason_for_travel")))
      .withColumn("category_of_traveller", trim(col("category_of_traveller")))

      .withColumn("movements_count", col("movements_count").cast(DoubleType()))

      # visualisation helpers
      .withColumn("month_number", regexp_extract(date_format(col("month_date"), "MM"), r"(\d{2})", 1).cast(IntegerType()))
      .withColumn("month_name", date_format(col("month_date"), "MMM"))
)

print("\n=== TRANSFORMED SCHEMA ===")
df.printSchema()

print("\n=== SAMPLE TRANSFORMED DATA ===")
display(df.limit(5))

run_dq_checks(
    df,
    "overseas_arrivals_and_departures",
    ["financial_year", "month_date", "category_of_traveller"]
)

write_silver(df, "overseas_arrivals_and_departures", DATE_PARTITION)

# COMMAND ----------

print("--- Inspecting temporary_work_visa_granted_skilled (Bronze) ---")

df_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/temporary_work_visa_granted_skilled")
df_raw = drop_bronze_metadata(df_raw)
df_raw = standardise_column_names(df_raw)

print("\n=== SCHEMA ===")
df_raw.printSchema()

print("\n=== SAMPLE DATA ===")
df_raw.show(5, truncate=False)

print("\n=== ROW COUNT ===")
print(f"Total rows: {df_raw.count():,}")

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, regexp_replace

# CELL — Silver: temporary_work_visa_granted_skilled
print("--- temporary_work_visa_granted_skilled ---")

df = spark.read.format("delta").load(f"{BRONZE_PATH}/temporary_work_visa_granted_skilled")
df = drop_bronze_metadata(df)
df = standardise_column_names(df)

df = (df
      .withColumn("financial_year_of_visa_grant",
                  trim(col("financial_year_of_visa_grant")))

      .withColumn("financial_year_quarter",
                  regexp_extract(trim(col("financial_year_quarter")), r"(Q\d)", 1))

      .withColumn("client_location",
                  trim(col("client_location")))

      .withColumn("visa_subclass",
                  trim(col("visa_subclass")))

      .withColumn("applicant_type",
                  trim(col("applicant_type")))

      .withColumn("visa_type",
                  trim(col("visa_type")))

      .withColumn("visa_sub_type",
                  trim(col("visa_sub_type")))

      .withColumn("nominated_position_location_state",
                  upper(trim(col("nominated_position_location_state"))))

      .withColumn("nominated_position_location_statistical_area_level_3",
                  trim(col("nominated_position_location_statistical_area_level_3")))

      .withColumn("nominated_position_location_statistical_area_level_4",
                  trim(col("nominated_position_location_statistical_area_level_4")))

      .withColumn("nominated_occupation_major_group",
                  trim(col("nominated_occupation_major_group")))

      .withColumn("nominated_occupation_unit_group",
                  trim(col("nominated_occupation_unit_group")))

      .withColumn("nominated_occupation",
                  trim(col("nominated_occupation")))

      .withColumn("nominated_occupation_skill_level",
                  trim(col("nominated_occupation_skill_level")))

      .withColumn("sponsor_industry",
                  trim(col("sponsor_industry")))

      .withColumn("gender",
                  trim(col("gender")))

      .withColumn("citizenship_country",
                  trim(col("citizenship_country")))

      # age group cleanup
      .withColumnRenamed("age_group", "age_group_in_years")

      .withColumn("age_group_in_years",
                  regexp_replace(col("age_group_in_years"), r"\s*-\s*", "-"))

      # measure column rename
      .withColumnRenamed("total", "total_granted")

      .withColumn("total_granted",
                  col("total_granted").cast(IntegerType()))
)

print("\n=== TRANSFORMED SCHEMA ===")
df.printSchema()

print("\n=== SAMPLE TRANSFORMED DATA ===")
display(df.limit(5))

run_dq_checks(
    df,
    "temporary_work_visa_granted_skilled",
    ["financial_year_of_visa_grant",
     "citizenship_country",
     "nominated_occupation"]
)

write_silver(df, "temporary_work_visa_granted_skilled", DATE_PARTITION)

# COMMAND ----------

print("--- Inspecting temporary_work_visa_holders_skilled (Bronze) ---")

df_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/temporary_work_visa_holders_skilled")
df_raw = drop_bronze_metadata(df_raw)
df_raw = standardise_column_names(df_raw)

print("\n=== SCHEMA ===")
df_raw.printSchema()

print("\n=== SAMPLE DATA ===")
df_raw.show(5, truncate=False)

print("\n=== ROW COUNT ===")
print(f"Total rows: {df_raw.count():,}")

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, to_timestamp, to_date, year, month

# CELL — Silver: temporary_work_visa_holders_skilled
print("--- temporary_work_visa_holders_skilled ---")

df = spark.read.format("delta").load(f"{BRONZE_PATH}/temporary_work_visa_holders_skilled")
df = drop_bronze_metadata(df)
df = standardise_column_names(df)

df = (df
      # snapshot date cleanup
      .withColumn("snapshot_date_timestamp",
                  to_timestamp(col("snapshot_date"),
                               "yyyy-MM-dd'T'HH:mm:ss"))

      .withColumn("snapshot_date",
                  to_date(col("snapshot_date_timestamp")))

      .withColumn("snapshot_year",
                  year(col("snapshot_date")))

      .withColumn("snapshot_month",
                  month(col("snapshot_date")))

      .withColumn("applicant_type",
                  trim(col("applicant_type")))

      .withColumn("visa_type",
                  trim(col("visa_type")))

      .withColumn("visa_sub_type",
                  trim(col("visa_sub_type")))

      .withColumn("nominated_position_location_state",
                  upper(trim(col("nominated_position_location_state"))))

      .withColumn("nominated_position_location_statistical_area_level_3",
                  trim(col("nominated_position_location_statistical_area_level_3")))

      .withColumn("nominated_position_location_statistical_area_level_4",
                  trim(col("nominated_position_location_statistical_area_level_4")))

      .withColumn("nominated_occupation_major_group",
                  trim(col("nominated_occupation_major_group")))

      .withColumn("nominated_occupation_unit_group",
                  trim(col("nominated_occupation_unit_group")))

      .withColumn("nominated_occupation",
                  trim(col("nominated_occupation")))

      .withColumn("nominated_occupation_skill_level",
                  trim(col("nominated_occupation_skill_level")))

      .withColumn("sponsor_industry",
                  trim(col("sponsor_industry")))

      .withColumn("gender",
                  trim(col("gender")))

      .withColumn("citizenship_country",
                  trim(col("citizenship_country")))

      # age group cleanup
      .withColumnRenamed("age_group", "age_group_in_years")

      .withColumn("age_group_in_years",
                  regexp_replace(col("age_group_in_years"),
                                 r"\s*-\s*", "-"))

      .withColumn("visa_subclass",
                  trim(col("visa_subclass")))

      # measure column rename
      .withColumnRenamed("total", "total_holders")

      .withColumn("total_holders",
                  col("total_holders").cast(IntegerType()))
)

print("\n=== TRANSFORMED SCHEMA ===")
df.printSchema()

print("\n=== SAMPLE TRANSFORMED DATA ===")
display(df.limit(5))

run_dq_checks(
    df,
    "temporary_work_visa_holders_skilled",
    ["snapshot_date",
     "citizenship_country",
     "nominated_occupation"]
)

write_silver(df, "temporary_work_visa_holders_skilled", DATE_PARTITION)

# COMMAND ----------

print("--- Inspecting working_holiday (Bronze) ---")

df_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/working_holiday")
df_raw = drop_bronze_metadata(df_raw)
df_raw = standardise_column_names(df_raw)

print("\n=== SCHEMA ===")
df_raw.printSchema()

print("\n=== SAMPLE DATA ===")
df_raw.show(5, truncate=False)

print("\n=== ROW COUNT ===")
print(f"Total rows: {df_raw.count():,}")

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, regexp_replace

# CELL — Silver: working_holiday
print("--- working_holiday ---")

df = spark.read.format("delta").load(f"{BRONZE_PATH}/working_holiday")
df = drop_bronze_metadata(df)
df = standardise_column_names(df)

df = (df
      .withColumn("financial_year_of_visa_grant",
                  trim(col("financial_year_of_visa_grant")))

      .withColumn("financial_year_quarter",
                  regexp_extract(trim(col("financial_year_quarter")), r"(Q\d)", 1))

      .withColumn("visa_subclass",
                  trim(col("visa_subclass")))

      .withColumn("gender",
                  trim(col("gender")))

      # age cleanup
      .withColumnRenamed("age_at_lodgement", "age_at_lodgement_in_years")

      .withColumn("age_at_lodgement_in_years",
                  regexp_replace(col("age_at_lodgement_in_years"),
                                 r"\s*years", ""))

      .withColumn("age_at_lodgement_in_years",
                  col("age_at_lodgement_in_years").cast(IntegerType()))

      .withColumn("citizenship_country",
                  trim(col("citizenship_country")))

      .withColumn("visa_type",
                  trim(col("visa_type")))

      # measure column rename
      .withColumnRenamed("total", "total_grants")

      .withColumn("total_grants",
                  col("total_grants").cast(IntegerType()))
)

print("\n=== TRANSFORMED SCHEMA ===")
df.printSchema()

print("\n=== SAMPLE TRANSFORMED DATA ===")
display(df.limit(5))

run_dq_checks(
    df,
    "working_holiday",
    ["financial_year_of_visa_grant",
     "citizenship_country",
     "visa_type"]
)

write_silver(df, "working_holiday", DATE_PARTITION)

# COMMAND ----------

print("--- Inspecting temporary_visa_holders (Bronze) ---")

df_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/temporary_visa_holders")
df_raw = drop_bronze_metadata(df_raw)
df_raw = standardise_column_names(df_raw)

print("\n=== SCHEMA ===")
df_raw.printSchema()

print("\n=== SAMPLE DATA ===")
df_raw.show(5, truncate=False)

print("\n=== ROW COUNT ===")
print(f"Total rows: {df_raw.count():,}")

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, to_date, year, month

# CELL — Silver: temporary_visa_holders
print("--- temporary_visa_holders ---")

df = spark.read.format("delta").load(f"{BRONZE_PATH}/temporary_visa_holders")
df = drop_bronze_metadata(df)
df = standardise_column_names(df)

df = (df
      # snapshot date cleanup
      .withColumn("snapshot_date_timestamp",
                  to_timestamp(col("snapshot_date"),
                               "yyyy-MM-dd'T'HH:mm:ss"))

      .withColumn("snapshot_date",
                  to_date(col("snapshot_date_timestamp")))

      .withColumn("snapshot_year",
                  year(col("snapshot_date")))

      .withColumn("snapshot_month",
                  month(col("snapshot_date")))

      .withColumn("visa_category",
                  trim(col("visa_category")))

      .withColumn("visa_subclass",
                  trim(col("visa_subclass")))

      .withColumn("visa_type",
                  trim(col("visa_type")))

      .withColumn("applicant_type",
                  trim(col("applicant_type")))

      .withColumn("citizenship_country",
                  trim(col("citizenship_country")))

      .withColumn("visa_holders",
                  col("visa_holders").cast(IntegerType()))
)

print("\n=== TRANSFORMED SCHEMA ===")
df.printSchema()

print("\n=== SAMPLE TRANSFORMED DATA ===")
display(df.limit(5))

run_dq_checks(
    df,
    "temporary_visa_holders",
    ["snapshot_date", "visa_category", "citizenship_country"]
)

write_silver(df, "temporary_visa_holders", DATE_PARTITION)

# COMMAND ----------

print("--- Inspecting visitor_visas (Bronze) ---")

df_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/visitor_visas")
df_raw = drop_bronze_metadata(df_raw)
df_raw = standardise_column_names(df_raw)

print("\n=== SCHEMA ===")
df_raw.printSchema()

print("\n=== SAMPLE DATA ===")
df_raw.show(5, truncate=False)

print("\n=== ROW COUNT ===")
print(f"Total rows: {df_raw.count():,}")

# COMMAND ----------

from pyspark.sql.functions import regexp_extract

# CELL — Silver: visitor_visas
print("--- visitor_visas ---")

df = spark.read.format("delta").load(f"{BRONZE_PATH}/visitor_visas")
df = drop_bronze_metadata(df)
df = standardise_column_names(df)

df = (df
      .withColumn("financial_year_of_visa_grant",trim(col("financial_year_of_visa_grant")))
      .withColumn("financial_year_quarter",regexp_extract(trim(col("financial_year_quarter")), r"(Q\d)", 1))
      .withColumn("month",trim(col("month")))
      .withColumn("visa_subclass",trim(col("visa_subclass")))
      .withColumn("visa_type",trim(col("visa_type")))
      .withColumn("visa_group",trim(col("visa_group")))
      .withColumn("client_location",trim(col("client_location")))
      .withColumn("citizenship_country",trim(col("citizenship_country")))

      # rename measure column
      .withColumnRenamed("total", "total_grants")
      .withColumn("total_grants",col("total_grants").cast(IntegerType()))

      # visualisation helpers
      .withColumn("month_number",regexp_extract(col("month"), r"(\d{2})", 1).cast(IntegerType()))
      .withColumn("month_name",regexp_extract(col("month"), r"[A-Z]{3}", 0)))

print("\n=== TRANSFORMED SCHEMA ===")
df.printSchema()

print("\n=== SAMPLE TRANSFORMED DATA ===")
display(df.limit(5))

run_dq_checks(
    df,
    "visitor_visas",
    ["financial_year_of_visa_grant", "month", "citizenship_country"]
)

write_silver(df, "visitor_visas", DATE_PARTITION)

# COMMAND ----------

# CELL 13 — Silver Layer Verification
SILVER_TABLES = [
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

print("\n" + "="*60)
print("SILVER LAYER VERIFICATION")
print("="*60)
for table in SILVER_TABLES:
    path = f"{SILVER_PATH}/{table}"
    try:
        df = spark.read.format("delta").load(path)
        print(f"  OK {table}: {df.count():,} rows, {len(df.columns)} columns")
    except Exception as e:
        print(f"  FAIL {table}: {e}")

print(f"\nSilver transforms complete for partition: {DATE_PARTITION}")