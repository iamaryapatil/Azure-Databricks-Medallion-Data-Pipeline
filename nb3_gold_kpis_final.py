# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 3 — Gold Layer: KPI Aggregations
# MAGIC ***** Visa Analytics Pipeline**
# MAGIC - Reads all 9 Silver Delta tables
# MAGIC - Produces analytics-ready KPI tables for visualisation
# MAGIC - Column names match exactly from Notebook 2 Silver transforms

# COMMAND ----------

# CELL 1 — Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, round, max, min,
    countDistinct, lit, current_timestamp,
    date_format, when, desc
)
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# CELL 2 — Parameters & Storage Config
dbutils.widgets.text("date_partition", datetime.today().strftime("%Y%m%d"))
DATE_PARTITION = dbutils.widgets.get("date_partition")
print(f"Running Gold aggregations for partition: {DATE_PARTITION}")


MOUNT_POINT = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
SILVER_PATH = f"{MOUNT_POINT}/silver"
GOLD_PATH   = f"{MOUNT_POINT}/gold"

# COMMAND ----------

# CELL 3 — Helper: write Gold table
def write_gold(df, table_name, date_partition):
    df = (df
          .withColumn("_gold_timestamp", current_timestamp())
          .withColumn("_date_partition", lit(date_partition)))
    path = f"{GOLD_PATH}/{table_name}"
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(path))
    print(f"  Written: {table_name} ({df.count():,} rows)")

# COMMAND ----------

# CELL 4 — Load Silver tables and register as Spark SQL temp views
tables = [
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

for t in tables:
    spark.read.format("delta").load(f"{SILVER_PATH}/{t}").createOrReplaceTempView(t)
    print(f"  Loaded: {t}")

print("\nAll Silver tables registered as temp views.")

# COMMAND ----------

# CELL 5 — Gold: Student Visa Grants by Financial Year & Sector
# KPI: Which sectors and countries drive student visa volumes over time
print("\nBuilding: student_visa_by_sector_and_country")

student_sector = spark.sql("""
    SELECT
        financial_year_of_visa_grant,
        financial_year_quarter,
        sector,
        citizenship_country,
        gender,
        age_group_in_years,
        SUM(total_grants)                   AS total_grants,
        COUNT(*)                            AS record_count
    FROM student_visa_granted
    WHERE financial_year_of_visa_grant IS NOT NULL
    GROUP BY
        financial_year_of_visa_grant,
        financial_year_quarter,
        sector,
        citizenship_country,
        gender,
        age_group_in_years
    ORDER BY financial_year_of_visa_grant DESC, total_grants DESC
""")

write_gold(student_sector, "student_visa_by_sector_and_country", DATE_PARTITION)

# COMMAND ----------

# CELL 6 — Gold: Student Visa Top Nationalities Summary
# KPI: Which countries send the most students overall
print("\nBuilding: student_visa_top_nationalities")

student_nationalities = spark.sql("""
    SELECT
        citizenship_country,
        SUM(total_grants)                                   AS total_grants,
        COUNT(DISTINCT financial_year_of_visa_grant)        AS financial_years_active,
        ROUND(AVG(total_grants), 1)                         AS avg_grants_per_record,
        MAX(total_grants)                                   AS peak_grants
    FROM student_visa_granted
    WHERE citizenship_country IS NOT NULL
      AND citizenship_country != ''
    GROUP BY citizenship_country
    ORDER BY total_grants DESC
""")

write_gold(student_nationalities, "student_visa_top_nationalities", DATE_PARTITION)

# COMMAND ----------

# CELL 7 — Gold: Graduate Visa Pipeline (lodged vs granted)
# KPI: Conversion from lodgement to grant by nationality
print("\nBuilding: graduate_visa_pipeline")

graduate_pipeline = spark.sql("""
    SELECT
        COALESCE(l.financial_year, g.financial_year)       AS financial_year,
        COALESCE(l.citizenship_country, g.citizenship_country) AS citizenship_country,
        COALESCE(l.visa_subclass, g.visa_subclass)         AS visa_subclass,
        COALESCE(l.total_lodged, 0)                        AS total_lodged,
        COALESCE(g.total_granted, 0)                       AS total_granted,
        ROUND(
            COALESCE(g.total_granted, 0) * 100.0
            / NULLIF(COALESCE(l.total_lodged, 0), 0), 2
        )                                                  AS grant_rate_pct
    FROM (
        SELECT financial_year_of_visa_lodged   AS financial_year,
               citizenship_country,
               visa_subclass,
               SUM(total_lodged)               AS total_lodged
        FROM temporary_graduate_visa_lodged
        GROUP BY financial_year_of_visa_lodged, citizenship_country, visa_subclass
    ) l
    FULL OUTER JOIN (
        SELECT financial_year_of_visa_grant    AS financial_year,
               citizenship_country,
               visa_subclass,
               SUM(total_granted)              AS total_granted
        FROM temporary_graduate_visa_granted
        GROUP BY financial_year_of_visa_grant, citizenship_country, visa_subclass
    ) g
    ON  l.financial_year       = g.financial_year
    AND l.citizenship_country  = g.citizenship_country
    AND l.visa_subclass        = g.visa_subclass
    ORDER BY financial_year DESC, total_lodged DESC
""")

write_gold(graduate_pipeline, "graduate_visa_pipeline", DATE_PARTITION)

# COMMAND ----------

# CELL 8 — Gold: Overseas Arrivals & Departures by Direction and Category
# KPI: Travel movement volumes by direction, category and reason
print("\nBuilding: overseas_movements_summary")

movements = spark.sql("""
    SELECT
        financial_year,
        move_direction,
        travel_mode,
        category_of_traveller,
        main_reason_for_travel,
        country_of_citizenship,
        country_of_intended_residence,
        SUM(movements_count)                AS total_movements,
        COUNT(*)                            AS record_count
    FROM overseas_arrivals_and_departures
    WHERE financial_year IS NOT NULL
    GROUP BY
        financial_year,
        move_direction,
        travel_mode,
        category_of_traveller,
        main_reason_for_travel,
        country_of_citizenship,
        country_of_intended_residence
    ORDER BY financial_year DESC, total_movements DESC
""")

write_gold(movements, "overseas_movements_summary", DATE_PARTITION)

# COMMAND ----------

# CELL 9 — Gold: Overseas Movements Monthly Trend
# KPI: Monthly travel volumes for trend analysis in Tableau
print("\nBuilding: overseas_movements_monthly_trend")

movements_monthly = spark.sql("""
    SELECT
        month_date,
        month_name,
        month_number,
        move_direction,
        category_of_traveller,
        SUM(movements_count)                AS total_movements
    FROM overseas_arrivals_and_departures
    WHERE month_date IS NOT NULL
    GROUP BY
        month_date,
        month_name,
        month_number,
        move_direction,
        category_of_traveller
    ORDER BY month_date DESC
""")

write_gold(movements_monthly, "overseas_movements_monthly_trend", DATE_PARTITION)

# COMMAND ----------

# CELL 10 — Gold: Skilled Work Visa Grants by Occupation & Country
# KPI: Which occupations and nationalities dominate skilled work visas
print("\nBuilding: work_visa_grants_by_occupation")

work_grants = spark.sql("""
    SELECT
        financial_year_of_visa_grant,
        financial_year_quarter,
        visa_subclass,
        visa_type,
        nominated_occupation_major_group,
        nominated_occupation,
        nominated_occupation_skill_level,
        nominated_position_location_state,
        sponsor_industry,
        citizenship_country,
        gender,
        age_group_in_years,
        SUM(total_granted)                  AS total_granted
    FROM temporary_work_visa_granted_skilled
    WHERE financial_year_of_visa_grant IS NOT NULL
    GROUP BY
        financial_year_of_visa_grant,
        financial_year_quarter,
        visa_subclass,
        visa_type,
        nominated_occupation_major_group,
        nominated_occupation,
        nominated_occupation_skill_level,
        nominated_position_location_state,
        sponsor_industry,
        citizenship_country,
        gender,
        age_group_in_years
    ORDER BY financial_year_of_visa_grant DESC, total_granted DESC
""")

write_gold(work_grants, "work_visa_grants_by_occupation", DATE_PARTITION)

# COMMAND ----------

# CELL 11 — Gold: Skilled Work Visa Holders Stock
# KPI: Current holder stock by occupation, state, and nationality
print("\nBuilding: work_visa_holders_summary")

work_holders = spark.sql("""
    SELECT
        snapshot_date,
        snapshot_year,
        snapshot_month,
        visa_subclass,
        visa_type,
        nominated_occupation_major_group,
        nominated_occupation,
        nominated_position_location_state,
        citizenship_country,
        age_group_in_years,
        SUM(total_holders)                  AS total_holders
    FROM temporary_work_visa_holders_skilled
    WHERE snapshot_date IS NOT NULL
    GROUP BY
        snapshot_date,
        snapshot_year,
        snapshot_month,
        visa_subclass,
        visa_type,
        nominated_occupation_major_group,
        nominated_occupation,
        nominated_position_location_state,
        citizenship_country,
        age_group_in_years
    ORDER BY snapshot_date DESC, total_holders DESC
""")

write_gold(work_holders, "work_visa_holders_summary", DATE_PARTITION)

# COMMAND ----------

# CELL 12 — Gold: Working Holiday Visa Grants by Country & Age
# KPI: Working holiday volumes by nationality and age
print("\nBuilding: working_holiday_summary")

wh_summary = spark.sql("""
    SELECT
        financial_year_of_visa_grant,
        financial_year_quarter,
        visa_subclass,
        visa_type,
        citizenship_country,
        gender,
        age_at_lodgement_in_years,
        SUM(total_grants)                   AS total_grants
    FROM working_holiday
    WHERE financial_year_of_visa_grant IS NOT NULL
    GROUP BY
        financial_year_of_visa_grant,
        financial_year_quarter,
        visa_subclass,
        visa_type,
        citizenship_country,
        gender,
        age_at_lodgement_in_years
    ORDER BY financial_year_of_visa_grant DESC, total_grants DESC
""")

write_gold(wh_summary, "working_holiday_summary", DATE_PARTITION)

# COMMAND ----------

# CELL 13 — Gold: Temporary Visa Holders Stock by Category
# KPI: Holder stock over time by visa category and nationality
print("\nBuilding: temporary_visa_holders_summary")

temp_holders = spark.sql("""
    SELECT
        snapshot_date,
        snapshot_year,
        snapshot_month,
        visa_category,
        visa_subclass,
        visa_type,
        applicant_type,
        citizenship_country,
        SUM(visa_holders)                   AS total_holders
    FROM temporary_visa_holders
    WHERE snapshot_date IS NOT NULL
    GROUP BY
        snapshot_date,
        snapshot_year,
        snapshot_month,
        visa_category,
        visa_subclass,
        visa_type,
        applicant_type,
        citizenship_country
    ORDER BY snapshot_date DESC, total_holders DESC
""")

write_gold(temp_holders, "temporary_visa_holders_summary", DATE_PARTITION)

# COMMAND ----------

# CELL 14 — Gold: Visitor Visa Grants by Country & Subclass
# KPI: Visitor visa volumes by nationality and visa type
print("\nBuilding: visitor_visa_summary")

visitor_summary = spark.sql("""
    SELECT
        financial_year_of_visa_grant,
        financial_year_quarter,
        month,
        month_number,
        month_name,
        visa_subclass,
        visa_type,
        visa_group,
        client_location,
        citizenship_country,
        SUM(total_grants)                   AS total_grants
    FROM visitor_visas
    WHERE financial_year_of_visa_grant IS NOT NULL
    GROUP BY
        financial_year_of_visa_grant,
        financial_year_quarter,
        month,
        month_number,
        month_name,
        visa_subclass,
        visa_type,
        visa_group,
        client_location,
        citizenship_country
    ORDER BY financial_year_of_visa_grant DESC, total_grants DESC
""")

write_gold(visitor_summary, "visitor_visa_summary", DATE_PARTITION)

# COMMAND ----------

# CELL 15 — Gold: Executive KPI Summary
# KPI: Top-line numbers across all visa types for executive dashboard
print("\nBuilding: executive_kpi_summary")

exec_kpi = spark.sql("""
    SELECT 'Student Visa'              AS visa_category,
           SUM(total_grants)           AS total_volume,
           COUNT(DISTINCT citizenship_country) AS distinct_nationalities,
           MIN(financial_year_of_visa_grant)   AS earliest_year,
           MAX(financial_year_of_visa_grant)   AS latest_year
    FROM student_visa_granted

    UNION ALL

    SELECT 'Graduate Visa Lodged',
           SUM(total_lodged),
           COUNT(DISTINCT citizenship_country),
           MIN(financial_year_of_visa_lodged),
           MAX(financial_year_of_visa_lodged)
    FROM temporary_graduate_visa_lodged

    UNION ALL

    SELECT 'Graduate Visa Granted',
           SUM(total_granted),
           COUNT(DISTINCT citizenship_country),
           MIN(financial_year_of_visa_grant),
           MAX(financial_year_of_visa_grant)
    FROM temporary_graduate_visa_granted

    UNION ALL

    SELECT 'Skilled Work Visa Granted',
           SUM(total_granted),
           COUNT(DISTINCT citizenship_country),
           MIN(financial_year_of_visa_grant),
           MAX(financial_year_of_visa_grant)
    FROM temporary_work_visa_granted_skilled

    UNION ALL

    SELECT 'Working Holiday Visa',
           SUM(total_grants),
           COUNT(DISTINCT citizenship_country),
           MIN(financial_year_of_visa_grant),
           MAX(financial_year_of_visa_grant)
    FROM working_holiday

    UNION ALL

    SELECT 'Visitor Visa',
           SUM(total_grants),
           COUNT(DISTINCT citizenship_country),
           MIN(financial_year_of_visa_grant),
           MAX(financial_year_of_visa_grant)
    FROM visitor_visas

    ORDER BY total_volume DESC
""")

write_gold(exec_kpi, "executive_kpi_summary", DATE_PARTITION)

# COMMAND ----------

# CELL 16 — Gold Layer Verification
GOLD_TABLES = [
    "student_visa_by_sector_and_country",
    "student_visa_top_nationalities",
    "graduate_visa_pipeline",
    "overseas_movements_summary",
    "overseas_movements_monthly_trend",
    "work_visa_grants_by_occupation",
    "work_visa_holders_summary",
    "working_holiday_summary",
    "temporary_visa_holders_summary",
    "visitor_visa_summary",
    "executive_kpi_summary",
]

print("\n" + "="*60)
print("GOLD LAYER VERIFICATION")
print("="*60)
for table in GOLD_TABLES:
    path = f"{GOLD_PATH}/{table}"
    try:
        df = spark.read.format("delta").load(path)
        print(f"  OK {table}: {df.count():,} rows")
    except Exception as e:
        print(f"  FAIL {table}: {e}")

print(f"\nGold aggregations complete for partition: {DATE_PARTITION}")
print("Full pipeline execution finished.")
print("Bronze -> Silver -> Gold: DONE")