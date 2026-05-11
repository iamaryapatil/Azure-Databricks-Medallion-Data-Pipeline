# Databricks notebook source


MOUNT_POINT = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
BRONZE_PATH = f"{MOUNT_POINT}/bronze"
SILVER_PATH = f"{MOUNT_POINT}/silver"
GOLD_PATH   = f"{MOUNT_POINT}/gold"

# COMMAND ----------

spark.sql("SHOW CATALOGS").display()

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS demo_workspace.bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS demo_workspace.silver")
spark.sql("CREATE DATABASE IF NOT EXISTS demo_workspace.gold")
print("Databases created in demo_workspace catalog.")

# COMMAND ----------

spark.sql("SHOW EXTERNAL LOCATIONS").display()

# COMMAND ----------

CATALOG_LOCATION = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/catalog"

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS ***
    MANAGED LOCATION '{CATALOG_LOCATION}'
""")
spark.sql("CREATE DATABASE IF NOT EXISTS ***.bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS ***.silver")
spark.sql("CREATE DATABASE IF NOT EXISTS ***.gold")
print("Catalog and databases created.")

# COMMAND ----------

bronze_tables = [
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

for table in bronze_tables:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS ***.bronze.{table}
        USING DELTA
        LOCATION '{BRONZE_PATH}/{table}'
    """)
    print(f"Registered: ***.bronze.{table}")

# COMMAND ----------

silver_tables = [
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

for table in silver_tables:
    path = f"{SILVER_PATH}/{table}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS ***.silver.{table}
        USING DELTA
        LOCATION '{path}'
    """)
    print(f"  Registered: ***.silver.{table}")

# COMMAND ----------

gold_tables = [
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

for table in gold_tables:
    path = f"{GOLD_PATH}/{table}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS ***.gold.{table}
        USING DELTA
        LOCATION '{path}'
    """)
    print(f"  Registered: ***.gold.{table}")