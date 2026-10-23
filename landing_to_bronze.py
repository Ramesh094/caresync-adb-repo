# Databricks notebook source
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("landing_timestamp", "")
dbutils.widgets.text("source_file_format", "")


source_system = dbutils.widgets.get("source_system")
table_name = dbutils.widgets.get("table_name")
landing_timestamp = dbutils.widgets.get("landing_timestamp")
source_file_format = dbutils.widgets.get("source_file_format")
# landing_df = spark.read.format("parquet").load(f"/Volumes/adb_caresync/landing/landing_volume/{source_system}/{table_name}/{landing_timestamp}/")



# COMMAND ----------

base_landing_path = "/Volumes/adb_caresync/landing/landing_volume"
source_path = f"{base_landing_path}/{source_system}"
table_path = f"{source_path}/{table_name}"
landing_path = f"{table_path}/{landing_timestamp}/"


def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


missing_path = next((path for path in [source_path, table_path, landing_path] if not path_exists(path)), None)

if missing_path:
    message = (
        f"No new files are received for source_system={source_system}, "
        f"table_name={table_name}, landing_timestamp={landing_timestamp}."
    )
    print(message)
    dbutils.notebook.exit(message)

# COMMAND ----------

if source_file_format == "csv":
    landing_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(landing_path)
else:
    landing_df = spark.read.format("parquet").load(landing_path)

print(f"Step 1: Parameters loaded")
print(f"  Source System       : {source_system}")
print(f"  Table Name          : {table_name}")
print(f"  Landing Timestamp   : {landing_timestamp}")
print(f"  Source Rows         : {landing_df.count()}")
print("-" * 50)

# COMMAND ----------

from pyspark.sql import functions as F

updated_df = (landing_df.withColumn("landing_timestamp", F.lit(landing_timestamp))
              .withColumn("insert_timestamp", F.current_timestamp())
              )

print(f"Step 2: Columns added — landing_timestamp, insert_timestamp")

# COMMAND ----------

bronze_table = f"adb_caresync.bronze.{table_name}"
print(f"Step 3: Performing APPEND load into {bronze_table}")
updated_df.write.mode("append").saveAsTable(bronze_table)
print(f"Step 4: APPEND load completed successfully")
print("-" * 50)
print(f"Write to bronze completed for table: {bronze_table}")