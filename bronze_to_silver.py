# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("landing_timestamp", "")
dbutils.widgets.text("table_name", "")

dbutils.widgets.text("load_type", "")
dbutils.widgets.text("merge_keys", "")

landing_timestamp = dbutils.widgets.get("landing_timestamp")
table_name = dbutils.widgets.get("table_name")
load_type = dbutils.widgets.get("load_type")
merge_keys = dbutils.widgets.get("merge_keys")

bronze_df = spark.read.table(f"adb_caresync.bronze.{table_name}").filter(f"landing_timestamp = '{landing_timestamp}'")
updated_df = (
    bronze_df.withColumn("insert_timestamp", F.current_timestamp())
    .withColumn("update_timestamp", F.current_timestamp())
)

# COMMAND ----------

# DBTITLE 1,Write to Silver
from delta.tables import DeltaTable

silver_table = f"adb_caresync.silver.{table_name}"

print(f"Step 1: Parameters loaded")
print(f"  Table Name   : {table_name}")
print(f"  Silver Table : {silver_table}")
print(f"  Load Type    : {load_type}")
print(f"  Merge Keys   : {merge_keys}")
print(f"  Source Rows  : {updated_df.count()}")
print("-" * 50)

if load_type == "FULL":
    print(f"Step 2: Performing FULL load (overwrite) into {silver_table}")
    updated_df.write.mode("overwrite").saveAsTable(silver_table)
    print(f"Step 3: FULL load completed successfully")

elif load_type == "MERGE":
    keys = [k.strip() for k in merge_keys.split(",")]
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in keys])
    print(f"Step 2: Performing MERGE (upsert) into {silver_table}")
    print(f"  Merge Keys     : {keys}")
    print(f"  Merge Condition : {merge_condition}")

    if spark.catalog.tableExists(silver_table):
        print(f"Step 3: Target table exists — executing Delta merge")
        delta_table = DeltaTable.forName(spark, silver_table)
        delta_table.alias("target").merge(
            updated_df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll(
        ).whenNotMatchedInsertAll(
        ).execute()
        print(f"Step 4: MERGE completed successfully")
    else:
        print(f"Step 3: Target table does not exist — bootstrapping with full overwrite")
        updated_df.write.mode("overwrite").saveAsTable(silver_table)
        print(f"Step 4: Initial load completed successfully")

elif load_type == "APPEND":
    print(f"Step 2: Performing APPEND load into {silver_table}")
    updated_df.write.mode("append").saveAsTable(silver_table)
    print(f"Step 3: APPEND load completed successfully")

else:
    raise ValueError(f"Unsupported load_type: {load_type}")

print("-" * 50)
print(f"Write to silver completed for table: {silver_table}")

# COMMAND ----------

record_count = updated_df.count()
dbutils.notebook.exit(str(record_count))