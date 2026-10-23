# Databricks notebook source
# DBTITLE 1,STTM Header
# MAGIC %md
# MAGIC # NB_patient_360
# MAGIC **Target:** `adb_caresync.gold.patient_360` | **Grain:** One row per patient | **Load:** FULL overwrite
# MAGIC
# MAGIC **Driving Table:** `silver.patients` | **Joins:** LEFT JOIN hospitals, insurance_providers, aggregated lab_results

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# COMMAND ----------

# DBTITLE 1,Read Source Tables
patients = spark.table("adb_caresync.silver.patients")
hospitals = spark.table("adb_caresync.silver.hospitals")
insurance_providers = spark.table("adb_caresync.silver.insurance_providers")
lab_results = spark.table("adb_caresync.silver.lab_results")

# COMMAND ----------

# DBTITLE 1,Aggregate Lab Results per Patient
lab_agg = (
    lab_results
    .groupBy("patient_id")
    .agg(
        F.count("*").alias("total_tests"),
        F.count(F.when(F.col("result_status") == "Abnormal", 1)).alias("abnormal_test_count"),
        F.count(F.when(F.col("result_status") == "Critical", 1)).alias("critical_test_count"),
        F.max("test_date").alias("last_test_date"),
    )
)

# COMMAND ----------

# DBTITLE 1,Build patient_360 with LEFT JOINs
patient_360 = (
    patients.alias("p")
    .join(hospitals.alias("h"), F.col("p.registered_hospital_id") == F.col("h.hospital_id"), "left")
    .join(insurance_providers.alias("ip"), F.col("p.insurance_provider_id") == F.col("ip.insurance_provider_id"), "left")
    .join(lab_agg.alias("la"), F.col("p.patient_id") == F.col("la.patient_id"), "left")
    .select(
        F.col("p.patient_id"),
        F.col("p.first_name"),
        F.col("p.last_name"),
        F.col("p.date_of_birth").cast("date").alias("date_of_birth"),
        F.floor(F.datediff(F.current_date(), F.col("p.date_of_birth")) / 365.25).cast(IntegerType()).alias("age"),
        F.col("p.gender"),
        F.col("p.city"),
        F.col("p.state"),
        F.col("p.registered_hospital_id").alias("hospital_id"),
        F.col("h.hospital_name"),
        F.col("p.insurance_provider_id"),
        F.col("ip.provider_name").alias("insurance_provider_name"),
        F.coalesce(F.col("la.total_tests"), F.lit(0)).alias("total_tests"),
        F.coalesce(F.col("la.abnormal_test_count"), F.lit(0)).alias("abnormal_test_count"),
        F.coalesce(F.col("la.critical_test_count"), F.lit(0)).alias("critical_test_count"),
        F.col("la.last_test_date"),
    )
)

# COMMAND ----------

# DBTITLE 1,Write to Gold
gold_table = "adb_caresync.gold.patient_360"
patient_360.write.mode("overwrite").saveAsTable(gold_table)

record_count = patient_360.count()
print(f"Rows written to {gold_table}: {record_count}")
dbutils.notebook.exit(str(record_count))