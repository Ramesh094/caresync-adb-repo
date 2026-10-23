# Databricks notebook source
# DBTITLE 1,STTM Header
# MAGIC %md
# MAGIC # NB_hospital_daily_summary
# MAGIC **Target:** `adb_caresync.gold.hospital_daily_summary` | **Grain:** One row per hospital per calendar day | **Load:** FULL overwrite
# MAGIC
# MAGIC **Driving Table:** `silver.lab_results` (grouped by hospital_id, test_date) | **Joins:** LEFT JOIN hospitals, calendar | **Window:** LAG() for day-over-day trend

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

# COMMAND ----------

# DBTITLE 1,Read Source Tables
lab_results = spark.table("adb_caresync.silver.lab_results")
hospitals = spark.table("adb_caresync.silver.hospitals")
calendar = spark.table("adb_caresync.silver.calendar")

# COMMAND ----------

# DBTITLE 1,Aggregate Lab Results per Hospital per Day
daily_agg = (
    lab_results
    .groupBy("hospital_id", "test_date")
    .agg(
        F.count("*").alias("total_tests"),
        F.countDistinct("patient_id").alias("unique_patients_tested"),
        F.countDistinct("doctor_id").alias("active_doctor_count"),
        F.count(F.when(F.col("result_status") == "Abnormal", 1)).alias("abnormal_test_count"),
        F.count(F.when(F.col("result_status") == "Critical", 1)).alias("critical_test_count"),
    )
    .withColumnRenamed("test_date", "summary_date")
)

# COMMAND ----------

# DBTITLE 1,Enrich with Hospital and Calendar
enriched = (
    daily_agg.alias("da")
    .join(hospitals.alias("h"), F.col("da.hospital_id") == F.col("h.hospital_id"), "left")
    .join(
        calendar.alias("c"),
        F.col("c.date_key") == F.date_format(F.col("da.summary_date"), "yyyyMMdd").cast("int"),
        "left",
    )
    .select(
        F.col("da.hospital_id"),
        F.col("h.hospital_name"),
        F.col("da.summary_date"),
        F.col("c.day_name"),
        F.col("c.is_weekend"),
        F.col("da.total_tests"),
        F.col("da.unique_patients_tested"),
        F.col("da.active_doctor_count"),
        F.col("da.abnormal_test_count"),
        F.col("da.critical_test_count"),
        F.round(F.col("da.abnormal_test_count") / F.col("da.total_tests") * 100, 2).cast(DecimalType(5, 2)).alias("abnormal_rate_pct"),
        F.round(F.col("da.critical_test_count") / F.col("da.total_tests") * 100, 2).cast(DecimalType(5, 2)).alias("critical_rate_pct"),
    )
)

# COMMAND ----------

# DBTITLE 1,Apply Day-over-Day Window Logic
w = Window.partitionBy("hospital_id").orderBy("summary_date")

hospital_daily_summary = (
    enriched
    .withColumn("prev_day_total_tests", F.lag("total_tests").over(w))
    .withColumn(
        "test_volume_change_pct",
        F.when(
            (F.col("prev_day_total_tests").isNull()) | (F.col("prev_day_total_tests") == 0),
            F.lit(None),
        ).otherwise(
            F.round(
                (F.col("total_tests") - F.col("prev_day_total_tests"))
                / F.col("prev_day_total_tests") * 100,
                2,
            )
        ).cast(DecimalType(6, 2)),
    )
)

# COMMAND ----------

# DBTITLE 1,Write to Gold
gold_table = "adb_caresync.gold.hospital_daily_summary"
hospital_daily_summary.write.mode("overwrite").saveAsTable(gold_table)

record_count = hospital_daily_summary.count()
print(f"Rows written to {gold_table}: {record_count}")
dbutils.notebook.exit(str(record_count))