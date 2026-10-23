# Databricks notebook source
# DBTITLE 1,STTM Header
# MAGIC %md
# MAGIC # NB_lab_test_trends
# MAGIC **Target:** `adb_caresync.gold.lab_test_trends` | **Grain:** One row per test type per calendar month | **Load:** FULL overwrite
# MAGIC
# MAGIC **Driving Table:** `silver.lab_results` grouped by test_name, year, month | **Join:** Optional LEFT JOIN calendar | **Window:** LAG() for month-over-month trend

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

# COMMAND ----------

# DBTITLE 1,Read Source Tables
lab_results = spark.table("adb_caresync.silver.lab_results")
calendar = spark.table("adb_caresync.silver.calendar")

# COMMAND ----------

# DBTITLE 1,Aggregate Lab Results per Month
monthly_agg = (
    lab_results
    .groupBy(
        F.col("test_name"),
        F.year("test_date").alias("year"),
        F.month("test_date").alias("month"),
    )
    .agg(
        F.count("*").alias("total_tests"),
        F.count(F.when(F.col("result_status") == "Abnormal", 1)).alias("abnormal_test_count"),
        F.count(F.when(F.col("result_status") == "Critical", 1)).alias("critical_test_count"),
    )
)

# COMMAND ----------

# DBTITLE 1,Enrich with Calendar Attributes
enriched = (
    monthly_agg.alias("ma")
    .join(
        calendar.alias("c"),
        F.col("c.date_key") == F.date_format(
            F.make_date(F.col("ma.year"), F.col("ma.month"), F.lit(1)),
            "yyyyMMdd",
        ).cast("int"),
        "left",
    )
    .select(
        F.col("ma.test_name"),
        F.col("ma.year"),
        F.col("ma.month"),
        F.col("c.month_name"),
        F.concat(F.lit("Q"), F.col("c.quarter")).alias("quarter_name"),
        F.col("ma.total_tests"),
        F.col("ma.abnormal_test_count"),
        F.col("ma.critical_test_count"),
        F.round(F.col("ma.abnormal_test_count") / F.col("ma.total_tests") * 100, 2).cast(DecimalType(5, 2)).alias("abnormal_rate_pct"),
        F.round(F.col("ma.critical_test_count") / F.col("ma.total_tests") * 100, 2).cast(DecimalType(5, 2)).alias("critical_rate_pct"),
    )
)

# COMMAND ----------

# DBTITLE 1,Apply Month-over-Month Window Logic
w = Window.partitionBy("test_name").orderBy("year", "month")

lab_test_trends = (
    enriched
    .withColumn(
        "prev_month_abnormal_rate_pct",
        F.lag("abnormal_rate_pct").over(w).cast(DecimalType(5, 2)),
    )
    .withColumn(
        "abnormal_rate_change_pct",
        (F.col("abnormal_rate_pct") - F.col("prev_month_abnormal_rate_pct")).cast(DecimalType(6, 2)),
    )
)

# COMMAND ----------

# DBTITLE 1,Write to Gold
gold_table = "adb_caresync.gold.lab_test_trends"
lab_test_trends.write.mode("overwrite").saveAsTable(gold_table)

record_count = lab_test_trends.count()
print(f"Rows written to {gold_table}: {record_count}")
dbutils.notebook.exit(str(record_count))