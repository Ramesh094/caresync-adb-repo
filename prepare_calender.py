# Databricks notebook source
# DBTITLE 1,Prepare Calendar
# MAGIC %md
# MAGIC # Prepare Calendar Table
# MAGIC
# MAGIC Generates a comprehensive date dimension table `adb_caresync.silver.calendar` spanning 1940–2030 with date keys, fiscal periods, and day-level attributes.

# COMMAND ----------

# DBTITLE 1,Generate Calendar Table
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# Generate date range: 1940-01-01 to 2030-12-31
start_date = "1940-01-01"
end_date = "2030-12-31"

calendar_df = (
    spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS full_date")
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("day_of_week", F.dayofweek("full_date"))        # 1=Sun, 7=Sat
    .withColumn("day_of_year", F.dayofyear("full_date"))
    .withColumn("week_of_year", F.weekofyear("full_date"))
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .withColumn("day_name_short", F.date_format("full_date", "EEE"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("month_name_short", F.date_format("full_date", "MMM"))
    .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
    .withColumn("year_quarter", F.concat(F.col("year"), F.lit("-Q"), F.col("quarter")))
    .withColumn("is_weekend", F.when(F.dayofweek("full_date").isin(1, 7), True).otherwise(False))
    .withColumn("is_weekday", F.when(F.dayofweek("full_date").isin(1, 7), False).otherwise(True))
    .withColumn("is_month_start", F.when(F.col("day") == 1, True).otherwise(False))
    .withColumn("is_month_end", F.when(F.col("full_date") == F.last_day("full_date"), True).otherwise(False))
    # Fiscal year (Apr-Mar): fiscal year = year if month >= 4, else year - 1
    .withColumn("fiscal_year", F.when(F.col("month") >= 4, F.col("year")).otherwise(F.col("year") - 1))
    .withColumn("fiscal_quarter", F.when(F.col("month").between(4, 6), 1)
                                    .when(F.col("month").between(7, 9), 2)
                                    .when(F.col("month").between(10, 12), 3)
                                    .otherwise(4))
    .withColumn("fiscal_year_quarter", F.concat(F.lit("FY"), F.col("fiscal_year"), F.lit("-Q"), F.col("fiscal_quarter")))
    .select(
        "date_key", "full_date", "year", "quarter", "month", "day",
        "day_of_week", "day_of_year", "week_of_year",
        "day_name", "day_name_short", "month_name", "month_name_short",
        "year_month", "year_quarter",
        "is_weekend", "is_weekday", "is_month_start", "is_month_end",
        "fiscal_year", "fiscal_quarter", "fiscal_year_quarter"
    )
)

display(calendar_df)

# COMMAND ----------

# DBTITLE 1,Write to Silver
silver_table = "adb_caresync.silver.calendar"

calendar_df.write.mode("overwrite").saveAsTable(silver_table)

print(f"Calendar table written to {silver_table}")
print(f"Total rows: {spark.read.table(silver_table).count()}")