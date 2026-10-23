-- Databricks notebook source
-- STTM: lab_test_trends
-- Target: adb_caresync.gold.lab_test_trends
-- Grain: One row per test type (test_name) per calendar month
-- Load: FULL — CREATE OR REPLACE TABLE ... AS SELECT
-- Driving Table: silver.lab_results (grouped by test_name, year/month of test_date)
-- Joins: OPTIONAL LEFT JOIN calendar
-- Window: LAG() partitioned by test_name, ordered by year, month

-- COMMAND ----------

CREATE OR REPLACE TABLE adb_caresync.gold.lab_test_trends AS

WITH monthly_agg AS (
  SELECT
    lr.test_name,
    YEAR(lr.test_date)                                                        AS year,
    MONTH(lr.test_date)                                                       AS month,
    COUNT(*)                                                                  AS total_tests,
    COUNT(CASE WHEN lr.result_status = 'Abnormal' THEN 1 END)                AS abnormal_test_count,
    COUNT(CASE WHEN lr.result_status = 'Critical' THEN 1 END)                AS critical_test_count
  FROM adb_caresync.silver.lab_results lr
  GROUP BY lr.test_name, YEAR(lr.test_date), MONTH(lr.test_date)
),

enriched AS (
  SELECT
    ma.test_name,
    ma.year,
    ma.month,
    c.month_name,
    CONCAT('Q', c.quarter)                                                    AS quarter_name,
    ma.total_tests,
    ma.abnormal_test_count,
    ma.critical_test_count,
    CAST(ROUND(ma.abnormal_test_count / ma.total_tests * 100, 2)
         AS DECIMAL(5,2))                                                     AS abnormal_rate_pct,
    CAST(ROUND(ma.critical_test_count / ma.total_tests * 100, 2)
         AS DECIMAL(5,2))                                                     AS critical_rate_pct
  FROM      monthly_agg                            ma
  LEFT JOIN adb_caresync.silver.calendar            c
         ON c.date_key = CAST(DATE_FORMAT(
              DATE_TRUNC('MONTH', MAKE_DATE(ma.year, ma.month, 1)), 'yyyyMMdd') AS INT)
)

SELECT
  test_name,
  year,
  month,
  month_name,
  quarter_name,
  total_tests,
  abnormal_test_count,
  critical_test_count,
  abnormal_rate_pct,
  critical_rate_pct,
  CAST(
    LAG(abnormal_rate_pct) OVER (PARTITION BY test_name ORDER BY year, month)
    AS DECIMAL(5,2)
  )                                                                           AS prev_month_abnormal_rate_pct,
  CAST(
    abnormal_rate_pct
    - LAG(abnormal_rate_pct) OVER (PARTITION BY test_name ORDER BY year, month)
    AS DECIMAL(6,2)
  )                                                                           AS abnormal_rate_change_pct
FROM enriched;

-- COMMAND ----------

SELECT COUNT(*) AS row_count FROM adb_caresync.gold.lab_test_trends;

-- COMMAND ----------

SELECT * FROM adb_caresync.gold.lab_test_trends ORDER BY test_name, year, month LIMIT 10;
