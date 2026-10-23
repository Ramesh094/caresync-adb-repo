-- Databricks notebook source
-- STTM: hospital_daily_summary
-- Target: adb_caresync.gold.hospital_daily_summary
-- Grain: One row per hospital per calendar day
-- Load: FULL — CREATE OR REPLACE TABLE ... AS SELECT
-- Driving Table: silver.lab_results (grouped by hospital_id, test_date)
-- Joins: LEFT JOIN hospitals, LEFT JOIN calendar
-- Window: LAG() partitioned by hospital_id, ordered by summary_date

-- COMMAND ----------

CREATE OR REPLACE TABLE adb_caresync.gold.hospital_daily_summary AS

WITH daily_agg AS (
  SELECT
    lr.hospital_id,
    lr.test_date                                                              AS summary_date,
    COUNT(*)                                                                  AS total_tests,
    COUNT(DISTINCT lr.patient_id)                                             AS unique_patients_tested,
    COUNT(DISTINCT lr.doctor_id)                                              AS active_doctor_count,
    COUNT(CASE WHEN lr.result_status = 'Abnormal' THEN 1 END)                AS abnormal_test_count,
    COUNT(CASE WHEN lr.result_status = 'Critical' THEN 1 END)                AS critical_test_count
  FROM adb_caresync.silver.lab_results lr
  GROUP BY lr.hospital_id, lr.test_date
),

enriched AS (
  SELECT
    da.hospital_id,
    h.hospital_name,
    da.summary_date,
    c.day_name,
    c.is_weekend,
    da.total_tests,
    da.unique_patients_tested,
    da.active_doctor_count,
    da.abnormal_test_count,
    da.critical_test_count,
    ROUND(da.abnormal_test_count / da.total_tests * 100, 2)                   AS abnormal_rate_pct,
    ROUND(da.critical_test_count / da.total_tests * 100, 2)                   AS critical_rate_pct
  FROM      daily_agg                             da
  LEFT JOIN adb_caresync.silver.hospitals          h  ON da.hospital_id = h.hospital_id
  LEFT JOIN adb_caresync.silver.calendar           c  ON c.date_key    = CAST(DATE_FORMAT(da.summary_date, 'yyyyMMdd') AS INT)
)

SELECT
  hospital_id,
  hospital_name,
  summary_date,
  day_name,
  is_weekend,
  total_tests,
  unique_patients_tested,
  active_doctor_count,
  abnormal_test_count,
  critical_test_count,
  CAST(abnormal_rate_pct AS DECIMAL(5,2))                                     AS abnormal_rate_pct,
  CAST(critical_rate_pct AS DECIMAL(5,2))                                     AS critical_rate_pct,
  LAG(total_tests) OVER (PARTITION BY hospital_id ORDER BY summary_date)       AS prev_day_total_tests,
  CAST(
    CASE
      WHEN LAG(total_tests) OVER (PARTITION BY hospital_id ORDER BY summary_date) IS NULL
        OR LAG(total_tests) OVER (PARTITION BY hospital_id ORDER BY summary_date) = 0
      THEN NULL
      ELSE ROUND(
        (total_tests - LAG(total_tests) OVER (PARTITION BY hospital_id ORDER BY summary_date))
        / LAG(total_tests) OVER (PARTITION BY hospital_id ORDER BY summary_date) * 100, 2)
    END AS DECIMAL(6,2)
  )                                                                           AS test_volume_change_pct
FROM enriched;

-- COMMAND ----------

SELECT COUNT(*) AS row_count FROM adb_caresync.gold.hospital_daily_summary;

-- COMMAND ----------

SELECT * FROM adb_caresync.gold.hospital_daily_summary LIMIT 10;
