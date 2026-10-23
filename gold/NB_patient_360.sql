-- Databricks notebook source
-- STTM: patient_360
-- Target: adb_caresync.gold.patient_360
-- Grain: One row per patient
-- Load: FULL — CREATE OR REPLACE TABLE ... AS SELECT
-- Driving Table: silver.patients
-- Joins: LEFT JOIN hospitals, insurance_providers, aggregated lab_results

-- COMMAND ----------

CREATE OR REPLACE TABLE adb_caresync.gold.patient_360 AS

WITH lab_agg AS (
  SELECT
    patient_id,
    COUNT(*)                                                          AS total_tests,
    COUNT(CASE WHEN result_status = 'Abnormal' THEN 1 END)           AS abnormal_test_count,
    COUNT(CASE WHEN result_status = 'Critical' THEN 1 END)           AS critical_test_count,
    MAX(test_date)                                                    AS last_test_date
  FROM adb_caresync.silver.lab_results
  GROUP BY patient_id
)

SELECT
  p.patient_id,
  p.first_name,
  p.last_name,
  CAST(p.date_of_birth AS DATE)                                       AS date_of_birth,
  FLOOR(DATEDIFF(CURRENT_DATE(), p.date_of_birth) / 365.25)           AS age,
  p.gender,
  p.city,
  p.state,
  p.registered_hospital_id                                            AS hospital_id,
  h.hospital_name,
  p.insurance_provider_id,
  ip.provider_name                                                    AS insurance_provider_name,
  COALESCE(la.total_tests, 0)                                         AS total_tests,
  COALESCE(la.abnormal_test_count, 0)                                 AS abnormal_test_count,
  COALESCE(la.critical_test_count, 0)                                 AS critical_test_count,
  la.last_test_date

FROM      adb_caresync.silver.patients            p
LEFT JOIN adb_caresync.silver.hospitals           h   ON p.registered_hospital_id = h.hospital_id
LEFT JOIN adb_caresync.silver.insurance_providers ip   ON p.insurance_provider_id  = ip.insurance_provider_id
LEFT JOIN lab_agg                                 la  ON p.patient_id             = la.patient_id;

-- COMMAND ----------

SELECT COUNT(*) AS row_count FROM adb_caresync.gold.patient_360;

-- COMMAND ----------

SELECT * FROM adb_caresync.gold.patient_360 LIMIT 10;
