-- Databricks notebook source
-- MAGIC %md
-- MAGIC Read the input data to be score against the model from the landing location /FileStore/avi_files/predictive_maintenance.csv

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE machine_readings_raw
COMMENT "The raw transaction readings, ingested from /FileStore/machine_data/machine_readings_.csv"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * FROM cloud_files("/FileStore/machine_data", "csv", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE machine_readings_cleaned(
  CONSTRAINT valid_machine_reading EXPECT (UDI IS NOT NULL OR PRODUCT_ID IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")

COMMENT "Drop all rows with nulls for Time and store these records in a silver delta table"
AS SELECT * FROM STREAM(live.machine_readings_raw)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE predictions
COMMENT "Use the light gradient boosting model with the vectorized udf registered in the previous step to predict machines likely to fail"
TBLPROPERTIES ("quality" = "gold")
AS SELECT UDI, predict_failure(UDI, Product_ID, Type, Air_temperature_K, Process_temperature_K, Rotational_speed_rpm, Torque_Nm, Tool_wear_min) as failure_prediction from STREAM(live.machine_readings_cleaned)

-- COMMAND ----------


