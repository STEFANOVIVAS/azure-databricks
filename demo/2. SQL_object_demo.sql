-- Databricks notebook source
-- MAGIC %run "../Includes/Configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format('parquet').saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

SELECT * FROM demo.race_results_python

-- COMMAND ----------


