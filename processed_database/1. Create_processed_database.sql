-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1projectlake/processed"

-- COMMAND ----------

DESCRIBE DATABASE f1_processed

-- COMMAND ----------

drop database f1_processed;

-- COMMAND ----------


