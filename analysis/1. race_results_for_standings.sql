-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT races.race_year,
      constructors.name as team_name,
      drivers.name as driver_name,
      results.position,
      results.points,
      11 - results.position as calculated_points
FROM f1_processed.results
INNER JOIN f1_processed.races on (results.race_id=races.race_id)
INNER JOIN f1_processed.drivers on (results.driver_id=drivers.driver_id)
INNER JOIN f1_processed.constructors on (results.constructor_id=constructors.constructor_id)
where results.position <=10

-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_race_results

-- COMMAND ----------


