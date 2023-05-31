# Databricks notebook source
# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

races_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

races_results_df.createOrReplaceTempView("race_results_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM race_results_view
# MAGIC WHERE race_year=2020

# COMMAND ----------

race_year=2019

# COMMAND ----------

race_results_2019=spark.sql(f"SELECT * FROM race_results_view WHERE race_year={race_year}")

# COMMAND ----------

display(race_results_2019)

# COMMAND ----------


