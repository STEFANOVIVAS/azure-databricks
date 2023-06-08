# Databricks notebook source
dbutils.widgets.text("param_file_date","2021-03-21")
var_file_date=dbutils.widgets.get("param_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

# MAGIC %run "../Includes/Commom_functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform dim tables

# COMMAND ----------

races_df=spark.read.format("delta").load(f'{processed_folder_path}/races').withColumnRenamed("name","race_name").withColumnRenamed("date","race_date")

# COMMAND ----------

circuits_df=spark.read.format("delta").load(f'{processed_folder_path}/circuits').withColumnRenamed("location","circuit_location")

# COMMAND ----------

drivers_df=spark.read.format("delta").load(f"{processed_folder_path}/drivers").withColumnRenamed("name","driver_name").withColumnRenamed("nationality","driver_nationality").withColumnRenamed("number","driver_number")

# COMMAND ----------

constructors_df=spark.read.format("delta").load(f"{processed_folder_path}/constructors").withColumnRenamed("name","constructor_team")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform fact tables

# COMMAND ----------

results_df=spark.read.format("delta").load(f"{processed_folder_path}/results").filter(f"file_date = '{var_file_date}'").withColumnRenamed("time","race_time").withColumnRenamed("race_id","race_result_id").withColumnRenamed("file_date","results_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join circuits to races

# COMMAND ----------

races_circuits_df=races_df.join(circuits_df,"circuit_id","inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join results to all other dataframes

# COMMAND ----------

race_results_df=results_df.join(races_circuits_df,results_df.race_result_id==races_circuits_df.race_id,"inner").join(drivers_df,"driver_id","inner").join(constructors_df,"constructor_id","inner")

# COMMAND ----------

results_final_df=race_results_df.select("race_id","race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","constructor_team","grid","position","fastest_lap","race_time","points","results_file_date").withColumn("created_date",current_timestamp()).withColumnRenamed("results_file_date","file_date")

# COMMAND ----------

merge_condition="oldData.race_id=newData.race_id and oldData.driver_name=newData.driver_name"
merge_delta_data('f1_presentation','race_results',presentation_folder_path,results_final_df,'race_id',merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results WHERE race_year=2021;

# COMMAND ----------


