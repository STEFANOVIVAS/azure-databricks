# Databricks notebook source
# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

races_df=spark.read.parquet(f'{processed_folder_path}/races').withColumnRenamed("name","race_name").withColumnRenamed("date","race_date")

# COMMAND ----------

circuits_df=spark.read.parquet(f'{processed_folder_path}/circuits').withColumnRenamed("location","circuit_location")

# COMMAND ----------

results_df=spark.read.parquet(f"{processed_folder_path}/results").withColumnRenamed("time","race_time")

# COMMAND ----------

drivers_df=spark.read.parquet(f"{processed_folder_path}/drivers").withColumnRenamed("name","driver_name").withColumnRenamed("nationality","driver_nationality").withColumnRenamed("number","driver_number")

# COMMAND ----------

constructors_df=spark.read.parquet(f"{processed_folder_path}/constructors").withColumnRenamed("name","constructor_team")

# COMMAND ----------

races_circuits_df=races_df.join(circuits_df,"circuit_id","inner")

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

race_results_df=results_df.join(races_circuits_df,"race_id","inner").join(drivers_df,"driver_id","inner").join(constructors_df,"constructor_id","inner")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

results_final_df=race_results_df.select("race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","constructor_team","grid","position","fastest_lap","race_time","points").withColumn("created_date",current_timestamp())

# COMMAND ----------

display(results_final_df.filter("race_year=2020 and race_name='Abu Dhabi Grand Prix'").orderBy(results_final_df.points.desc()))

# COMMAND ----------

results_final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------


