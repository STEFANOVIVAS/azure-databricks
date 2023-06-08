# Databricks notebook source

v_result=dbutils.notebook.run("1.Ingest_circuit_files",0,{"param_data_source":"Ergast API","param_file_date":"2021-04-18"})

# COMMAND ----------


v_result

# COMMAND ----------


v_result=dbutils.notebook.run("2.Ingest_race_files",0,{"param_data_source":"Ergast API","param_file_date":"2021-04-18"})

# COMMAND ----------


v_result

# COMMAND ----------


v_result=dbutils.notebook.run("3. Ingest_constructors",0,{"param_data_source":"Ergast API","param_file_date":"2021-04-18"})

# COMMAND ----------


v_result

# COMMAND ----------


v_result=dbutils.notebook.run("4. Ingest_drivers",0,{"param_data_source":"Ergast API","param_file_date":"2021-04-18"})

# COMMAND ----------


v_result

# COMMAND ----------


v_result=dbutils.notebook.run("5. Ingest_results",0,{"param_data_source":"Ergast API","param_file_date":"2021-04-18"})

# COMMAND ----------


v_result

# COMMAND ----------


v_result=dbutils.notebook.run("6. Ingest_pit_stops",0,{"param_data_source":"Ergast API","param_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------


v_result=dbutils.notebook.run("7. Ingest_lap_times",0,{"param_data_source":"Ergast API","param_file_date":"2021-04-18"})

# COMMAND ----------


v_result

# COMMAND ----------


v_result=dbutils.notebook.run("8. Ingest_qualifying",0,{"param_data_source":"Ergast API","param_file_date":"2021-04-18"})

# COMMAND ----------


v_result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,count(*)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------


