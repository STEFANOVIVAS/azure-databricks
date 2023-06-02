# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Set schema and load data

# COMMAND ----------

dbutils.widgets.text("param_data_source","")
var_data_source=dbutils.widgets.get("param_data_source")

# COMMAND ----------

dbutils.widgets.text("param_file_date","2021-03-21")
var_file_date=dbutils.widgets.get("param_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

# MAGIC %run "../Includes/Commom_functions"

# COMMAND ----------

constructors_schema="constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df=spark.read.schema(constructors_schema).json(f"{raw_folder_path}/{var_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted columns

# COMMAND ----------

constructors_dropped_df=constructors_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and and ingestion date

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructros_rename_df=add_ingestion_date(constructors_dropped_df).withColumnRenamed("constructorId","constructor_id").withColumnRenamed("constructorRef","constructor_ref").withColumn("data_source",lit(var_data_source)).withColumn("file_date",lit(var_file_date))

# COMMAND ----------

display(constructros_rename_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write data to data lake

# COMMAND ----------

constructros_rename_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------


