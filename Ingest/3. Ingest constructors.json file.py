# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Set schema and load data

# COMMAND ----------

constructors_schema="constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df=spark.read.schema(constructors_schema).json("/mnt/formula1projectlake/raw/constructors.json")

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted columns

# COMMAND ----------

constructors_dropped_df=constructors_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and and ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructros_rename_df=constructors_dropped_df.withColumnRenamed("constructorId","constructor_id").withColumnRenamed("constructorRef","constructor_ref").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(constructros_rename_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write data to data lake

# COMMAND ----------

constructros_rename_df.write.mode("overwrite").parquet("/mnt/formula1projectlake/processed/constructors")

# COMMAND ----------


