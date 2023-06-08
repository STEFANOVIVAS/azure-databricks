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

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType

# COMMAND ----------

races_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                   StructField("year",IntegerType(),True),
                                   StructField("round",IntegerType(),True),
                                   StructField("circuitId",IntegerType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("date",StringType(),True),
                                   StructField("time",StringType(),True),
                                   StructField("url",StringType(),True)])

# COMMAND ----------

circuits_df=spark.read.option("header",True).schema(races_schema).csv(f'{raw_folder_path}/{var_file_date}/races.csv')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Adding ingestion date and race timestamp to dataframe

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,concat,col,lit

# COMMAND ----------

races_transform_df=add_ingestion_date(circuits_df).withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),'yyyy-MM-dd HH:mm:ss')).withColumn("data_source",lit(var_data_source)).withColumn("file_date",lit(var_file_date))

# COMMAND ----------

display(races_transform_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Select only the required columns

# COMMAND ----------

races_selected_df=races_transform_df.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("date"),col("race_timestamp"),col("ingestion_date"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Rename columns as required 

# COMMAND ----------

races_final_df=races_selected_df.withColumnRenamed("raceId","race_id").withColumnRenamed("year","race_year").withColumnRenamed("circuitId","circuit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to datalake as parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races

# COMMAND ----------

dbutils.notebook.exit("Success")
