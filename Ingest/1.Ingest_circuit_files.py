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

circuits_schema=StructType(fields=[StructField("circuitId",IntegerType(),False),
                                   StructField("circuitRef",StringType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("location",StringType(),True),
                                   StructField("country",StringType(),True),
                                   StructField("lat",DoubleType(),True),
                                   StructField("lng",DoubleType(),True),
                                   StructField("alt",IntegerType(),True),
                                   StructField("url",StringType(),True)])

# COMMAND ----------

circuits_df=spark.read.option("header",True).schema(circuits_schema).csv(f'{raw_folder_path}/{var_file_date}/circuits.csv')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

circuits_selected_df=circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns as required 

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").withColumnRenamed("lat","latitude").withColumnRenamed("lng","longitude").withColumnRenamed("alt","altitude").withColumn("data_source",lit(var_data_source)).withColumn("file_date",lit(var_file_date))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Adding the ingestion date to the dataframe

# COMMAND ----------

circuits_final_df=add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")
