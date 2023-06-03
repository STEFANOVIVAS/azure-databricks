# Databricks notebook source
dbutils.widgets.text("param_data_source","")
var_data_source=dbutils.widgets.get("param_data_source")

# COMMAND ----------

dbutils.widgets.text("param_file_date","2021-03-21")
var_file_date=dbutils.widgets.get("param_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Set schema and load data

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

# MAGIC %run "../Includes/Commom_functions"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType


# COMMAND ----------

results_schema=StructType(fields=[StructField("resultId",IntegerType(),False),
                                  StructField("raceId",IntegerType(),False),
                                  StructField("driverId",IntegerType(),False),
                                  StructField("constructorId",IntegerType(),False),
                                  StructField("number",IntegerType(),True),
                                  StructField("grid",IntegerType(),False),
                                  StructField("position",IntegerType(),True),
                                  StructField("positionText",StringType(),False),
                                  StructField("positionOrder",IntegerType(),False),
                                  StructField("points",FloatType(),False),
                                  StructField("laps",IntegerType(),True),
                                  StructField("milliseconds",IntegerType(),True),
                                  StructField("fastestLap",IntegerType(),True),
                                  StructField("rank",IntegerType(),True),
                                  StructField("fastestLapTime",StringType(),True),
                                  StructField("fastestLapSpeed",StringType(),True),
                                  StructField("statusId",IntegerType(),False),
                                  StructField("time",StringType(),True)])

# COMMAND ----------

results_df=spark.read.schema(results_schema).json(f"{raw_folder_path}/{var_file_date}/results.json")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and cretae ingestion date column

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

results_renamed_df=add_ingestion_date(results_df).withColumnRenamed("resultId","result_id").withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("constructorId","constructor_id").withColumnRenamed("positionText","position_text").withColumnRenamed("positionOrder", "position_order").withColumnRenamed("fastestLap","fastest_lap").withColumnRenamed("fastestLapTime", "fastest_lap_time").withColumnRenamed("fastestLapSpeed","fastest_lap_speed").withColumn("data_source",lit(var_data_source)).withColumn("file_date",lit(var_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop unwanted columns

# COMMAND ----------

results_final_df=results_renamed_df.drop(col('statusId'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - write results files to data lake

# COMMAND ----------

overwrite_partition(results_final_df, 'f1_processed','results','race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


