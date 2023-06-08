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

from pyspark.sql.types import StructType,StructField,IntegerType,StringType


# COMMAND ----------

lap_times_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                    StructField("driverId",IntegerType(),False),
                                    StructField("lap",IntegerType(),True),
                                    StructField("position",IntegerType(),True),
                                    StructField("time",StringType(),True),
                                    StructField("milliseconds",IntegerType(),True)                               
                                    
                                    ])

# COMMAND ----------

lap_times_df=spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/{var_file_date}/lap_times")

# COMMAND ----------

lap_times_df.printSchema()

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and create ingestion date column

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

lap_times_final_df=add_ingestion_date(lap_times_df).withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumn("data_source",lit(var_data_source)).withColumn("file_date",lit(var_file_date))

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - write results files to data lake

# COMMAND ----------

merge_condition="oldData.race_id=newData.race_id and oldData.driver_id=newData.driver_id and oldData.lap=newData.lap"
merge_delta_data('f1_processed','lap_times',processed_folder_path,lap_times_final_df,'race_id',merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,count(*)
# MAGIC FROM f1_processed.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

dbutils.notebook.exit("Success")
