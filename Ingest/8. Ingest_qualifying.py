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

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType


# COMMAND ----------

qualifying_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("constructorId",IntegerType(),True),
                                    StructField("qualifyId",IntegerType(),False),
                                    StructField("number",IntegerType(),True),
                                    StructField("position",IntegerType(),True),
                                    StructField("q1",StringType(),True),
                                    StructField("q2",StringType(),True),
                                    StructField("q3",StringType(),True)                               
                                    
                                    ])

# COMMAND ----------

qualifying_df=spark.read.option("multiline",True).json(f"{raw_folder_path}/{var_file_date}/qualifying/")

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and create ingestion date column

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

qualifying_final_df=add_ingestion_date(qualifying_df).withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumnRenamed("constructorId","constructor_id").withColumnRenamed("qualifyId","qualify_id").withColumn("data_source",lit(var_data_source)).withColumn("file_date",lit(var_file_date))

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - write results files to data lake

# COMMAND ----------

overwrite_partition(qualifying_final_df, 'f1_processed','qualifying','race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,count(*)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------


