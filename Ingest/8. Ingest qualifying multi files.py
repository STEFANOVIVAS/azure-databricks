# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Set schema and load data

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

qualifying_df=spark.read.option("multiline",True).json(f"{raw_folder_path}/qualifying/")

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and create ingestion date column

# COMMAND ----------

qualifying_final_df=add_ingestion_date(qualifying_df).withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumnRenamed("constructorId","constructor_id").withColumnRenamed("qualifyId","qualify_id")

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - write results files to data lake

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------


