# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Set schema and load data

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

lap_times_df=spark.read.schema(lap_times_schema).csv("/mnt/formula1projectlake/raw/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and create ingestion date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

lap_times_final_df=lap_times_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - write results files to data lake

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet("/mnt/formula1projectlake/processed/lap_times")

# COMMAND ----------


