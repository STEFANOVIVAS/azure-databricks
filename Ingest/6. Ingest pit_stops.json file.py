# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Set schema and load data

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType


# COMMAND ----------

pit_stops_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("stop",StringType(),True),
                                    StructField("lap",IntegerType(),True),
                                    StructField("time",StringType(),True),
                                    StructField("duration",StringType(),True),
                                    StructField("milliseconds",IntegerType(),True)                               
                                    
                                    ])

# COMMAND ----------

pit_stops_df=spark.read.schema(pit_stops_schema).option("multiline",True).json("/mnt/formula1projectlake/raw/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and create ingestion date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

pit_stops_final_df=pit_stops_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - write results files to data lake

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").parquet("/mnt/formula1projectlake/processed/pit_stops/")

# COMMAND ----------


