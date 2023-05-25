# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Set schema and load data

# COMMAND ----------



# COMMAND ----------

display(qualifying_df)

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

qualifying_df=spark.read.option("multiline",True).json("/mnt/formula1projectlake/raw/qualifying/")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and create ingestion date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

qualifying_final_df=qualifying_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumnRenamed("constructorId","constructor_id").withColumnRenamed("qualifyId","qualify_id").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - write results files to data lake

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet("/mnt/formula1projectlake/processed/qualifying/")

# COMMAND ----------


