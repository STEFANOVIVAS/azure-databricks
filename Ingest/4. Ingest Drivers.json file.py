# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Set schema and load data

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),True),
                               StructField("surname",StringType(),True)                       
                               ])

# COMMAND ----------

driver_schema=StructType(fields=[StructField("code",StringType(),True),
                                 StructField("dob",DateType(),True),
                                 StructField("driverId",IntegerType(),False),
                                 StructField("driverRef",StringType(),True),
                                 StructField("name",name_schema),
                                 StructField("nationality",StringType(),True),
                                 StructField("number",IntegerType(),True),
                                 StructField("url",StringType(),True)])

# COMMAND ----------

drivers_df=spark.read.schema(driver_schema).json('/mnt/formula1projectlake/raw/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns, concat name colmun and create ingestion date column

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,concat,lit

# COMMAND ----------

drivers_renamed_df=drivers_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("driverRef","driver_ref").withColumn("name",concat(col("name.forename"),lit(" "), col("name.surname"))).withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop unwanted columns 

# COMMAND ----------

drivers_final_df=drivers_renamed_df.drop(col('url'))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write file to data lake

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/formula1projectlake/processed/drivers")

# COMMAND ----------


