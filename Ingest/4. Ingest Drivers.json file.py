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

drivers_df=spark.read.schema(driver_schema).json(f'{raw_folder_path}/drivers.json')

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns, concat name colmun and create ingestion date column

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,concat,lit

# COMMAND ----------

drivers_renamed_df=add_ingestion_date(drivers_df).withColumnRenamed("driverId","driver_id").withColumnRenamed("driverRef","driver_ref").withColumn("name",concat(col("name.forename"),lit(" "), col("name.surname")))

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

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------


