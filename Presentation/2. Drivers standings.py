# Databricks notebook source
# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

drivers_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import sum,col,count,when

drivers_standings_df=drivers_df.groupBy("race_year","driver_name","driver_nationality","constructor_team").agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

display(drivers_standings_df.filter("race_year=2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc

driver_rank_spec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df=drivers_standings_df.withColumn("rank",rank().over(driver_rank_spec))
                                                         


# COMMAND ----------

display(final_df.filter("race_year=2020"))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.drivers_standings")
