# Databricks notebook source
# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

constructors_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

from pyspark.sql.functions import sum,col,count,when

constructors_standings_df=constructors_df.groupBy("race_year","constructor_team").agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

display(constructors_standings_df.filter("race_year=2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc

constructor_rank_spec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df=constructors_standings_df.withColumn("rank",rank().over(constructor_rank_spec))
                                                         


# COMMAND ----------

display(final_df.filter("race_year=2020"))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")
