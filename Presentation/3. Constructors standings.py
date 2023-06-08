# Databricks notebook source
dbutils.widgets.text("param_file_date","2021-03-21")
var_file_date=dbutils.widgets.get("param_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

# MAGIC %run "../Includes/Commom_functions"

# COMMAND ----------

rece_results_list=spark.read.parquet(f"{presentation_folder_path}/race_results").filter(f"file_date = '{var_file_date}'").select ("race_year").distinct().collect()

# COMMAND ----------

race_year_list=[]
for race_year in rece_results_list:
    race_year_list.append(race_year.race_year)
print(race_year_list)

# COMMAND ----------

from pyspark.sql.functions import col

constructors_df=spark.read.parquet(f"{presentation_folder_path}/race_results").filter(col("race_year").isin(race_year_list))


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

overwrite_partition(final_df,'f1_presentation','constructors_standings','race_year')
