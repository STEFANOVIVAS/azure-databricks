# Databricks notebook source
dbutils.widgets.text("param_file_date","2021-03-21")
var_file_date=dbutils.widgets.get("param_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

# MAGIC %run "../Includes/Commom_functions"

# COMMAND ----------

rece_results_list=spark.read.format("delta").load(f"{presentation_folder_path}/race_results").filter(f"file_date = '{var_file_date}'").select ("race_year").distinct().collect()

# COMMAND ----------

race_year_list=[]
for race_year in rece_results_list:
    race_year_list.append(race_year.race_year)
print(race_year_list)

# COMMAND ----------

from pyspark.sql.functions import col

constructors_df=spark.read.format("delta").load(f"{presentation_folder_path}/race_results").filter(col("race_year").isin(race_year_list))


# COMMAND ----------

from pyspark.sql.functions import sum,col,count,when

constructors_standings_df=constructors_df.groupBy("race_year","constructor_team").agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc

constructor_rank_spec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df=constructors_standings_df.withColumn("rank",rank().over(constructor_rank_spec))
                                                         


# COMMAND ----------

merge_condition="oldData.race_year=newData.race_year and oldData.constructor_team=newData.constructor_team"
merge_delta_data('f1_presentation','constructor_standings',presentation_folder_path,constructors_standings_df,'race_year',merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_presentation.constructor_standings
# MAGIC WHERE race_year=2021
# MAGIC ORDER BY total_points DESC
