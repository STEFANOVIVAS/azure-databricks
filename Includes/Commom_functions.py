# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df=input_df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------

def re_arange_partition_columns(input_df,partition_name):
    columns=[]
    for column_name in input_df.schema.names:
        if column_name != partition_name:
            columns.append(column_name)
    columns.append(partition_name)
    output_df=input_df.select(columns)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df,db_name,table_name,partition_column):
    output_df=re_arange_partition_columns(input_df,partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(f"{partition_column}").format("parquet").saveAsTable(f"{db_name}.{table_name}")

