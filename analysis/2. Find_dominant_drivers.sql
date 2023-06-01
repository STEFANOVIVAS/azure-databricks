-- Databricks notebook source
select driver_name,
count(*) as total_races,
SUM(calculated_points) as Total_points,
AVG(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by driver_name
having count(*) >=50
order by avg_points desc


-- COMMAND ----------


