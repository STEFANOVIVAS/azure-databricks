-- Databricks notebook source
select team_name,
count(*) as total_races,
SUM(calculated_points) as Total_points,
AVG(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by team_name
having count(*) >=100
order by avg_points desc


-- COMMAND ----------

select team_name,
count(*) as total_races,
SUM(calculated_points) as Total_points,
AVG(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2011 and 2022
group by team_name
having count(*) >=100
order by avg_points desc

-- COMMAND ----------


