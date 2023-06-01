-- Databricks notebook source
WITH dominant_drivers AS(
select driver_name,
count(*) as total_races,
SUM(calculated_points) as Total_points,
AVG(calculated_points) as avg_points,
RANK() OVER (ORDER BY AVG(calculated_points) DESC) as driver_rank
from f1_presentation.calculated_race_results
group by driver_name
having count(*) >=50
order by avg_points desc)

select race_year,
driver_name,
count(*) as total_races,
SUM(calculated_points) as Total_points,
AVG(calculated_points) as avg_points

from f1_presentation.calculated_race_results
where driver_name IN (select driver_name from dominant_drivers where driver_rank <=10 )
group by race_year,driver_name
order by race_year,avg_points desc





-- COMMAND ----------

WITH dominant_drivers AS(
select driver_name,
count(*) as total_races,
SUM(calculated_points) as Total_points,
AVG(calculated_points) as avg_points,
RANK() OVER (ORDER BY AVG(calculated_points) DESC) as driver_rank
from f1_presentation.calculated_race_results
group by driver_name
having count(*) >=50
order by avg_points desc)

select race_year,
driver_name,
count(*) as total_races,
SUM(calculated_points) as Total_points,
AVG(calculated_points) as avg_points

from f1_presentation.calculated_race_results
where driver_name IN (select driver_name from dominant_drivers where driver_rank <=10 )
group by race_year,driver_name
order by race_year,avg_points desc

-- COMMAND ----------

WITH dominant_drivers AS(
select driver_name,
count(*) as total_races,
SUM(calculated_points) as Total_points,
AVG(calculated_points) as avg_points,
RANK() OVER (ORDER BY AVG(calculated_points) DESC) as driver_rank
from f1_presentation.calculated_race_results
group by driver_name
having count(*) >=50
order by avg_points desc)

select race_year,
driver_name,
count(*) as total_races,
SUM(calculated_points) as Total_points,
AVG(calculated_points) as avg_points

from f1_presentation.calculated_race_results
where driver_name IN (select driver_name from dominant_drivers where driver_rank <=10 )
group by race_year,driver_name
order by race_year,avg_points desc

-- COMMAND ----------


