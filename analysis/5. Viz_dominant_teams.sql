-- Databricks notebook source
WITH dominant_teams AS(
select team_name,
count(*) as total_races,
SUM(calculated_points) as Total_points,
AVG(calculated_points) as avg_points,
RANK() OVER (ORDER BY AVG(calculated_points) DESC) as team_rank
from f1_presentation.calculated_race_results
group by team_name
having count(*) >=100
order by avg_points desc)

select race_year,
team_name,
count(*) as total_races,
SUM(calculated_points) as Total_points,
AVG(calculated_points) as avg_points

from f1_presentation.calculated_race_results
where team_name IN (select team_name from dominant_teams where team_rank <=5 )
group by race_year,team_name
order by race_year,avg_points desc





-- COMMAND ----------

WITH dominant_teams AS(
select team_name,
count(*) as total_races,
SUM(calculated_points) as Total_points,
AVG(calculated_points) as avg_points,
RANK() OVER (ORDER BY AVG(calculated_points) DESC) as team_rank
from f1_presentation.calculated_race_results
group by team_name
having count(*) >=100
order by avg_points desc)

select race_year,
team_name,
count(*) as total_races,
SUM(calculated_points) as Total_points,
AVG(calculated_points) as avg_points

from f1_presentation.calculated_race_results
where team_name IN (select team_name from dominant_teams where team_rank <=5 )
group by race_year,team_name
order by race_year,avg_points desc

-- COMMAND ----------


