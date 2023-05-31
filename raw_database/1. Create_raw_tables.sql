-- Databricks notebook source
CREATE DATABASE f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId Int,
  circuitRef String,
  name String,
  location String,
  country String,
  lat Double,
  lng Double,
  alt Int,
  url String
)
USING csv
OPTIONS (path "/mnt/formula1projectlake/raw/circuits.csv",header true) 

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId Int,
  year Int,
  round Int,
  circuitId Int,
  name String,
  date String,
  time String,
  url String

)
USING csv
OPTIONS (path "/mnt/formula1projectlake/raw/races.csv",header true)

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

USE f1_raw;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Constructors table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(constructorId INT, 
constructorRef STRING,
name STRING, 
nationality STRING, 
url STRING
)
USING json
OPTIONS (path "/mnt/formula1projectlake/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create drivers table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  code STRING,
  dob DATE,
  driverId INT,
  driverRef STRING,
  name STRUCT<forename:STRING,surname:STRING>, 
  nationality STRING,
  number INTEGER,
  url STRING 
)
USING json
OPTIONS (path "/mnt/formula1projectlake/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create pit stops table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  raceId INTEGER,
  driverId INTEGER,
  stop STRING,
  lap INTEGER,
  time STRING,
  duration STRING,
  milliseconds INTEGER
)
USING json
OPTIONS (path "/mnt/formula1projectlake/raw/pit_stops.json", multiline true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw_results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId integer,
  raceId integer,
  driverId integer,
  constructorId integer,
  number integer,
  grid integer,
  position integer,
  positionText string ,  positionOrder integer,
  points float,
  laps integer,
  milliseconds integer,
  fastestLap integer,
  rank integer,
  fastestLapTime string,
  fastestLapSpeed string,
  statusId integer,
  time string

)
USING json
OPTIONS (path "/mnt/formula1projectlake/raw/results.json" )

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create lap_times table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId integer,
driverId integer,
lap integer, 
position integer,
time string,
milliseconds integer)
USING csv
OPTIONS (path "/mnt/formula1projectlake/raw/lap_times" )

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId long,
driverId long,
number long,
position long,
q1 string,
q2 string,
q3 string,
qualifyId long ,
raceId long
)
USING json
OPTIONS (path "/mnt/formula1projectlake/raw/qualifying", multiline true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------


