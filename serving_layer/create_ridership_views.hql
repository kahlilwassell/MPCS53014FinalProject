-- Drop table if it exists
DROP TABLE IF EXISTS kjwassell_cta_ridership_with_day_orc;

-- Create table for ridership with day
CREATE TABLE kjwassell_cta_ridership_with_day_orc (
    station_id STRING,
    station_name STRING,
    `date` DATE,
    day STRING,
    rides INT
)
STORED AS ORC;

-- Populate table
INSERT INTO kjwassell_cta_ridership_with_day_orc
SELECT 
    station_id,
    stationname,
    `date`,
    CASE 
        WHEN dayofweek(`date`) = 1 THEN 'Su'
        WHEN dayofweek(`date`) = 2 THEN 'M'
        WHEN dayofweek(`date`) = 3 THEN 'T'
        WHEN dayofweek(`date`) = 4 THEN 'W'
        WHEN dayofweek(`date`) = 5 THEN 'Th'
        WHEN dayofweek(`date`) = 6 THEN 'F'
        WHEN dayofweek(`date`) = 7 THEN 'S'
    END AS day,
    rides
FROM kjwassell_cta_ridership_orc;

-- Drop table if it exists
DROP TABLE IF EXISTS kjwassell_cta_avg_rides_by_day_orc;

-- Create table for average rides by day
CREATE TABLE kjwassell_cta_avg_rides_by_day_orc (
    station_id STRING,
    station_name STRING,
    day STRING,
    avg_rides FLOAT
)
STORED AS ORC;

-- Populate table
INSERT INTO kjwassell_cta_avg_rides_by_day_orc
SELECT 
    station_id,
    station_name,
    day,
    AVG(rides) AS avg_rides
FROM kjwassell_cta_ridership_with_day_orc
GROUP BY station_id, station_name, day;
