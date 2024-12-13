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
DROP TABLE IF EXISTS kjwassell_cta_total_rides_by_day_orc ;

-- Create table for total rides and number of days by day
CREATE TABLE kjwassell_cta_total_rides_by_day_orc (
    station_id STRING,
    station_name STRING,
    day STRING,
    total_rides BIGINT, -- Total rides for the station and day
    num_days INT        -- Number of days contributing to the total rides
)
STORED AS ORC;


-- Populate table with total rides and number of days
INSERT INTO kjwassell_cta_total_rides_by_day_orc
SELECT 
    station_id,
    station_name,
    day,
    SUM(rides) AS total_rides,        -- Calculate total rides for the station and day
    COUNT(DISTINCT `date`) AS num_days -- Count distinct dates (number of days)
FROM kjwassell_cta_ridership_with_day_orc
GROUP BY station_id, station_name, day;