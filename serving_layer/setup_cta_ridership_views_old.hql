# Daily Station ridership aggregation
CREATE TABLE kjwassell_daily_station_ridership_orc STORED AS ORC AS
SELECT
    station_id,
    stationname,
    `date`,
    SUM(rides) AS total_rides
FROM kjwassell_cta_ridership_orc
GROUP BY station_id, stationname, `date`;


# Monthly ridership aggregation
CREATE TABLE kjwassell_monthly_ridership_orc STORED AS ORC AS
SELECT
    station_id,
    stationname,
    MONTH(`date`) AS month,
    YEAR(`date`) AS year,
    SUM(rides) AS total_rides
FROM kjwassell_cta_ridership_orc
GROUP BY station_id, stationname, MONTH(`date`), YEAR(`date`)
limit 10;

# Top Stations by ridership
CREATE TABLE kjwassell_top_stations_orc STORED AS ORC AS
SELECT
    station_id,
    stationname,
    SUM(rides) AS total_rides
FROM kjwassell_cta_ridership_orc
GROUP BY station_id, stationname
ORDER BY total_rides DESC
LIMIT 10;
