# Daily Station ridership aggregation
CREATE TABLE kjwassell_daily_station_ridership_orc STORED AS ORC AS
SELECT
    station_id,
    stationname,
    `date`,
    SUM(rides) AS total_rides
FROM kjwassell_cta_ridership_orc
GROUP BY station_id, stationname, `date`;


# Day type based aggregation
CREATE TABLE kjwassell_day_type_ridership_orc STORED AS ORC AS
SELECT
    station_id,
    stationname,
    daytype,
    SUM(rides) AS total_rides,
    AVG(rides) AS avg_rides
FROM kjwassell_cta_ridership_orc
GROUP BY station_id, stationname, daytype;

# Geospatial aggregation
CREATE TABLE kjwassell_geospatial_station_ridership_orc STORED AS ORC AS
SELECT
    r.station_id,
    r.stationname,
    s.latitude,
    s.longitude,
    SUM(r.rides) AS total_rides
FROM kjwassell_cta_ridership_orc r
JOIN kjwassell_cta_stations_orc s
ON r.station_id = s.map_id
GROUP BY r.station_id, r.stationname, s.latitude, s.longitude;

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
