-- Create Stations Table
CREATE EXTERNAL TABLE IF NOT EXISTS cta_stations (
    stop_id INT,
    direction STRING,
    stop_name STRING,
    station_name STRING,
    station_description STRING,
    map_id INT,
    ada BOOLEAN,
    location STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/kjwassell/cta_data/stations/';

-- Create Ridership Table
CREATE EXTERNAL TABLE IF NOT EXISTS cta_ridership (
    station_id INT,
    date DATE,
    daily_entries INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/kjwassell/cta_data/ridership/';

-- Create Routes Table
CREATE EXTERNAL TABLE IF NOT EXISTS cta_routes (
    route_id STRING,
    route_short_name STRING,
    route_long_name STRING,
    route_type STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/kjwassell/cta_data/routes/';
