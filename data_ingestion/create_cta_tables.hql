-- External table for CTA stations metadata
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

-- External table for CTA ridership data
CREATE EXTERNAL TABLE IF NOT EXISTS cta_ridership (
    station_id INT,
    date DATE,
    daily_entries INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/kjwassell/cta_data/ridership/';

-- External table for CTA routes data
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

-- Validate table creation with sample queries
SELECT * FROM cta_stations LIMIT 10;
SELECT * FROM cta_ridership LIMIT 10;
SELECT * FROM cta_routes LIMIT 10;
