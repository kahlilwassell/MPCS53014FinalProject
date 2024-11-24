-- Create Stations Table
CREATE TABLE IF NOT EXISTS cta_stations (
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
STORED AS TEXTFILE;

-- Load data into Stations Table
LOAD DATA INPATH '/kjwassell/cta_data/stations/CTA_-_System_Information_-_List_of__L__Stops.csv'
INTO TABLE cta_stations;

-- Create Ridership Table
CREATE TABLE IF NOT EXISTS cta_ridership (
    station_id INT,
    date DATE,
    daily_entries INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Load data into Ridership Table
LOAD DATA INPATH '/kjwassell/cta_data/ridership/CTA_-_Ridership_-__L__Station_Entries_-_Daily_Totals.csv'
INTO TABLE cta_ridership;

-- Create Routes Table
CREATE TABLE IF NOT EXISTS cta_routes (
    route_id STRING,
    route_short_name STRING,
    route_long_name STRING,
    route_type STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Load data into Routes Table
LOAD DATA INPATH '/kjwassell/cta_data/routes/routes.txt'
INTO TABLE cta_routes;
