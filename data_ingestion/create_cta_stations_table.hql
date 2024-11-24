-- External table for CTA stations metadata
CREATE EXTERNAL TABLE IF NOT EXISTS kjwassell_cta_stations (
    stop_id INT,
    direction STRING,
    stop_name STRING,
    station_name STRING,
    station_description STRING,
    map_id INT,
    ada BOOLEAN,
    red BOOLEAN,
    blue BOOLEAN,
    g BOOLEAN,
    brn BOOLEAN,
    p BOOLEAN,
    pexp BOOLEAN,
    y BOOLEAN,
    pnk BOOLEAN,
    o BOOLEAN,
    location STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/kjwassell/cta_data/stations/';

-- Validate table creation with a sample query
SELECT * FROM kjwassell_cta_stations LIMIT 10;
