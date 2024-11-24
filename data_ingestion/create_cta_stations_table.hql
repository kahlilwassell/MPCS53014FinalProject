DROP TABLE IF EXISTS kjwassell_cta_stations;

-- External table for CTA stations metadata
CREATE EXTERNAL TABLE kjwassell_cta_stations(
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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",  -- Removed the escape character before the comma
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/kjwassell/cta_data/stations'
TBLPROPERTIES("skip.header.line.count"="1");

-- Validate table creation with a sample query
-- Note: Hive does not allow SELECT in the same script as table creation.
-- Run this query after table creation is confirmed.
-- SELECT * FROM kjwassell_cta_stations LIMIT 10;

