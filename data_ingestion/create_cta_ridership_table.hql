DROP TABLE IF EXISTS kjwassell_cta_ridership;

CREATE EXTERNAL TABLE kjwassell_cta_ridership(
    station_id STRING,
    stationname STRING,
    `date` STRING,
    daytype STRING,
    rides STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/kjwassell/cta_data/ridership'
TBLPROPERTIES("skip.header.line.count"="1");

-- Validate table creation with a sample query
-- Note: Hive does not allow SELECT in the same script as table creation.
-- Run this query after table creation is confirmed.
-- SELECT * FROM kjwassell_cta_ridership LIMIT 10;