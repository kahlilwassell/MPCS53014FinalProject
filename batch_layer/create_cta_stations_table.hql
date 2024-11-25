DROP TABLE IF EXISTS kjwassell_cta_stations_csv;

-- External table for CTA stations metadata
CREATE EXTERNAL TABLE kjwassell_cta_stations_csv(
    stop_id INT,
    direction_id STRING,
    stop_name STRING,
    station_name STRING,
    station_descriptive_name STRING,
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
    latitude FLOAT,
    longitude FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/kjwassell/cta_data/stations'
TBLPROPERTIES("skip.header.line.count"="1");