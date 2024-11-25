DROP TABLE IF EXISTS kjwassell_cta_ridership_csv;

CREATE EXTERNAL TABLE kjwassell_cta_ridership_csv(
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