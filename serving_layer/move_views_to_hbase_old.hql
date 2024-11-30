-- Daily Station Ridership Aggregation to HBase
DROP TABLE IF EXISTS kjwassell_daily_station_ridership_hbase;

CREATE EXTERNAL TABLE kjwassell_daily_station_ridership_hbase (
    rowkey STRING,
    station_id STRING,
    stationname STRING,
    `date` STRING, -- Adjusted to STRING for compatibility
    total_rides BIGINT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key#b,data:station_id#b,data:stationname,data:date#b,data:total_rides#b"
)
TBLPROPERTIES ("hbase.table.name" = "kjwassell_daily_station_ridership_hbase");

INSERT OVERWRITE TABLE kjwassell_daily_station_ridership_hbase
SELECT 
    CONCAT(station_id, '#', CAST(`date` AS STRING)) AS rowkey,
    station_id,
    stationname,
    CAST(`date` AS STRING) AS `date`, -- Explicit conversion
    total_rides
FROM kjwassell_daily_station_ridership_orc;

-- Day Type Ridership Aggregation to HBase
DROP TABLE IF EXISTS kjwassell_day_type_ridership_hbase;

CREATE EXTERNAL TABLE kjwassell_day_type_ridership_hbase (
    rowkey STRING,
    station_id STRING,
    stationname STRING,
    daytype STRING,
    total_rides BIGINT,
    avg_rides FLOAT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key#b,data:station_id#b,data:stationname,data:daytype,data:total_rides#b,data:avg_rides#b"
)
TBLPROPERTIES ("hbase.table.name" = "kjwassell_day_type_ridership_hbase");

INSERT OVERWRITE TABLE kjwassell_day_type_ridership_hbase
SELECT 
    CONCAT(station_id, '#', daytype) AS rowkey,
    station_id,
    stationname,
    daytype,
    total_rides,
    avg_rides
FROM kjwassell_day_type_ridership_orc;

-- Geospatial Station Ridership Aggregation to HBase
DROP TABLE IF EXISTS kjwassell_geospatial_station_ridership_hbase;

CREATE EXTERNAL TABLE kjwassell_geospatial_station_ridership_hbase (
    rowkey STRING,
    station_id STRING,
    stationname STRING,
    latitude FLOAT,
    longitude FLOAT,
    total_rides BIGINT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key#b,data:station_id#b,data:stationname,data:latitude#b,data:longitude#b,data:total_rides#b"
)
TBLPROPERTIES ("hbase.table.name" = "kjwassell_geospatial_station_ridership_hbase");

INSERT OVERWRITE TABLE kjwassell_geospatial_station_ridership_hbase
SELECT 
    station_id AS rowkey,
    station_id,
    stationname,
    latitude,
    longitude,
    total_rides
FROM kjwassell_geospatial_station_ridership_orc;

-- Monthly Ridership Aggregation to HBase
DROP TABLE IF EXISTS kjwassell_monthly_ridership_hbase;

CREATE EXTERNAL TABLE kjwassell_monthly_ridership_hbase (
    rowkey STRING,
    station_id STRING,
    stationname STRING,
    year STRING,      -- Changed to STRING for compatibility with HBase
    month STRING,     -- Changed to STRING for compatibility with HBase
    total_rides BIGINT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key#b,data:station_id#b,data:stationname,data:year#b,data:month#b,data:total_rides#b"
)
TBLPROPERTIES ("hbase.table.name" = "kjwassell_monthly_ridership_hbase");


INSERT OVERWRITE TABLE kjwassell_monthly_ridership_hbase
SELECT 
    CONCAT(station_id, '#', year, '#', month) AS rowkey,
    station_id,
    stationname,
    year,
    month,
    total_rides
FROM kjwassell_monthly_ridership_orc;



-- Top Stations by Ridership to HBase
DROP TABLE IF EXISTS kjwassell_top_stations_hbase;

CREATE EXTERNAL TABLE kjwassell_top_stations_hbase (
    rowkey STRING,
    station_id STRING,
    stationname STRING,
    total_rides BIGINT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key#b,data:station_id#b,data:stationname,data:total_rides#b"
)
TBLPROPERTIES ("hbase.table.name" = "kjwassell_top_stations_hbase");

INSERT OVERWRITE TABLE kjwassell_top_stations_hbase
SELECT 
    station_id AS rowkey,
    station_id,
    stationname,
    total_rides
FROM kjwassell_top_stations_orc;
