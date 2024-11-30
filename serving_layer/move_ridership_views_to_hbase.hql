-- Drop and recreate HBase-backed tables with the correct encoding

-- Table 1: kjwassell_cta_ridership_with_day_orc to HBase
DROP TABLE IF EXISTS kjwassell_cta_ridership_with_day_hbase;

CREATE EXTERNAL TABLE kjwassell_cta_ridership_with_day_hbase (
    rowkey STRING,
    station_id STRING,
    station_name STRING,
    `date` STRING,
    day STRING,
    rides INT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key#b,data:station_id#b,data:station_name,data:`date`,data:day,data:rides#b"
)
TBLPROPERTIES ("hbase.table.name" = "kjwassell_cta_ridership_with_day_hbase");

INSERT OVERWRITE TABLE kjwassell_cta_ridership_with_day_hbase
SELECT 
    CONCAT(station_id, '_', `date`) AS rowkey,
    station_id,
    station_name,
    `date`,
    day,
    rides
FROM kjwassell_cta_ridership_with_day_orc;

-- Table 2: kjwassell_cta_avg_rides_by_day_orc to HBase
DROP TABLE IF EXISTS kjwassell_cta_avg_rides_by_day_hbase;

CREATE EXTERNAL TABLE kjwassell_cta_avg_rides_by_day_hbase (
    rowkey STRING,
    station_id STRING,
    station_name STRING,
    day STRING,
    avg_rides FLOAT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key#b,data:station_id#b,data:station_name,data:day,data:avg_rides#b"
)
TBLPROPERTIES ("hbase.table.name" = "kjwassell_cta_avg_rides_by_day_hbase");

INSERT OVERWRITE TABLE kjwassell_cta_avg_rides_by_day_hbase
SELECT 
    CONCAT(station_id, '_', day) AS rowkey,
    station_id,
    station_name,
    day,
    avg_rides
FROM kjwassell_cta_avg_rides_by_day_orc;

