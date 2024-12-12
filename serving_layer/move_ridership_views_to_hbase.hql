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

-- Table 2: kjwassell_cta_total_rides_by_day_orc to HBase
-- Drop the old HBase table
DROP TABLE IF EXISTS kjwassell_cta_total_rides_by_day_hbase;

-- Create the HBase table to store total rides and number of days
CREATE EXTERNAL TABLE kjwassell_cta_total_rides_by_day_hbase (
    rowkey STRING,
    station_id STRING,
    station_name STRING,
    day STRING,
    total_rides BIGINT, -- Total rides for this station and day
    num_days INT        -- Number of days contributing to this total
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key#b,data:station_id#b,data:station_name,data:day,data:total_rides#b,data:num_days#b"
)
TBLPROPERTIES ("hbase.table.name" = "kjwassell_cta_total_rides_by_day_hbase");


-- Insert total rides and count of days into the HBase table
INSERT OVERWRITE TABLE kjwassell_cta_total_rides_by_day_hbase
SELECT 
    CONCAT(station_id, '_', day) AS rowkey, -- Combine station_id and day to create a unique key
    station_id,
    station_name,
    day,
    total_rides,            -- Precomputed total rides
    num_days                -- Precomputed number of days
FROM kjwassell_cta_total_rides_by_day_orc; -- Use the adjusted ORC table


