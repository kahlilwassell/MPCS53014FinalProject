-- Drop the HBase table if it already exists
DROP TABLE IF EXISTS kjwassell_station_name_to_station_id_hbase;

-- Create the HBase table mapping in Hive
CREATE EXTERNAL TABLE kjwassell_station_name_to_station_id_hbase (
    rowkey STRING,  -- Plain text row key for station_name
    station_id STRING -- Station ID mapped to the station name
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,data:station_id"  -- Use plain text for station_name and station_id
)
TBLPROPERTIES ("hbase.table.name" = "kjwassell_station_name_to_station_id_hbase");

-- Insert data into HBase-backed table
INSERT OVERWRITE TABLE kjwassell_station_name_to_station_id_hbase
SELECT 
    station_name AS rowkey,  -- Use station_name as the row key
    station_id  -- Station ID as a field
FROM kjwassell_cta_ridership_with_day_hbase;
