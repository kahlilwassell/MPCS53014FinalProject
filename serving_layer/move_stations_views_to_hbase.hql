-- Drop and recreate HBase-backed tables with the correct encoding

-- Table 3: kjwassell_cta_station_view_orc to HBase
DROP TABLE IF EXISTS kjwassell_cta_station_view_hbase;

CREATE EXTERNAL TABLE kjwassell_cta_station_view_hbase (
    rowkey STRING,
    map_id STRING,
    station_name STRING,
    ada BOOLEAN,
    red BOOLEAN,
    blue BOOLEAN,
    g BOOLEAN,
    brn BOOLEAN,
    p BOOLEAN,
    pexp BOOLEAN,
    y BOOLEAN,
    pnk BOOLEAN,
    o BOOLEAN
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key#b,data:map_id#b,data:station_name,data:ada#b,data:red#b,data:blue#b,data:g#b,data:brn#b,data:p#b,data:pexp#b,data:y#b,data:pnk#b,data:o#b"
)
TBLPROPERTIES ("hbase.table.name" = "kjwassell_cta_station_view_hbase");

INSERT OVERWRITE TABLE kjwassell_cta_station_view_hbase
SELECT 
    map_id AS rowkey,
    map_id,
    station_name,
    ada,
    red,
    blue,
    g,
    brn,
    p,
    pexp,
    y,
    pnk,
    o
FROM kjwassell_cta_station_view_orc;
