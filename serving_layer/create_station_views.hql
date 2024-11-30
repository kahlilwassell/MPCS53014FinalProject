-- Drop table if it exists
DROP TABLE IF EXISTS kjwassell_cta_station_view_orc;

-- Create table for consolidated station view
CREATE TABLE kjwassell_cta_station_view_orc (
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
STORED AS ORC;

-- Populate table with aggregated data
INSERT INTO kjwassell_cta_station_view_orc
SELECT 
    map_id,
    station_name,
    MAX(ada) AS ada,
    MAX(red) AS red,
    MAX(blue) AS blue,
    MAX(g) AS g,
    MAX(brn) AS brn,
    MAX(p) AS p,
    MAX(pexp) AS pexp,
    MAX(y) AS y,
    MAX(pnk) AS pnk,
    MAX(o) AS o
FROM kjwassell_cta_stations_orc
GROUP BY map_id, station_name;
