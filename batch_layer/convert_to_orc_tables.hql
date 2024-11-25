DROP TABLE IF EXISTS kjwassell_cta_ridership_orc;
# make an orc version of the ridership table
CREATE TABLE kjwassell_cta_ridership_orc (
    station_id STRING,
    stationname STRING,
    `date` DATE,
    daytype STRING,
    rides INT
)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="ZLIB");;

# insert data from the csv table with correct type.
INSERT INTO kjwassell_cta_ridership_orc
SELECT
    station_id,
    stationname,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(`date`, 'MM/dd/yyyy'))) AS DATE,
    daytype,
    CAST(rides AS INT)
FROM
    kjwassell_cta_ridership_csv;

DROP TABLE IF EXISTS kjwassell_cta_stations_orc;
# make an orc version of the stations table
CREATE TABLE kjwassell_cta_stations_orc (
    stop_id STRING,
    direction_id STRING,
    stop_name STRING,
    station_name STRING,
    station_descriptive_name STRING,
    map_id STRING,
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
STORED AS ORC
TBLPROPERTIES ("orc.compress"="ZLIB");;

INSERT INTO kjwassell_cta_stations_orc
SELECT
    stop_id,
    direction_id,
    stop_name,
    station_name,
    station_descriptive_name,
    map_id,
    CAST(ada AS BOOLEAN),
    CAST(red AS BOOLEAN),
    CAST(blue AS BOOLEAN),
    CAST(g AS BOOLEAN),
    CAST(brn AS BOOLEAN),
    CAST(p AS BOOLEAN),
    CAST(pexp AS BOOLEAN),
    CAST(y AS BOOLEAN),
    CAST(pnk AS BOOLEAN),
    CAST(o AS BOOLEAN),
    CAST(latitude AS FLOAT),
    CAST(longitude AS FLOAT)
FROM
    kjwassell_cta_stations_csv;

