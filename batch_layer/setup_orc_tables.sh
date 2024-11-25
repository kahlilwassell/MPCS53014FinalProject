SELECT
    station_id,
    stationname,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(`date`, 'MM/dd/yyyy'))) AS formatted_date,
    daytype,
    rides
FROM
    kjwassell_cta_ridership_csv
LIMIT 10;
