# move files and repair table
hadoop fs -put $HOME/kjwassell/MPCS53014FinalProject/raw_data/CTA_L_Stops.csv \
wasbs://hive-mpcs53014-2024-11-10t16-50-58-559z@hivempcs53014hdistorage.blob.core.windows.net/kjwassell/cta_data/stations/
beeline -u "jdbc:hive2://10.0.0.50:10001/;transportMode=http" -e "MSCK REPAIR TABLE kjwassell_cta_stations_csv;"
hadoop fs -put $HOME/kjwassell/MPCS53014FinalProject/raw_data/CTA_L_Ridership_Daily_Totals.csv \
wasbs://hive-mpcs53014-2024-11-10t16-50-58-559z@hivempcs53014hdistorage.blob.core.windows.net/kjwassell/cta_data/stations/
beeline -u "jdbc:hive2://10.0.0.50:10001/;transportMode=http" -e "MSCK REPAIR TABLE kjwassell_cta_ridership_csv;"