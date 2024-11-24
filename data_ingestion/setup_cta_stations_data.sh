#!/bin/bash

# Define directories and files
HADOOP_BASE_DIR="/kjwassell/cta_data"
LOCAL_BASE_DIR="$HOME/kjwassell/MPCS53014FinalProject/raw_data"
HQL_FILE="$HOME/kjwassell/MPCS53014FinalProject/data_ingestion/create_cta_stations_table.hql"
BEELINE_CONN="jdbc:hive2://10.0.0.50:10001/;transportMode=http"
TABLE_NAME="kjwassell_cta_stations"

# Step 1: Clean up Hadoop directories
echo "Cleaning up existing Hadoop files..."
hadoop fs -rm -r -skipTrash $HADOOP_BASE_DIR/stations

# Step 2: Create Hadoop directory for stations
echo "Creating Hadoop directory for stations..."
hadoop fs -mkdir -p $HADOOP_BASE_DIR/stations

# Step 3: Upload stations data to Hadoop
STATIONS_FILE="$LOCAL_BASE_DIR/CTA_L_Stops.csv"
echo "Uploading stations data to Hadoop..."
if [[ -f "$STATIONS_FILE" ]]; then
    hadoop fs -put -f "$STATIONS_FILE" $HADOOP_BASE_DIR/stations/
else
    echo "Error: Stations file $STATIONS_FILE not found."
    exit 1
fi

# Verify upload
echo "Verifying Hadoop upload for stations data..."
hadoop fs -ls $HADOOP_BASE_DIR/stations
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to upload stations data to Hadoop."
    exit 1
fi

# Step 4: Execute the HiveQL file using Beeline
echo "Executing HiveQL file using Beeline..."
beeline -u "$BEELINE_CONN" -f "$HQL_FILE" > beeline_output.log 2> beeline_error.log

# Check for errors during execution
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to execute HiveQL file $HQL_FILE using Beeline."
    echo "Check beeline_error.log for details."
    exit 1
else
    echo "Hive table for stations created successfully."
fi

# Step 5: Align File Location with Hive Table Metadata
echo "Retrieving table location for $TABLE_NAME from Hive..."
TABLE_LOCATION=$(beeline -u "$BEELINE_CONN" --silent=true --outputformat=csv2 -e "DESCRIBE EXTENDED $TABLE_NAME;" | grep -i "Location" | awk -F'\t' '{print $2}')
if [[ -z "$TABLE_LOCATION" ]]; then
    echo "Error: Could not retrieve table location for $TABLE_NAME."
    exit 1
fi

echo "Table location: $TABLE_LOCATION"

# Upload file to table location
echo "Uploading stations data to table location..."
hadoop fs -put -f "$STATIONS_FILE" "$TABLE_LOCATION/"
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to upload stations data to table location $TABLE_LOCATION."
    exit 1
fi

# Step 6: Verify data visibility in Hive
echo "Verifying data in Hive table $TABLE_NAME..."
beeline -u "$BEELINE_CONN" -e "SELECT * FROM $TABLE_NAME LIMIT 10;"
if [[ $? -ne 0 ]]; then
    echo "Error: Hive query to check data failed."
    exit 1
fi

# Success message
echo "Stations data setup completed successfully."
