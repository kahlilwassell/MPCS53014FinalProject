#!/bin/bash

# Define directories and files
HDFS_BASE_DIR="/kjwassell/cta_data"
LOCAL_BASE_DIR="$HOME/kjwassell/MPCS53014FinalProject/raw_data"
HQL_FILE="$HOME/kjwassell/MPCS53014FinalProject/data_ingestion/create_cta_stations_table.hql"
BEELINE_CONN="jdbc:hive2://10.0.0.50:10001/;transportMode=http"

# Step 1: Clean up HDFS directories
echo "Cleaning up existing HDFS files..."
hdfs dfs -rm -r -skipTrash $HDFS_BASE_DIR/*

# Step 2: Remove headers from stations file
echo "Removing headers from stations file..."
tail -n +2 "$LOCAL_BASE_DIR/CTA_-_System_Information_-_List_of__L__Stops.csv" > "$LOCAL_BASE_DIR/stations_no_header.csv"

# Step 3: Create HDFS directory for stations
echo "Creating HDFS directory for stations..."
hdfs dfs -mkdir -p $HDFS_BASE_DIR/stations

# Step 4: Upload stations data to HDFS
echo "Uploading stations data to HDFS..."
hdfs dfs -put -f "$LOCAL_BASE_DIR/stations_no_header.csv" $HDFS_BASE_DIR/stations/

# Verify upload
echo "Verifying HDFS upload for stations data..."
hdfs dfs -ls $HDFS_BASE_DIR/stations

# Step 5: Execute the HiveQL file using Beeline
echo "Executing HiveQL file using Beeline..."
beeline -u "$BEELINE_CONN" -f "$HQL_FILE" > beeline_output.log 2> beeline_error.log

# Check for errors during execution
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to execute HiveQL file $HQL_FILE using Beeline."
    exit 1
else
    echo "Hive table for stations created and data loaded successfully."
fi

# Success message
echo "Stations data setup completed successfully."
