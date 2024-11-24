#!/bin/bash

# Define directories and files
HDFS_BASE_DIR="/kjwassell/cta_data"
LOCAL_BASE_DIR="~/kjwassell/MPCS53014FinalProject/raw_data"
HQL_FILE="/path/to/create_cta_tables.hql"
BEELINE_CONN="jdbc:hive2://10.0.0.50:10001/;transportMode=http"

# Step 1: Create HDFS directories
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p $HDFS_BASE_DIR/stations
hdfs dfs -mkdir -p $HDFS_BASE_DIR/ridership
hdfs dfs -mkdir -p $HDFS_BASE_DIR/routes

# Step 2: Upload data to HDFS
echo "Uploading data to HDFS..."
hdfs dfs -put -f $LOCAL_BASE_DIR/CTA_-_System_Information_-_List_of__L__Stops.csv $HDFS_BASE_DIR/stations/
hdfs dfs -put -f $LOCAL_BASE_DIR/CTA_-_Ridership_-__L__Station_Entries_-_Daily_Totals.csv $HDFS_BASE_DIR/ridership/
hdfs dfs -put -f $LOCAL_BASE_DIR/routes.txt $HDFS_BASE_DIR/routes/

# Verify upload
echo "Verifying HDFS upload..."
hdfs dfs -ls $HDFS_BASE_DIR/stations
hdfs dfs -ls $HDFS_BASE_DIR/ridership
hdfs dfs -ls $HDFS_BASE_DIR/routes

# Step 3: Execute the HiveQL file using Beeline
echo "Executing HiveQL file using Beeline..."
beeline -u "$BEELINE_CONN" -f "$HQL_FILE"

# Check for errors during execution
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to execute HiveQL file $HQL_FILE using Beeline."
    exit 1
else
    echo "Hive tables created and data loaded successfully."
fi

# Success message
echo "HDFS upload and Hive table setup completed successfully."