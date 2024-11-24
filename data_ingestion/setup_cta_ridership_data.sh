#!/bin/bash

# Define directories and files
HDFS_BASE_DIR="/kjwassell/cta_data"
LOCAL_BASE_DIR="$HOME/kjwassell/MPCS53014FinalProject/raw_data"
HQL_FILE="$HOME/kjwassell/MPCS53014FinalProject/data_ingestion/create_cta_ridership_table.hql"
BEELINE_CONN="jdbc:hive2://10.0.0.50:10001/;transportMode=http"

# Step 1: Clean up HDFS directories
echo "Cleaning up existing HDFS files..."
hdfs dfs -rm -r -skipTrash $HDFS_BASE_DIR/*

# Step 2: Create HDFS directory for ridership
echo "Creating HDFS directory for ridership..."
hdfs dfs -mkdir -p $HDFS_BASE_DIR/ridership

# Step 3: Upload ridership data to HDFS
RIDERSHIP_FILE="$LOCAL_BASE_DIR/CTA_L_Ridership_Daily_Totals.csv"
echo "Uploading ridership data to HDFS..."
if [[ -f "$RIDERSHIP_FILE" ]]; then
    hdfs dfs -put -f "$RIDERSHIP_FILE" $HDFS_BASE_DIR/ridership/
else
    echo "Error: ridership file $RIDERSHIP_FILE not found."
    exit 1
fi

# Verify upload
echo "Verifying HDFS upload for ridership data..."
hdfs dfs -ls $HDFS_BASE_DIR/ridership
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to upload ridership data to HDFS."
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
    echo "Hive table for ridership created and data loaded successfully."
fi

# Success message
echo "Ridership data setup completed successfully."
