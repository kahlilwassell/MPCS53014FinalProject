#!/bin/bash

# Define directories and files
HADOOP_BASE_DIR="/kjwassell/cta_data"
LOCAL_BASE_DIR="$HOME/kjwassell/MPCS53014FinalProject/raw_data"
HQL_FILE="$HOME/kjwassell/MPCS53014FinalProject/data_ingestion/create_cta_ridership_table.hql"
BEELINE_CONN="jdbc:hive2://10.0.0.50:10001/;transportMode=http"

# Step 1: Clean up Hadoop directories
echo "Cleaning up existing Hadoop files..."
hadoop fs -rm -r -skipTrash $HADOOP_BASE_DIR/ridership

# Step 2: Create Hadoop directory for ridership
echo "Creating Hadoop directory for ridership..."
hadoop fs -mkdir -p $HADOOP_BASE_DIR/ridership

# Step 3: Upload ridership data to Hadoop
RIDERSHIP_FILE="$LOCAL_BASE_DIR/CTA_L_Ridership_Daily_Totals.csv"
echo "Uploading ridership data to Hadoop..."
if [[ -f "$RIDERSHIP_FILE" ]]; then
    hadoop fs -put -f "$RIDERSHIP_FILE" $HADOOP_BASE_DIR/ridership/
else
    echo "Error: Ridership file $RIDERSHIP_FILE not found."
    exit 1
fi

# Verify upload
echo "Verifying Hadoop upload for ridership data..."
hadoop fs -ls $HADOOP_BASE_DIR/ridership
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to upload ridership data to Hadoop."
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

# move files and repair table for ridership
hadoop fs -put $HOME/kjwassell/MPCS53014FinalProject/raw_data/CTA_L_Ridership_Daily_Totals.csv \
wasbs://hive-mpcs53014-2024-11-10t16-50-58-559z@hivempcs53014hdistorage.blob.core.windows.net/kjwassell/cta_data/stations/
beeline -u "jdbc:hive2://10.0.0.50:10001/;transportMode=http" -e "MSCK REPAIR TABLE kjwassell_cta_ridership_csv;"

# Success message
echo "Ridership data setup completed successfully."
