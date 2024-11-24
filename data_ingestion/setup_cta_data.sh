#!/bin/bash

# Define directories and files
HDFS_BASE_DIR="/kjwassell/cta_data"
LOCAL_BASE_DIR="$HOME/kjwassell/MPCS53014FinalProject/raw_data"
HQL_FILE="$HOME/kjwassell/MPCS53014FinalProject/data_ingestion/create_cta_tables.hql"
BEELINE_CONN="jdbc:hive2://10.0.0.50:10001/;transportMode=http"

# Step 1: Create HDFS directories
echo "Creating HDFS directories..."
for dir in stations ridership routes; do
    if ! hdfs dfs -test -d $HDFS_BASE_DIR/$dir; then
        hdfs dfs -mkdir -p $HDFS_BASE_DIR/$dir
    fi
done

# Step 2: Upload data to HDFS
echo "Uploading data to HDFS..."
for file in CTA_-_System_Information_-_List_of__L__Stops.csv \
            CTA_-_Ridership_-__L__Station_Entries_-_Daily_Totals.csv \
            routes.txt; do
    base_dir=$(echo $file | awk -F'_' '{print tolower($2)}') # Extract dir name like stations/ridership
    if ! hdfs dfs -test -e $HDFS_BASE_DIR/$base_dir/$file; then
        hdfs dfs -put -f $LOCAL_BASE_DIR/$file $HDFS_BASE_DIR/$base_dir/
    fi
done

# Verify upload
echo "Verifying HDFS upload..."
hdfs dfs -ls -R $HDFS_BASE_DIR

# Step 3: Execute the HiveQL file using Beeline
echo "Executing HiveQL file using Beeline..."
beeline -u "$BEELINE_CONN" -f "$HQL_FILE" > beeline_output.log 2> beeline_error.log

# Check for errors during execution
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to execute HiveQL file $HQL_FILE using Beeline."
    exit 1
else
    echo "Hive tables created and data loaded successfully."
fi

# Success message
echo "HDFS upload and Hive table setup completed successfully."
