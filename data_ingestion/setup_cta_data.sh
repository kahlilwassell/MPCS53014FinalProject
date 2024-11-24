#!/bin/bash

# Define directories and files
HDFS_BASE_DIR="/kjwassell/cta_data"
LOCAL_BASE_DIR="$HOME/kjwassell/MPCS53014FinalProject/raw_data"
HQL_FILE="$HOME/kjwassell/MPCS53014FinalProject/data_ingestion/create_cta_tables.hql"
BEELINE_CONN="jdbc:hive2://10.0.0.50:10001/;transportMode=http"

# Step 1: Clean up HDFS directories
echo "Cleaning up existing HDFS files..."
hdfs dfs -rm -r -skipTrash $HDFS_BASE_DIR/*

# Step 2: Remove headers from local files
echo "Removing headers from local files..."
tail -n +2 "$LOCAL_BASE_DIR/CTA_-_System_Information_-_List_of__L__Stops.csv" > "$LOCAL_BASE_DIR/stations_no_header.csv"
tail -n +2 "$LOCAL_BASE_DIR/CTA_-_Ridership_-__L__Station_Entries_-_Daily_Totals.csv" > "$LOCAL_BASE_DIR/ridership_no_header.csv"
tail -n +2 "$LOCAL_BASE_DIR/routes.txt" > "$LOCAL_BASE_DIR/routes_no_header.txt"

# Step 3: Reformat dates in ridership data
echo "Reformatting dates in ridership data..."
awk -F',' 'BEGIN {OFS=","} NR==1 {print $0} NR>1 {split($3, d, "/"); print $1, $2, d[3]"-"d[1]"-"d[2], $4, $5}' \
"$LOCAL_BASE_DIR/ridership_no_header.csv" > "$LOCAL_BASE_DIR/ridership_reformatted.csv"

# Step 4: Create HDFS directories
echo "Creating HDFS directories..."
for dir in stations ridership routes; do
    if ! hdfs dfs -test -d $HDFS_BASE_DIR/$dir; then
        hdfs dfs -mkdir -p $HDFS_BASE_DIR/$dir
    fi
done

# Step 5: Upload data to HDFS
echo "Uploading data to HDFS..."
declare -A file_to_dir=(
    ["stations_no_header.csv"]="stations"
    ["ridership_reformatted.csv"]="ridership"
    ["routes_no_header.txt"]="routes"
)

for file in "${!file_to_dir[@]}"; do
    target_dir="${file_to_dir[$file]}"
    hdfs dfs -put -f "$LOCAL_BASE_DIR/$file" $HDFS_BASE_DIR/$target_dir/
done

# Verify upload
echo "Verifying HDFS upload..."
hdfs dfs -ls -R $HDFS_BASE_DIR

# Step 6: Execute the HiveQL file using Beeline
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
