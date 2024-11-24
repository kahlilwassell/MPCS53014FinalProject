#!/bin/bash

# Define URLs and paths
DOWNLOAD_DIR="/kjwassell/cta_data"
HDFS_DIR="/kjwassell/cta_data"
LOG_FILE="/kjwassell/cta_data_upload.log"

# Array of URLs to download
URLS=(
    "https://www.transitchicago.com/download/gtfs.zip"
)

# Ensure the download directory exists
mkdir -p "$DOWNLOAD_DIR"

# Log the start time
echo "Starting data download and upload process at $(date)" >> "$LOG_FILE"

# Download files
for URL in "${URLS[@]}"; do
    FILE_NAME="gtfs.zip"  # Use correct file name for the GTFS ZIP
    FILE_PATH="$DOWNLOAD_DIR/$FILE_NAME"
    echo "Downloading $URL to $FILE_PATH..."
    curl -L -o "$FILE_PATH" "$URL"

    if [[ $? -ne 0 ]]; then
        echo "Failed to download $URL" >> "$LOG_FILE"
        continue
    fi

    echo "Successfully downloaded $URL" >> "$LOG_FILE"
done

# Handle GTFS zip file
GTFS_ZIP="$DOWNLOAD_DIR/gtfs.zip"
if [[ -f "$GTFS_ZIP" ]]; then
    echo "Unzipping $GTFS_ZIP..." >> "$LOG_FILE"
    unzip -o "$GTFS_ZIP" -d "$DOWNLOAD_DIR" >> "$LOG_FILE" 2>&1

    if [[ $? -ne 0 ]]; then
        echo "Failed to unzip $GTFS_ZIP" >> "$LOG_FILE"
    else
        echo "Successfully unzipped $GTFS_ZIP" >> "$LOG_FILE"
    fi
fi

# Upload files to HDFS
echo "Uploading files to HDFS directory: $HDFS_DIR..." >> "$LOG_FILE"
hadoop fs -mkdir -p "$HDFS_DIR"

for FILE in "$DOWNLOAD_DIR"/*; do
    FILE_NAME=$(basename "$FILE")
    HDFS_PATH="$HDFS_DIR/$FILE_NAME"
    echo "Uploading $FILE to $HDFS_PATH..."
    hadoop fs -put -f "$FILE" "$HDFS_PATH"

    if [[ $? -ne 0 ]]; then
        echo "Failed to upload $FILE to $HDFS_PATH" >> "$LOG_FILE"
    else
        echo "Successfully uploaded $FILE to $HDFS_PATH" >> "$LOG_FILE"
    fi

    # Verify upload
    hadoop fs -test -e "$HDFS_PATH"
    if [[ $? -ne 0 ]]; then
        echo "Failed to verify $HDFS_PATH in HDFS" >> "$LOG_FILE"
    else
        echo "Verified $HDFS_PATH in HDFS" >> "$LOG_FILE"
    fi
done

# Cleanup downloaded ZIP file
rm -f "$GTFS_ZIP"

# Log the end time
echo "Data upload process completed at $(date)" >> "$LOG_FILE"
