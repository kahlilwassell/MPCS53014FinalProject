#!/bin/bash

# Variables
HQL_FILE="/path/to/translate_to_orc_tables.hql"    # Path to your HQL file
BEELINE_CMD="beeline"                             # Beeline command (ensure it's in PATH)
HIVE_SERVER="jdbc:hive2://10.0.0.50:10001/"       # Hive server connection string
TRANSPORT_MODE="http"                             # Transport mode
LOG_DIR="/kjwassell/orc_conversion_logs"          # Directory to store logs
OUTPUT_LOG="$LOG_DIR/orc_conversion_output.log"   # Path to output log
ERROR_LOG="$LOG_DIR/orc_conversion_error.log"     # Path to error log

# Create log directory if it doesn't exist
if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
fi

# Execute the HQL file using Beeline
echo "Starting ORC table conversion..."
$BEELINE_CMD -u "${HIVE_SERVER};transportMode=$TRANSPORT_MODE" -f "$HQL_FILE" > "$OUTPUT_LOG" 2> "$ERROR_LOG"

# Check for errors
if [ -s "$ERROR_LOG" ]; then
    echo "Errors occurred during ORC table conversion. Check $ERROR_LOG for details." >&2
    exit 1
else
    echo "ORC table conversion completed successfully. Check $OUTPUT_LOG for details."
    exit 0
fi
