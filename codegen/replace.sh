#!/bin/bash

# Check if query name is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <query_name>"
    echo "Example: $0 20a"
    exit 1
fi

QUERY_NAME="$1"

# Extract the numeric part and letter part from query name
# For example: "20a" -> NUM="20", LETTER="a"
if [[ $QUERY_NAME =~ ^([0-9]+)([a-z]*)$ ]]; then
    NUM="${BASH_REMATCH[1]}"
    LETTER="${BASH_REMATCH[2]}"
else
    echo "Error: Invalid query name format. Expected format like '20a', '1b', etc."
    exit 1
fi

# Construct the full query identifier (e.g., "q20")
QUERY_ID="q${NUM}"

# Construct the stats file name
STATS_FILE="stats_jsons/TTJHP_org.zhu45.treetracker.benchmark.job.${QUERY_ID}.Query${NUM}${LETTER}OptJoinTreeOptOrderingShallowHJOrdering.json"

# Construct the SQL file name
SQL_FILE="join-order-benchmark/${QUERY_NAME}.sql"

# Check if the files exist before proceeding
if [ ! -f "$STATS_FILE" ]; then
    echo "Error: Stats file not found: $STATS_FILE"
    exit 1
fi

if [ ! -f "$SQL_FILE" ]; then
    echo "Error: SQL file not found: $SQL_FILE"
    exit 1
fi

# Create junk directory if it doesn't exist
mkdir -p junk

# Change to junk directory and remove all files
cd junk
rm -f *

# Copy the files
cp "../$STATS_FILE" "../$SQL_FILE" .

echo "Successfully copied files for query $QUERY_NAME:"
echo "  Stats: $STATS_FILE"
echo "  SQL: $SQL_FILE"
echo "Files are now in the junk/ directory."
