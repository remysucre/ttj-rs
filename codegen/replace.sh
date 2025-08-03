#!/bin/bash

# Check if query names are provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <query_name1> [query_name2] [query_name3] ..."
    echo "Example: $0 20a"
    echo "Example: $0 1a 2b 3c"
    exit 1
fi

# Function to process a single query
process_query() {
    local QUERY_NAME="$1"
    
    # Extract the numeric part and letter part from query name
    # For example: "20a" -> NUM="20", LETTER="a"
    if [[ $QUERY_NAME =~ ^([0-9]+)([a-z]*)$ ]]; then
        NUM="${BASH_REMATCH[1]}"
        LETTER="${BASH_REMATCH[2]}"
    else
        echo "Error: Invalid query name format '$QUERY_NAME'. Expected format like '20a', '1b', etc."
        return 1
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
        return 1
    fi

    if [ ! -f "$SQL_FILE" ]; then
        echo "Error: SQL file not found: $SQL_FILE"
        return 1
    fi

    # Copy the files to junk directory
    cp "$STATS_FILE" "junk/"
    cp "$SQL_FILE" "junk/"
    
    echo "  ✓ Copied files for query $QUERY_NAME"
    return 0
}

# Create junk directory if it doesn't exist
mkdir -p junk

# Change to junk directory and remove all files
cd junk
rm -f *
cd ..

echo "Processing queries: $@"
echo

# Process each query name
SUCCESSFUL_QUERIES=()
FAILED_QUERIES=()

for QUERY_NAME in "$@"; do
    if process_query "$QUERY_NAME"; then
        SUCCESSFUL_QUERIES+=("$QUERY_NAME")
    else
        FAILED_QUERIES+=("$QUERY_NAME")
    fi
done

echo
echo "Summary:"
echo "Successfully processed ${#SUCCESSFUL_QUERIES[@]} queries: ${SUCCESSFUL_QUERIES[*]}"
if [ ${#FAILED_QUERIES[@]} -gt 0 ]; then
    echo "Failed to process ${#FAILED_QUERIES[@]} queries: ${FAILED_QUERIES[*]}"
    exit 1
else
    echo "All queries processed successfully!"
    echo "Files are now in the junk/ directory."
fi
