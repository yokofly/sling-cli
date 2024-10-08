#!/bin/bash

# Load configuration
source ./config.sh

# Get today's date in the format used in partition keys, e.g., YYYYMMDD
TODAY=$(date +%Y%m%d)
echo "Today: $TODAY"

# Function to clean partitions for a single table
clean_table() {
    local table=$1
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Processing table: $table"

    # Query to find partitions matching today's date using TabSeparatedRaw to prevent escaping
    PARTITIONS=$(./timeplusd client --host="$TIMEPLUSD_HOST" --port="$TIMEPLUSD_PORT" \
        --query="
            SELECT partition
            FROM system.parts
            WHERE table = '$table'
              AND partition LIKE '%$TODAY%'
        " --database="$TIMEPLUSD_DATABASE" --format=TabSeparatedRaw)

    echo "Retrieved partitions: $PARTITIONS"

    if [ -z "$PARTITIONS" ]; then
        echo "No partitions to drop for table: $table on date: $TODAY"
        return
    fi

    # Iterate over each partition and drop it
    while IFS= read -r partition; do
        # Retain quotes in the partition identifier
        ALTER_PARTITION="$partition"

        echo "Attempting to drop partition: $ALTER_PARTITION from table: $table"

        # Initialize retry counter
        RETRY_COUNT=0
        MAX_RETRIES=3
        SUCCESS=0

        while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
            # Execute the ALTER TABLE DROP PARTITION command
            RESULT=$(./timeplusd client --host="$TIMEPLUSD_HOST" --port="$TIMEPLUSD_PORT" \
                --query="ALTER STREAM $table DROP PARTITION $ALTER_PARTITION" \
                --database="$TIMEPLUSD_DATABASE" 2>&1)

            if [ $? -eq 0 ]; then
                echo "Successfully dropped partition: $ALTER_PARTITION from table: $table"
                SUCCESS=1
                break
            else
                RETRY_COUNT=$((RETRY_COUNT + 1))
                echo "Failed to drop partition: $ALTER_PARTITION from table: $table. Attempt $RETRY_COUNT/$MAX_RETRIES. Error: $RESULT"
                if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
                    echo "Retrying in 3 seconds..."
                    sleep 3
                fi
            fi
        done

        if [ $SUCCESS -ne 1 ]; then
            echo "Failed to drop partition: $ALTER_PARTITION from table: $table after $MAX_RETRIES attempts. Continuing to next partition."
        fi

        sleep 1

    done <<< "$PARTITIONS"
}

# Iterate over all tables and clean partitions
for table in "${TABLES[@]}"; do
    clean_table "$table"
done

echo "Partition cleanup completed on $(date)"
