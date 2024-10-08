#!/bin/bash

# TIMEPLUSD connection details
TIMEPLUSD_HOST="localhost"
TIMEPLUSD_PORT="8463"
TIMEPLUSD_USER="default"
TIMEPLUSD_PASSWORD=""
TIMEPLUSD_DATABASE="default"

# List of tables to clean
TABLES=(
    "trade_data"
    "another_table"
    # Add more tables as needed
)

