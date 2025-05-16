#!/bin/bash

# Define input files (already in the local directory)
CSV1="Finding Principal Investigators (PIs) - uvm_profs_2023.csv"
CSV2="Finding Principal Investigators (PIs) - uvm_dept2col.csv"

# Use the data loader cache directory to store the processed data
TMPDIR="docs/.observablehq/cache/"

# Create cache directory if it doesn't exist
mkdir -p "$TMPDIR"

# Copy the files to the cache directory if not already there
if [ ! -f "$TMPDIR/uvm_profs_2023.csv" ]; then
  cp "./$CSV1" "$TMPDIR/uvm_profs_2023.csv"
fi

if [ ! -f "$TMPDIR/uvm_dept2col.csv" ]; then
  cp "./$CSV2" "$TMPDIR/uvm_dept2col.csv"
fi

# Generate a CSV file using DuckDB
duckdb :memory: << EOF
COPY (
  WITH expanded AS (
    SELECT *, UNNEST(STR_SPLIT(host_dept, '; ')) AS dept
    FROM read_csv_auto('$TMPDIR/uvm_profs_2023.csv')
    WHERE has_research_group != '999'
  ),
  joined AS (
    SELECT e.*, d.college
    FROM expanded e
    LEFT JOIN read_csv_auto('$TMPDIR/uvm_dept2col.csv') d
    ON TRIM(e.dept) = d.department
  )
  SELECT *
  FROM joined
) TO STDOUT WITH (FORMAT 'csv');
EOF