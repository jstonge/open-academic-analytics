#!/bin/bash

duckdb :memory: << EOF
COPY (
  WITH expanded AS (
    SELECT *, UNNEST(STR_SPLIT(host_dept, '; ')) AS dept
    FROM read_csv_auto('src/Finding Principal Investigators (PIs) - uvm_profs_2023.csv')
    WHERE has_research_group != '999'
  ),
  joined AS (
    SELECT e.*, d.college
    FROM expanded e
    LEFT JOIN read_csv_auto('src/Finding Principal Investigators (PIs) - uvm_dept2col.csv') d
    ON TRIM(e.dept) = d.department
  )
  SELECT *
  FROM joined
) TO STDOUT WITH (FORMAT 'csv');
EOF