import duckdb
import sys

# Create a connection to an in-memory database
conn = duckdb.connect(":memory:")

# Execute the query and output the results directly to stdout
conn.execute("""
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
""").df().to_csv(sys.stdout, index=False)

# Close the connection
conn.close()