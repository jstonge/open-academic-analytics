#!/usr/bin/env python3

# Load libraries (must be installed in environment)
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys

# Load your existing parquet file
df = pd.read_parquet("Finding Principal Investigators (PIs) - uvm_dep2col.parquet")

# Output as CSV to stdout
df.to_csv(sys.stdout, index=False)