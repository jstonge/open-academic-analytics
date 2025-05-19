import sys
import os
import argparse
import numpy as np
import pandas as pd
from pathlib import Path

ROOT_DIR = os.path.abspath(os.curdir)
sys.path.append(ROOT_DIR)

from scripts.modules.database_exporter import DatabaseExporter

def parse_args():
    parser = argparse.ArgumentParser("Author Data Processor")
    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        help="Input directory with database",
        required=True,
    )
    parser.add_argument(
        "-o", "--output", type=Path, help="output directory", required=True
    )
    return parser.parse_args()


def main():
    args = parse_args()
    
    
    # Initialize database exporter
    print(f"Connecting to database at {args.input / 'oa_data_raw.db'}")
    # db_exporter = DatabaseExporter("/Users/jstonge1/Documents/work/uvm/open-academic-analytics/data/raw/oa_data_raw.db")
    db_exporter = DatabaseExporter(str(args.input / "oa_data_raw.db"))
    
    # Query author data
    print("Querying author data from database")
    df = db_exporter.con.sql("SELECT * FROM author").fetchdf()
    print(f"Retrieved {len(df)} author records")
    
    # Check that required columns exist
    required_cols = ['aid', 'author_age']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"Warning: Missing required columns: {missing_cols}")
        print(f"Available columns: {df.columns.tolist()}")
    
    # Create helper function for generating random numbers with padding
    def gen_fill_char(lower, upper):
        return np.char.zfill(np.random.randint(lower, upper, len(df)).astype(str), 2)
    
    # Create standardized age representation for visualization
    print("Creating standardized age representation")
    try:
        df["age_std"] = "1" + df.author_age.astype(str).str.replace(".0", "").map(lambda x: x.zfill(3)) + "-" + gen_fill_char(1, 12) + "-" + gen_fill_char(1, 28)
        print("Successfully created age_std column")
    except Exception as e:
        print(f"Error creating age_std: {e}")
        # Provide a fallback approach
        df["age_std"] = df.apply(
            lambda row: f"1{str(int(row.author_age)).zfill(3)}-{np.random.randint(1, 12):02d}-{np.random.randint(1, 28):02d}" 
            if not pd.isna(row.author_age) else None, 
            axis=1
        )
        print("Created age_std column with fallback approach")
    
    # Save to parquet
    # output_path = "/Users/jstonge1/Documents/work/uvm/open-academic-analytics/web/data/author.parquet"
    output_path = args.output / "author.parquet"
    print(f"Saving {len(df)} processed author records to {output_path}")
    df.to_parquet(output_path)
    print("Done!")
    
    # Close database connection
    db_exporter.close()

if __name__ == "__main__":
    main()