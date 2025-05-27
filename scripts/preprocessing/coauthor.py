import sys
import os
import argparse
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
import calendar
import random

ROOT_DIR = os.path.abspath(os.curdir)
sys.path.append(ROOT_DIR)

from scripts.modules.database_exporter import DatabaseExporter

def parse_args():
    parser = argparse.ArgumentParser("Coauthor Data Processor")
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
    
    # We want a table with ego (selected author under analysis), its coauthors, and 
    # metadata about their relationship (do they share institutions? How many times they
    # have collaborated, age difference, etc.)
    print("Querying coauthor data from database")
    query = """
        SELECT 
            c.pub_year, c.pub_date::CHAR as pub_date,
            ego_a.aid, ego_a.institution, ego_a.display_name as name, ego_a.author_age, ego_a.first_pub_year, ego_a.last_pub_year,
            c.yearly_collabo, c.all_times_collabo, c.acquaintance, c.shared_institutions,
            coauth.aid as coauth_aid, coauth.display_name as coauth_name, coauth.author_age as coauth_age, coauth.first_pub_year as coauth_min_year,
            (coauth.author_age-ego_a.author_age) AS age_diff,

        FROM 
            coauthor2 c
        LEFT JOIN 
            author coauth ON c.coauthor_aid = coauth.aid AND c.pub_year = coauth.pub_year
        LEFT JOIN 
            author ego_a ON c.ego_aid = ego_a.aid AND c.pub_year = ego_a.pub_year
        WHERE 
            c.pub_year < 2024
        ORDER BY c.pub_year
    """

    df = db_exporter.con.sql(query).fetchdf()
    print(f"Retrieved {len(df)} coauthor relationships")

    # Check for required columns
    print("Checking data structure")
    required_cols = ['aid', 'name', 'author_age', 'coauth_age', 'age_diff', 'pub_date']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"Warning: Missing required columns: {missing_cols}")
        print(f"Available columns: {df.columns.tolist()}")
    
    # Filter rows with valid author_age
    print("Filtering and processing data")
    df = df[~df.author_age.isna()]
    df['author_age'] = df.author_age.astype(int)
    print(f"After filtering: {len(df)} relationships")
    
    # Handle incorrect coauthor min years
    # OpenAlex often thinks first works of a given author is much earlier than it really is
    print("Correcting coauthor publication years")
    df['coauth_min_year'] = df['coauth_min_year'].where(df['coauth_min_year'] >= 1950)
    df['coauth_age'] = df.pub_year - df.coauth_min_year
    df['age_diff'] = df.coauth_age - df.author_age
    print(f"Corrected {df.coauth_min_year.isna().sum()} missing coauthor min years")
    
    # Bucketize age difference
    print("Creating age buckets")
    agg_diff_vec = df.age_diff.to_numpy()
    categories = np.empty(agg_diff_vec.shape, dtype=object)

    categories[agg_diff_vec < -15] = "much_younger"
    categories[(agg_diff_vec >= -15) & (agg_diff_vec < -7)] = "younger"
    categories[(agg_diff_vec >= -7) & (agg_diff_vec < 7)] = "same_age"
    categories[(agg_diff_vec >= 7) & (agg_diff_vec < 15)] = "older"
    categories[agg_diff_vec >= 15] = "much_older"

    df['age_bucket'] = categories
    
    # Verify age buckets match missing values
    assert len(df[df['age_bucket'].isna()]) == len(df[df['coauth_age'].isna()])
    
    # Create standardized age representation
    print("Creating standardized age representation")
    try:
        df["age_std"] = "1" + df.author_age.astype(str).map(lambda x: x.zfill(3)) + "-" + df.pub_date.map(lambda x: "-".join(x.split("-")[-2:]))
        df["age_std"] = df.age_std.map(lambda x: x.replace("29", "28") if x.endswith("29") else x)
    except Exception as e:
        print(f"Error creating age_std: {e}")
        # Fallback approach
        df["age_std"] = df.apply(
            lambda row: f"1{str(int(row.author_age)).zfill(3)}-{row.pub_date.split('-')[1]}-{row.pub_date.split('-')[2]}" 
            if not pd.isna(row.author_age) and isinstance(row.pub_date, str) else None, 
            axis=1
        )
        df["age_std"] = df.age_std.map(lambda x: x.replace("29", "28") if x and x.endswith("29") else x)
        print("Created age_std column with fallback approach")
    
    # Save to parquet
    #  output_path = "/Users/jstonge1/Documents/work/uvm/open-academic-analytics/web/data/coauthor.parquet"
    output_path = args.output / "coauthor.parquet"
    print(f"Saving {len(df)} processed coauthor relationships to {output_path}")
    df.to_parquet(output_path)
    print("Done!")
    
    # Close database connection
    db_exporter.close()

if __name__ == '__main__':
    main()