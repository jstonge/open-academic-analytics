import duckdb
import pandas as pd
import numpy as np
import argparse
from pathlib import Path
from datetime import datetime
import calendar
import random

def parse_args():
    parser = argparse.ArgumentParser("Data Downloader")
    parser.add_argument(
        "-i",
        "--input",
        type= Path,
        help="JSONlines file with urls and hashes",
        required=True,
    )
    parser.add_argument(
        "-o", "--output", type=Path, help="output directory", required=True
    )
    return parser.parse_args()

def shuffle_date_within_month(date_str):
    # Parse the date string to a datetime object
    date = datetime.strptime(date_str, "%Y-%m-%d")

    # Get the number of days in the month of the given date
    _, num_days_in_month = calendar.monthrange(date.year, date.month)

    # Generate a random day within the same month
    random_day = random.randint(1, num_days_in_month)

    # Create a new date with the randomly chosen day
    shuffled_date = date.replace(day=random_day)

    # Return the date in the desired format
    return shuffled_date.strftime("%Y-%m-%d")

def main():
    
    args = parse_args()
    
    # INPUT_DIR = Path("../../data/raw")
    INPUT_DIR = args.input
    
    con = duckdb.connect(str(INPUT_DIR / "oa_data_raw.db"))
    
    # We want a table with ego (selected author under analysis), its coauthors, and 
    # metadata about their relationship (do they share institutions? How many times they
    # have collaborated, age difference, etc.)
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

    df = con.sql(query).fetchdf()

    # making sure
    df = df[~df.author_age.isna()]
    df['author_age'] = df.author_age.astype(int)
    
    # BIG MOVE. 
    # After inspection, many coauthor min years are most likely wrong.
    # OpenAlex very often think that first works of a given author is much earlier than it really is.
    # For now, we simply say that all coauthors before 1950 are wrong. We will try to fix that later.
    df['coauth_min_year'] = np.where(df.coauth_min_year < 1950, None, df.coauth_min_year)
    df['coauth_age'] = df.pub_year - df.coauth_min_year
    df['age_diff'] = df.coauth_age - df.author_age

    # df[df['coauthor_age'].isna()] # 6K rows we need to fix
    
    # bucketize age
    agg_diff_vec = df.age_diff.to_numpy()
    categories = np.empty(agg_diff_vec.shape, dtype=object)

    categories[agg_diff_vec < -15] = "much_younger"
    categories[(agg_diff_vec >= -15) & (agg_diff_vec < -7)] = "younger"
    categories[(agg_diff_vec >= -7) & (agg_diff_vec < 7)] = "same_age"
    categories[(agg_diff_vec >= 7) & (agg_diff_vec < 15)] = "older"
    categories[agg_diff_vec >= 15] = "much_older"

    df['age_bucket'] = categories

    assert len(df[df['age_bucket'].isna()]) == len(df[df['coauth_age'].isna()])

    df["age_std"] = "1"+df.author_age.astype(str).map(lambda x: x.zfill(3))+"-"+df.pub_date.map(lambda x: "-".join(x.split("-")[-2:]))
    df["age_std"] = df.age_std.map(lambda x: x.replace("29", "28") if x.endswith("29") else x)  
    
    df.to_parquet (args.output / "coauthor.parquet")
    
    con.close()

if __name__ == '__main__':
    main()