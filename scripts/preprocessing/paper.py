import duckdb
import pandas as pd
import numpy as np
import argparse
from pathlib import Path

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

def main():
    
    args = parse_args()
    
    # INPUT_DIR = Path("../../data/raw")
    INPUT_DIR = args.input
    # OUTPUT_DIR = Path("../../docs/data")
    OUTPUT_DIR = args.output
    
    con = duckdb.connect(str(INPUT_DIR / "oa_data_raw.db"))

    query = """
        SELECT p.ego_aid, a.display_name as name, p.pub_date, p.pub_year, p.title,
               p.cited_by_count, p.doi, p.wid, p.authors, p.work_type, 
               a.author_age as ego_age
        FROM paper p
        LEFT JOIN author a ON p.ego_aid = a.aid AND p.pub_year = a.pub_year
    """

    df = con.sql(query).fetchdf()

    # Drop papers without title
    df = df[~df.title.isna()]

    # Deduplicate papers by title and aid.
    # We grab the most recent papers.
    df = df.sort_values("pub_date", ascending=False).reset_index(drop=True)
    df['title'] = df.title.str.lower()
    df = df[~df[['ego_aid', 'title']].duplicated()]

    # df[(df.ego_aid.isna()) | (df.name.isna())]

    # Filter based on work type
    ACCEPTED_WORK_TYPES = ['article', 'preprint', 'book-chapter', 'book', 'report']
    df = df[df.work_type.isin(ACCEPTED_WORK_TYPES)]

    # Works mislabelled as article
    df = df[~df.title.str.contains("^Table", case=False)]
    df = df[~df.title.str.contains("Appendix", case=False)]
    df = df[~df.title.str.contains("Issue Cover", case=False)]
    df = df[~df.title.str.contains("This Week in Science", case=False)]
    df = df[~df.title.str.contains("^Figure ", case=False)]
    df = df[~df.title.str.contains("^Data for ", case=False)]
    df = df[~df.title.str.contains("^Author Correction: ", case=False)]
    df = df[~df.title.str.contains("supporting information", case=False)]
    df = df[~df.title.str.contains("^supplementary material", case=False)]
    df = df[~df.title.str.contains("^list of contributors", case=False)]
    df = df[~df.doi.str.contains("supplement", case=False, na=False)]
    df = df[~df.doi.str.contains("zenodo", case=False, na=False)]

    df['nb_coauthors'] = df.authors.map(lambda x: len(x.split(", ")))
    
    df.to_parquet(OUTPUT_DIR / "paper.parquet")
    
    con.close()
    
if __name__ == "__main__":
    main()