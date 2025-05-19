import sys
import os
import argparse
from pathlib import Path

ROOT_DIR = os.path.abspath(os.curdir)
sys.path.append(ROOT_DIR)

from scripts.modules.database_exporter import DatabaseExporter

def parse_args():
    parser = argparse.ArgumentParser("Paper Preprocessor")
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
    # db_exporter = DatabaseExporter("/Users/jstonge1/Documents/work/uvm/open-academic-analytics/data/raw/oa_data_raw.db")
    print(f"Connecting to database at {args.input / 'oa_data_raw.db'}")
    db_exporter = DatabaseExporter(str(args.input / "oa_data_raw.db"))

    # Query to get papers with author age
    query = """
        SELECT p.ego_aid, a.display_name as name, p.pub_date, p.pub_year, p.title,
               p.cited_by_count, p.doi, p.wid, p.authors, p.work_type, 
               a.author_age as ego_age
        FROM paper p
        LEFT JOIN author a ON p.ego_aid = a.aid AND p.pub_year = a.pub_year
    """

    print("Querying database for papers...")
    df = db_exporter.con.sql(query).fetchdf()
    print(f"Retrieved {len(df)} papers from database")

    # Step 1: Drop papers without title
    print("Filtering papers...")
    df = df[~df.title.isna()]
    print(f"After dropping papers without title: {len(df)} papers")

    # Step 2: Deduplicate papers by title and aid
    df = df.sort_values("pub_date", ascending=False).reset_index(drop=True)
    df['title'] = df.title.str.lower()
    df = df[~df[['ego_aid', 'title']].duplicated()]
    print(f"After deduplication: {len(df)} papers")

    # Step 3: Filter based on work type
    ACCEPTED_WORK_TYPES = ['article', 'preprint', 'book-chapter', 'book', 'report']
    df = df[df.work_type.isin(ACCEPTED_WORK_TYPES)]
    print(f"After filtering by work type: {len(df)} papers")

    # Step 4: Filter out works mislabelled as article
    print("Filtering out mislabeled articles...")
    filter_patterns = [
        "^Table", "Appendix", "Issue Cover", "This Week in Science",
        "^Figure ", "^Data for ", "^Author Correction: ", "supporting information",
        "^supplementary material", "^list of contributors"
    ]
    
    for pattern in filter_patterns:
        before_count = len(df)
        df = df[~df.title.str.contains(pattern, case=False, na=False)]
        filtered = before_count - len(df)
        if filtered > 0:
            print(f"  - Filtered {filtered} papers matching '{pattern}'")
    
    # Filter DOIs containing certain patterns
    for pattern in ["supplement", "zenodo"]:
        before_count = len(df)
        df = df[~df.doi.str.contains(pattern, case=False, na=False)]
        filtered = before_count - len(df)
        if filtered > 0:
            print(f"  - Filtered {filtered} papers with DOI containing '{pattern}'")

    # Step 5: Count coauthors
    print("Computing number of coauthors...")
    df['nb_coauthors'] = df.authors.apply(lambda x: len(x.split(", ")) if isinstance(x, str) else 0)
    
    # Save to parquet
    output_path = args.output / "paper.parquet"
    print(f"Saving {len(df)} processed papers to {output_path}")
    df.to_parquet(output_path)
    print("Done!")
    
    # Close database connection
    db_exporter.close()
    
if __name__ == "__main__":
    main()