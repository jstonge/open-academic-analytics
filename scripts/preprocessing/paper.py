"""
Paper Data Preprocessor

Cleans and filters paper records from the database to create a curated dataset
for analysis and visualization.

INPUT: oa_data_raw.db (raw paper and author data)
OUTPUT: paper.parquet (cleaned paper dataset with author metadata)

Key Features:
- Removes papers without titles and deduplicates by title + author
- Filters by accepted work types (articles, preprints, books, reports)
- Removes mislabeled articles (supplements, figures, corrections, etc.)
- Calculates coauthor counts for network analysis
- Joins with author data to include career stage information
- Prepares clean dataset for downstream coauthor and timeline analysis
"""

# Standard library imports
import sys
import os
import argparse
from pathlib import Path

# Local imports
ROOT_DIR = os.path.abspath(os.curdir)
sys.path.append(ROOT_DIR)
from scripts.modules.database_exporter import DatabaseExporter

# Paper filtering configuration
ACCEPTED_WORK_TYPES = [
    'article', 
    'preprint', 
    'book-chapter', 
    'book', 
    'report'
]

# Patterns for filtering mislabeled articles
FILTER_TITLE_PATTERNS = [
    "^Table", 
    "Appendix", 
    "Issue Cover", 
    "This Week in Science",
    "^Figure ", 
    "^Data for ", 
    "^Author Correction: ", 
    "supporting information",
    "^supplementary material", 
    "^list of contributors"
]

# DOI patterns to exclude
FILTER_DOI_PATTERNS = [
    "supplement", 
    "zenodo"
]

# Database configuration
DEFAULT_DB_NAME = "oa_data_raw.db"
OUTPUT_FILE = "paper.parquet"

# Data Cleaning Pipeline:
# 1. JOIN: paper + author tables (to get author career stage)
# 2. FILTER: Remove papers without titles
# 3. DEDUPE: Sort by date, deduplicate by (author_id, title)
# 4. FILTER: Keep only accepted work types
# 5. FILTER: Remove supplements, figures, corrections via title patterns
# 6. FILTER: Remove problematic DOIs (zenodo, supplements)
# 7. COMPUTE: Count coauthors from author list
# 8. EXPORT: Save as parquet for downstream analysis


def parse_args():
    """
    Parse command line arguments for paper preprocessing.
    
    Returns:
        argparse.Namespace: Parsed arguments with input and output paths
    """
    parser = argparse.ArgumentParser(
        "Paper Data Preprocessor",
        description="Clean and filter paper records for analysis"
    )
    parser.add_argument(
        "-i", "--input", type=Path, required=True,
        help="Input directory containing oa_data_raw.db database"
    )
    parser.add_argument(
        "-o", "--output", type=Path, required=True,
        help="Output directory for cleaned paper.parquet file"
    )
    return parser.parse_args()


def filter_mislabeled_articles(df):
    """
    Remove papers that are mislabeled as articles but are actually supplements,
    figures, corrections, or other non-research content.
    
    Args:
        df (pd.DataFrame): DataFrame with paper records
        
    Returns:
        pd.DataFrame: Filtered DataFrame
    """
    print("Filtering out mislabeled articles...")
    initial_count = len(df)
    
    # Filter by title patterns
    for pattern in FILTER_TITLE_PATTERNS:
        before_count = len(df)
        df = df[~df.title.str.contains(pattern, case=False, na=False)]
        filtered = before_count - len(df)
        if filtered > 0:
            print(f"  - Filtered {filtered} papers matching title pattern '{pattern}'")
    
    # Filter by DOI patterns
    for pattern in FILTER_DOI_PATTERNS:
        before_count = len(df)
        df = df[~df.doi.str.contains(pattern, case=False, na=False)]
        filtered = before_count - len(df)
        if filtered > 0:
            print(f"  - Filtered {filtered} papers with DOI containing '{pattern}'")
    
    total_filtered = initial_count - len(df)
    print(f"  - Total mislabeled articles removed: {total_filtered}")
    
    return df


def calculate_coauthor_counts(df):
    """
    Calculate the number of coauthors for each paper.
    
    Args:
        df (pd.DataFrame): DataFrame with paper records containing 'authors' column
        
    Returns:
        pd.DataFrame: DataFrame with added 'nb_coauthors' column
    """
    print("Computing number of coauthors...")
    df['nb_coauthors'] = df.authors.apply(
        lambda x: len(x.split(", ")) if isinstance(x, str) else 0
    )
    return df


def main():
    """
    Main preprocessing pipeline:
    1. Load raw paper data from database with author metadata
    2. Remove papers without titles
    3. Deduplicate by title and author ID
    4. Filter by accepted work types
    5. Remove mislabeled articles using title/DOI patterns
    6. Calculate number of coauthors
    7. Export cleaned dataset to parquet format
    """
    args = parse_args()
    
    # Initialize database connection
    db_path = args.input / DEFAULT_DB_NAME
    print(f"Connecting to database at {db_path}")
    db_exporter = DatabaseExporter(str(db_path))

    # Query to get papers with author metadata
    print("Querying database for papers with author metadata...")
    query = """
        SELECT p.ego_aid, a.display_name as name, p.pub_date, p.pub_year, p.title,
               p.cited_by_count, p.doi, p.wid, p.authors, p.work_type, 
               a.author_age as ego_age
        FROM paper p
        LEFT JOIN author a ON p.ego_aid = a.aid AND p.pub_year = a.pub_year
    """
    
    df = db_exporter.con.sql(query).fetchdf()
    print(f"Retrieved {len(df)} papers from database")

    # Step 1: Remove papers without titles
    print("Filtering papers without titles...")
    df = df[~df.title.isna()]
    print(f"After removing papers without titles: {len(df)} papers")

    # Step 2: Deduplicate papers by title and author ID
    print("Deduplicating papers...")
    df = df.sort_values("pub_date", ascending=False).reset_index(drop=True)
    df['title'] = df.title.str.lower()
    df = df[~df[['ego_aid', 'title']].duplicated()]
    print(f"After deduplication: {len(df)} papers")

    # Step 3: Filter by accepted work types
    print("Filtering by work types...")
    print(f"Accepted work types: {ACCEPTED_WORK_TYPES}")
    df = df[df.work_type.isin(ACCEPTED_WORK_TYPES)]
    print(f"After filtering by work type: {len(df)} papers")

    # Step 4: Remove mislabeled articles
    df = filter_mislabeled_articles(df)

    # Step 5: Calculate coauthor counts
    df = calculate_coauthor_counts(df)
    
    # Save processed data
    output_path = args.output / OUTPUT_FILE
    print(f"Saving {len(df)} processed papers to {output_path}")
    df.to_parquet(output_path)
    print("Paper preprocessing completed successfully!")
    
    # Print summary statistics
    print("\n=== Processing Summary ===")
    print(f"Final paper count: {len(df):,}")
    print(f"Unique authors: {df.ego_aid.nunique():,}")
    print(f"Year range: {df.pub_year.min()}-{df.pub_year.max()}")
    print(f"Work types: {df.work_type.value_counts().to_dict()}")
    print(f"Average coauthors per paper: {df.nb_coauthors.mean():.1f}")
    
    # Close database connection
    db_exporter.close()


if __name__ == "__main__":
    main()