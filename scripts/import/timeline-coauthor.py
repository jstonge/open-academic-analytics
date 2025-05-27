"""
Coauthor Timeline Processor

Analyzes academic collaboration patterns over time by processing paper data
and generating coauthor relationship records with temporal metadata.

INPUT: paper.parquet (processed papers) + author data from oa_data_raw.db
OUTPUT: coauthor records in database with PRIMARY KEY (ego_aid, coauthor_aid, pub_year)

Key Features:
- Tracks collaboration frequency (yearly vs all-time)
- Classifies relationship types (new_collab, existing_collab, new_collab_of_collab)
- Identifies shared institutional affiliations
- Handles temporal sequencing for network evolution analysis
- Creates lookup tables for performance optimization
- Processes authors sequentially to build collaboration histories
- Assigns randomized dates within publication years for visualization
"""

# Standard library imports
import calendar
import random
import sys
import os
from pathlib import Path
from datetime import datetime
from collections import Counter

# Third-party imports
import pandas as pd
from tqdm import tqdm
import argparse

# Local imports
ROOT_DIR = os.path.abspath(os.curdir)
sys.path.append(ROOT_DIR)
from scripts.modules.database_exporter import DatabaseExporter
from scripts.modules.utils import shuffle_date_within_month

# Database configuration
DEFAULT_DB_NAME = "oa_data_raw.db"
PAPER_FILE = "paper.parquet"

# Collaboration categories
COLLAB_TYPES = {
    'NEW': 'new_collab',
    'NEW_THROUGH_MUTUAL': 'new_collab_of_collab', 
    'EXISTING': 'existing_collab'
}

# Processing settings
BATCH_SIZE = 1000
PROGRESS_REPORT_INTERVAL = 10

# Data validation settings
MIN_VALID_YEAR = 1950

# Collaboration Classification Logic:
# - new_collab: First-time collaboration between ego and coauthor
# - new_collab_of_collab: New collaborator who is connected through existing collaborators
# - existing_collab: Ongoing collaboration from previous years
#
# Temporal Processing:
# 1. For each target author, process years sequentially
# 2. Track all-time collaborators vs new collaborators each year
# 3. Identify connections through mutual collaborators
# 4. Calculate collaboration frequencies and institutional overlap


def parse_args():
    """
    Parse command line arguments for coauthor timeline processing.
    
    Returns:
        argparse.Namespace: Parsed arguments with input and output paths
    """
    parser = argparse.ArgumentParser(
        "Coauthor Timeline Processor",
        description="Generate temporal coauthor relationship data from academic papers"
    )
    parser.add_argument(
        "-i", "--input", type=Path, required=True,
        help="Input directory containing paper.parquet with processed papers"
    )
    parser.add_argument(
        "-o", "--output", type=Path, required=True,
        help="Output directory containing oa_data_raw.db database"
    )
    return parser.parse_args()


def load_and_validate_data(args):
    """
    Load paper and author data, validate integrity.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        tuple: (db_exporter, df_pap, df_auth) - database connection and dataframes
    """
    # Initialize database connection
    db_path = args.output / DEFAULT_DB_NAME
    print(f"Connecting to database at {db_path}")
    db_exporter = DatabaseExporter(str(db_path))
    
    # Load processed papers
    paper_path = args.input / PAPER_FILE
    print(f"Loading paper data from {paper_path}")
    
    try:
        df_pap = pd.read_parquet(paper_path)
        print(f"Loaded {len(df_pap):,} papers")
    except Exception as e:
        print(f"Error loading paper data: {e}")
        db_exporter.close()
        raise
    
    # Load author data from database
    print("Loading author data from database")
    try:
        df_auth = db_exporter.con.sql("SELECT * from author").fetchdf()
        print(f"Loaded {len(df_auth):,} author records")
    except Exception as e:
        print(f"Error loading author data: {e}")
        db_exporter.close()
        raise
    
    return db_exporter, df_pap, df_auth


def create_optimization_lookups(df_auth):
    """
    Create lookup dictionaries for performance optimization.
    
    Args:
        df_auth (pd.DataFrame): Author data
        
    Returns:
        tuple: (target2info, coaut2info) - lookup dictionaries
    """
    print("Creating lookup tables for optimization...")
    
    # Create lookup for target author information by (aid, pub_year)
    target2info = df_auth[['aid', 'pub_year', 'institution', 'author_age']]\
                        .set_index(['aid', 'pub_year'])\
                        .apply(tuple, axis=1).to_dict()
    
    # Create lookup for coauthor information by (display_name, pub_year)
    coaut2info = df_auth[['display_name', 'pub_year', 'institution', 'aid']]\
                        .set_index(['display_name', 'pub_year'])\
                        .apply(tuple, axis=1).to_dict()
    
    print(f"Created lookup tables with {len(target2info):,} target entries and {len(coaut2info):,} coauthor entries")
    
    return target2info, coaut2info


def get_target_authors(df_pap):
    """
    Extract list of target authors to process.
    
    Args:
        df_pap (pd.DataFrame): Paper data
        
    Returns:
        pd.DataFrame: Target authors with ego_aid and name
    """
    targets = df_pap[['ego_aid', 'name']].drop_duplicates()
    print(f"Processing {len(targets):,} target authors")
    
    return targets


def get_author_publication_years(df_pap, target_aid):
    """
    Get publication years for a specific author from paper dataframe.
    
    Args:
        df_pap (pd.DataFrame): Paper dataframe
        target_aid (str): Author ID
        
    Returns:
        list: List of publication years
    """
    author_papers = df_pap[df_pap['ego_aid'] == target_aid]
    years = sorted(author_papers['pub_year'].unique())
    
    return years


def process_author_year(df_pap, target_aid, target_name, yr, target_info, 
                       coaut2info, set_all_collabs, all_time_collabo, 
                       set_collabs_of_collabs_never_worked_with):
    """
    Process coauthor relationships for a specific author and year.
    
    Args:
        df_pap (pd.DataFrame): Paper dataframe
        target_aid (str): Target author ID
        target_name (str): Target author name
        yr (int): Publication year to process
        target_info (tuple): Target author info (institution, age)
        coaut2info (dict): Coauthor lookup dictionary
        set_all_collabs (set): All-time collaborators
        all_time_collabo (dict): All-time collaboration counts
        set_collabs_of_collabs_never_worked_with (set): Indirect connections
        
    Returns:
        tuple: (coauthors, dates_in_year, new_collabs_this_year, time_collabo)
    """
    target_institution, auth_age = target_info
    
    # Initialize yearly tracking variables
    dates_in_year = []
    new_collabs_this_year = set()
    collabs_of_collabs_time_t = set()
    coauthName2aid = {}
    time_collabo = {}
    
    # Get papers for this year from dataframe
    works = df_pap[(df_pap['ego_aid'] == target_aid) & (df_pap['pub_year'] == yr)]
    
    if len(works) == 0:
        return [], dates_in_year, new_collabs_this_year, time_collabo
        
    print(f"  Processing {len(works)} papers for {target_name} in {yr}")

    for i, w in works.iterrows():
        # Add temporal noise for visualization
        try:
            pub_date = datetime.strptime(str(w['pub_date']), "%Y-%m-%d  %H:%M:%S")
            shuffled_date = shuffle_date_within_month(pub_date)
            dates_in_year.append(shuffled_date)
        except Exception as e:
            print(f"Error processing date {w['pub_date']}: {e}")
            shuffled_date = f"{yr}-01-01"
            dates_in_year.append(shuffled_date)

        # Process each coauthor
        if 'authors' not in w or pd.isna(w['authors']):
            print(f"Warning: Missing authors for paper {w['title']}")
            continue
            
        for coauthor_name in w['authors'].split(", "):
            if coauthor_name != w['name']:
                # Update collaboration count
                author_yearly_data = time_collabo.get(coauthor_name, {'count': 0, 'institutions': {}})
                author_yearly_data['count'] += 1

                # Get coauthor info
                coauthor_info = coaut2info.get((coauthor_name, yr))
                
                if coauthor_info is None:
                    time_collabo.pop(coauthor_name, None)
                    continue

                # Extract institution and aid
                inst_name, coauthor_aid = coauthor_info
                
                # Update institution tracking
                author_yearly_data['institutions'][inst_name] = author_yearly_data['institutions'].get(inst_name, 0) + 1

                # Update collaboration trackers
                time_collabo[coauthor_name] = author_yearly_data
                all_time_collabo[coauthor_name] = all_time_collabo.get(coauthor_name, 0) + 1

                # Store coauthor ID
                if coauthName2aid.get(coauthor_name) is None:
                    coauthName2aid[coauthor_name] = coauthor_aid

                # Track new collaborators
                if coauthor_name not in set_all_collabs:
                    new_collabs_this_year.add(coauthor_name)

    # Update indirect connections
    set_collabs_of_collabs_never_worked_with.update(
        collabs_of_collabs_time_t - new_collabs_this_year - set_all_collabs - set([target_name])
    )
    
    # Process yearly collaboration statistics
    coauthors = []
    if len(time_collabo) > 0:
        print(f"  Processing {len(time_collabo)} coauthors for {target_name} in {yr}")

        for coauthor_name, coauthor_data in time_collabo.items():
            coauthor_aid = coauthName2aid[coauthor_name]
            
            # Determine collaboration type
            if coauthor_name in (new_collabs_this_year - set_all_collabs):
                if coauthor_name in set_collabs_of_collabs_never_worked_with:
                    subtype = COLLAB_TYPES['NEW_THROUGH_MUTUAL']
                else:
                    subtype = COLLAB_TYPES['NEW']
            else:
                subtype = COLLAB_TYPES['EXISTING']

            # Assign publication date
            author_date = random.choice(dates_in_year) if dates_in_year else f"{yr}-01-01"
            
            # Create standardized age date for visualization
            shuffled_auth_age = "1" + author_date.replace(author_date.split("-")[0], str(auth_age).zfill(3))
            # Handle leap year edge case
            shuffled_auth_age = shuffled_auth_age.replace("29", "28") if shuffled_auth_age.endswith("29") else shuffled_auth_age

            # Determine shared institution
            shared_inst = None
            max_institution = None

            if coauthor_data['institutions'] and target_institution:
                max_institution = max(coauthor_data['institutions'], key=coauthor_data['institutions'].get)
                if max_institution == target_institution:
                    shared_inst = max_institution

            # Create coauthor record
            coauthors.append((
                target_aid,
                author_date, int(author_date[0:4]),
                coauthor_aid, coauthor_name, subtype,
                coauthor_data['count'], all_time_collabo[coauthor_name],
                shared_inst, max_institution
            ))
    
    return coauthors, dates_in_year, new_collabs_this_year, time_collabo


def process_single_author(df_pap, target_aid, target_name, target2info, coaut2info, existing_records):
    """
    Process all coauthor relationships for a single target author across all years.
    
    Args:
        df_pap (pd.DataFrame): Paper dataframe
        target_aid (str): Target author ID
        target_name (str): Target author name
        target2info (dict): Target author lookup
        coaut2info (dict): Coauthor lookup
        existing_records (set): Existing coauthor records to avoid duplicates
        
    Returns:
        list: New coauthor records to insert
    """
    if pd.isna(target_name):
        print(f"Warning: Name is missing for author {target_aid}, using ID as name")
        target_name = target_aid
        
    print(f"Processing {target_name} ({target_aid})")

    # Get publication years
    years = get_author_publication_years(df_pap, target_aid)
    if not years:
        print(f"No publication years found for {target_name}, skipping")
        return []
        
    print(f"Found publications in years: {years}")

    # Initialize tracking variables
    all_coauthors = []
    set_all_collabs = set()
    all_time_collabo = {}
    set_collabs_of_collabs_never_worked_with = set()

    # Process each year sequentially
    for yr in years:
        # Get target author info for this year
        target_info = target2info.get((target_aid, yr))
        if target_info is None:
            print(f"Missing info for {target_name} in {yr}")
            continue
        
        # Process this year's collaborations
        coauthors, dates_in_year, new_collabs_this_year, time_collabo = process_author_year(
            df_pap, target_aid, target_name, yr, target_info, 
            coaut2info, set_all_collabs, all_time_collabo, 
            set_collabs_of_collabs_never_worked_with
        )
        
        # Filter out existing records
        new_coauthors = []
        for coauthor in coauthors:
            coauthor_aid = coauthor[3]  # coauthor_aid is at index 3
            if (target_aid, coauthor_aid, yr) not in existing_records:
                new_coauthors.append(coauthor)
        
        all_coauthors.extend(new_coauthors)
        
        # Update all-time collaborators for next year
        set_all_collabs.update(new_collabs_this_year)

    return all_coauthors


def main():
    """
    Main processing pipeline:
    1. Load paper and author data with validation
    2. Create optimization lookup tables
    3. Process each target author sequentially
    4. For each author/year: analyze coauthor relationships
    5. Save new coauthor records to database
    """
    args = parse_args()
    
    # Load and validate input data
    db_exporter, df_pap, df_auth = load_and_validate_data(args)
    
    # Create optimization lookups
    target2info, coaut2info = create_optimization_lookups(df_auth)
    
    # Get list of target authors
    targets = get_target_authors(df_pap)

    # Process each target author
    total_new_records = 0
    
    for i, row in tqdm(targets.iterrows(), total=len(targets), desc="Processing authors"):
        target_aid, target_name = row['ego_aid'], row['name']
        
        # Get existing coauthor records to avoid duplicates
        _, cache_coauthor = db_exporter.get_author_cache(target_aid)
        existing_records = set([(aid, caid, yr) for aid, caid, yr in cache_coauthor])
        
        if i % PROGRESS_REPORT_INTERVAL == 0:
            print(f"\n--- Processing author {i+1}/{len(targets)}: {target_name} ---")
            print(f"Found {len(existing_records):,} existing coauthor records")
        
        # Process this author's coauthor relationships
        coauthors = process_single_author(
            df_pap, target_aid, target_name, target2info, coaut2info, existing_records
        )
        
        # Save new records to database
        if len(coauthors) > 0:
            if i % PROGRESS_REPORT_INTERVAL == 0:
                print(f"Inserting {len(coauthors):,} new coauthor records for {target_name}")
            db_exporter.save_coauthors(coauthors)
            total_new_records += len(coauthors)
        else:
            if i % PROGRESS_REPORT_INTERVAL == 0:
                print(f"No new coauthor records to insert for {target_name}")

    # Print final summary
    print(f"\n=== Processing Complete ===")
    print(f"Total authors processed: {len(targets):,}")
    print(f"Total new coauthor records created: {total_new_records:,}")
    
    # Close database connection
    print("Closing database connection")
    db_exporter.close()
    print("Done!")


if __name__ == "__main__":
    main()