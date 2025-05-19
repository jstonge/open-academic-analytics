"""
OUTPUT
======
`coauthor`: tidy with PRIMARY KEY (`title(coauthor name/aid; name is weird because dataviz)`, `doi`). _Metadata is in relation to ego_.
            Are they new acquaintance? Do they share institutions?
            How many do they have collaborated with ego this year? Since the start of ego's career?
"""
import calendar
from pathlib import Path
from tqdm import tqdm
import argparse
import random
import pandas as pd
from datetime import datetime
from collections import Counter
import sys, os

ROOT_DIR = os.path.abspath(os.curdir)
sys.path.append(ROOT_DIR)

from scripts.modules.database_exporter import DatabaseExporter
from scripts.modules.utils import shuffle_date_within_month


def parse_args():
    parser = argparse.ArgumentParser("Data Downloader")
    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        help="Input directory with paper.parquet",
        required=True,
    )
    parser.add_argument(
        "-o", "--output", type=Path, help="output directory", required=True
    )
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Initialize database exporter
    print(f"Connecting to database at {args.output / 'oa_data_raw.db'}")

    # db_exporter = DatabaseExporter("/Users/jstonge1/Documents/work/uvm/open-academic-analytics/data/raw/oa_data_raw.db")
    db_exporter = DatabaseExporter(str(args.output / "oa_data_raw.db"))
    
    # Load processed papers
    print(f"Loading paper data from {args.input / 'paper.parquet'}")
    try:
        # df_pap = pd.read_parquet("web/data/paper.parquet")
        df_pap = pd.read_parquet(args.input / "paper.parquet")
        print(f"Loaded {len(df_pap)} papers")
    except Exception as e:
        print(f"Error loading paper data: {e}")
        db_exporter.close()
        return
    
    print("Loading author data from database")
    try:
        df_auth = db_exporter.con.sql("SELECT * from author").fetchdf()
        print(f"Loaded {len(df_auth)} author records")
    except Exception as e:
        print(f"Error loading author data: {e}")
        db_exporter.close()
        return

    # Get list of authors
    targets = df_pap[['ego_aid', 'name']].drop_duplicates()
    print(f"Processing {len(targets)} target authors")

    # Create some lookup to make it faster
    print("Creating lookup tables for optimization")
    target2info = df_auth[['aid', 'pub_year', 'institution', 'author_age']]\
                        .set_index(['aid', 'pub_year']).apply(tuple, axis=1).to_dict()
    
    coaut2info = df_auth[['display_name', 'pub_year', 'institution', 'aid']]\
                        .set_index(['display_name', 'pub_year']).apply(tuple, axis=1).to_dict()
    
    # Process each author
    for i, row in tqdm(targets.iterrows(), total=len(targets)):
        
        target_aid, target_name = row['ego_aid'], row['name']
        
        # Check if target_name is NaN or None
        if pd.isna(target_name):
            print(f"Warning: Name is missing for author {target_aid}, using ID as name")
            target_name = target_aid
            
        print(f"Processing {target_name} ({target_aid})")

        # Get the years this author has publications
        years = db_exporter.con.execute("SELECT DISTINCT pub_year from df_pap WHERE ego_aid = ? ORDER BY pub_year", (target_aid,)).fetchall()
        if not years:
            print(f"No publication years found for {target_name}, skipping")
            continue
            
        print(f"Found publications in years: {[yr[0] for yr in years]}")

        # Get existing coauthor records using DatabaseExporter
        _, cache_coauthor = db_exporter.get_author_cache(target_aid)
        existing_records = set([(aid, caid, yr) for aid, caid, yr in cache_coauthor])
        print(f"Found {len(existing_records)} existing coauthor records")

        # Global values
        coauthors = []
        set_all_collabs = set()  # Set of all collaborators across time
        all_time_collabo = {}  # Dict(Name => (Collabo,)) across time
        set_collabs_of_collabs_never_worked_with = set() # useful to know when two authors know each other

        for yr in years:
            yr = yr[0]  # Extract year from tuple
            
            # Yearly values to keep track
            dates_in_year = []  # List to keep track of dates for papers in this year
            new_collabs_this_year = set()
            collabs_of_collabs_time_t = set()
            coauthName2aid = {} # Dict(Name => OpenAlex ID) for the year
            time_collabo = {} # Dict(Name => (Collabo,)) for the year

            # Get target author info for this year
            target_info = target2info.get((target_aid, yr))
            if target_info is None:
                print(f"Missing info for {target_name} in {yr}")
                continue
            else:
                target_institution, auth_age = target_info
            
            # Get papers for this year
            work_query = "SELECT * FROM df_pap WHERE ego_aid = ? AND pub_year = ?"
            works = db_exporter.con.execute(work_query, (target_aid, yr)).fetchdf()
            
            if len(works) == 0:
                print(f"No papers found for {target_name} in {yr}")
                continue
                
            print(f"Processing {len(works)} papers for {target_name} in {yr}")

            for i, w in works.iterrows():
                # Add some noise within year for visualization purpose
                try:
                    pub_date = datetime.strptime(str(w['pub_date']), "%Y-%m-%d  %H:%M:%S")
                    shuffled_date = shuffle_date_within_month(pub_date)
                    dates_in_year.append(shuffled_date)
                except Exception as e:
                    print(f"Error processing date {w['pub_date']}: {e}")
                    shuffled_date = f"{yr}-01-01"  # Fallback
                    dates_in_year.append(shuffled_date)

                # Check if authors field exists and is valid
                if 'authors' not in w or pd.isna(w['authors']):
                    print(f"Warning: Missing authors for paper {w['title']}")
                    continue
                    
                # Process each coauthor
                for coauthor_name in w['authors'].split(", "):
                    
                    if coauthor_name != w['name']:
                        # Increment collaboration count for the current year
                        author_yearly_data = time_collabo.get(coauthor_name, {'count': 0, 'institutions': {}})
                        author_yearly_data['count'] += 1

                        # Get coauthor info
                        coauthor_info = coaut2info.get((coauthor_name, yr))
                        
                        # If no info, skip this coauthor
                        if coauthor_info is None:
                            time_collabo.pop(coauthor_name, None)
                            continue

                        # Extract institution and aid
                        inst_name, coauthor_aid = coauthor_info
                        
                        # Update institution count
                        author_yearly_data['institutions'][inst_name] = author_yearly_data['institutions'].get(inst_name, 0) + 1

                        # Update collaboration trackers
                        time_collabo[coauthor_name] = author_yearly_data
                        all_time_collabo[coauthor_name] = all_time_collabo.get(coauthor_name, 0) + 1

                        # Store coauthor id
                        if coauthName2aid.get(coauthor_name) is None:
                            coauthName2aid[coauthor_name] = coauthor_aid

                        # Add new collaborators to the set for all years
                        if coauthor_name not in set_all_collabs:
                            new_collabs_this_year.add(coauthor_name)

            # Update set_collabs_of_collabs_never_worked_with
            set_collabs_of_collabs_never_worked_with.update(
                    collabs_of_collabs_time_t - new_collabs_this_year - set_all_collabs - set([target_name])
                    )

            # At the end of each year, do yearly collaboration stats.
            # we need to wait the end of a year to do all that.
            if len(time_collabo) > 0:
                print(f"Processing {len(time_collabo)} coauthors for {target_name} in {yr}")

                for coauthor_name, coauthor_data in time_collabo.items():
                    coauthor_aid = coauthName2aid[coauthor_name]
                    
                    # Skip if already in database
                    if (target_aid, coauthor_aid, yr) in existing_records:
                        continue

                    # Determine if it's a new or existing collaboration for the year
                    if coauthor_name in (new_collabs_this_year - set_all_collabs):
                        if coauthor_name in set_collabs_of_collabs_never_worked_with:
                            subtype = 'new_collab_of_collab'
                        else:
                            subtype = 'new_collab'
                    else:
                        subtype = 'existing_collab'

                    # Assign a date from the papers they collaborated on (if available)
                    author_date = random.choice(dates_in_year) if dates_in_year else f"{yr}-01-01"
                    
                    # Format author age date (used for visualization)
                    shuffled_auth_age = "1"+author_date.replace(author_date.split("-")[0], str(auth_age).zfill(3))
                    # Impossible leap year
                    shuffled_auth_age = shuffled_auth_age.replace("29", "28") if shuffled_auth_age.endswith("29") else shuffled_auth_age

                    # Find whether coauthor shares institution with target
                    shared_inst = None
                    max_institution = None

                    if coauthor_data['institutions'] and target_institution:
                        # Find most common institution for this coauthor
                        max_institution = max(coauthor_data['institutions'], key=coauthor_data['institutions'].get)
                        # Check if it matches target's institution
                        if max_institution == target_institution:
                            shared_inst = max_institution

                    # Create coauthor record
                    coauthors.append((
                        target_aid,
                        author_date, int(author_date[0:4]),  # Use one of the dates from this year's papers
                        coauthor_aid, coauthor_name, subtype,
                        coauthor_data['count'], all_time_collabo[coauthor_name],
                        shared_inst, max_institution
                    ))

                # Update all collaborators set for next year
                set_all_collabs.update(new_collabs_this_year)

        # WRITE TO DATABASE using DatabaseExporter
        if len(coauthors) > 0:
            print(f"Inserting {len(coauthors)} new coauthor records for {target_name}")
            db_exporter.save_coauthors(coauthors)
        else:
            print(f"No new coauthor records to insert for {target_name}")

    # Close database connection using DatabaseExporter
    print("Closing database connection")
    db_exporter.close()
    print("Done!")

if __name__ == "__main__":
    main()