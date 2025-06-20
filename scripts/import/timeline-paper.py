"""
Paper Timeline Processor

Fetches and processes academic papers for target researchers using OpenAlex API,
creating comprehensive paper and author records with temporal metadata.

INPUT: researchers.tsv (researcher annotations with OpenAlex IDs)
OUTPUT: paper and author records in oa_data_raw.db with timeline data

Key Features:
- Fetches papers by year from OpenAlex API for each researcher
- Processes paper metadata (DOI, citations, coauthors, institutions)
- Tracks author career progression and institutional affiliations
- Handles coauthor discovery and OpenAlex ID resolution
- Updates author age calculations based on publication history
- Filters non-English works and validates publication dates
"""
import sys
import os
import argparse
from pathlib import Path
from tqdm import tqdm
import pandas as pd
from datetime import datetime
from collections import Counter

ROOT_DIR = os.path.abspath(os.curdir)
sys.path.append(ROOT_DIR)

from scripts.modules.database_exporter import DatabaseExporter
from scripts.modules.data_fetcher import OpenAlexFetcher
from scripts.modules.utils import shuffle_date_within_month
from scripts.modules.author_processor import AuthorProcessor

def parse_args():
    parser = argparse.ArgumentParser("Data Downloader")
    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        help="TSV file with researcher information",
        required=True,
    )
    parser.add_argument(
        "-o", "--output", type=Path, help="output database", required=True
    )
    parser.add_argument(
        "-U", "--update", action="store_true", help="update author age"
    )
    return parser.parse_args()

def main():
    args = parse_args()
    # update_author_age = True
    update_author_age = args.update
    
    # Load researchers annotations
    assert args.input.exists(), f"Input file {args.input} does not exist"
    print(f"Loading researcher data from {args.input}")
    # target_aids = pd.read_csv("/Users/jstonge1/Documents/work/uvm/open-academic-analytics/data/raw/researchers.tsv", sep="\t")
    target_aids = pd.read_csv(args.input, sep="\t")
    print(f"Loaded {len(target_aids)} researcher records")
    
    # Get list of oa_uids for all authors of interest
    target_aids = target_aids[~target_aids['oa_uid'].isna()]
    print(f"Found {len(target_aids)} researchers with OpenAlex IDs")
    
    # Extract known first publication years if available
    known_years_df = target_aids[['oa_uid', 'first_pub_year']].dropna()
    known_first_pub_years = {k.upper(): int(v) for k, v in known_years_df.values}
    
    # Process target_aids
    target_aids['oa_uid'] = target_aids['oa_uid'].str.upper()
    
    # Initialize modules
    print(f"Connecting to database at {args.output}")
    # db_exporter = DatabaseExporter("/Users/jstonge1/Documents/work/uvm/open-academic-analytics/data/raw/oa_data_raw.db")
    db_exporter = DatabaseExporter(args.output)
    
    print("Initializing OpenAlex fetcher")
    fetcher = OpenAlexFetcher()
    
    print("Initializing AuthorProcessor")
    author_processor = AuthorProcessor(db_exporter)

    # Pre-load publication years from database
    author_processor.preload_publication_years()
    print(f"Preloaded {len(author_processor.publication_year_cache)} publication year ranges")

    # Process each researcher
    for i, row in tqdm(target_aids.iterrows(), total=len(target_aids)):
           
        # target_aid = 'A5040821463'
        target_aid = row['oa_uid']
        
        # Fetch display name from OpenAlex, as it's not in the raw data
        try:
            author_obj = fetcher.get_author_info(target_aid)
            target_name = author_obj['display_name']
        except Exception as e:
            print(f"Error fetching display name for {target_aid}: {e}")
            # If we can't get the display name, use the ID as a fallback
            target_name = target_aid
            
        print(f"\nProcessing {target_name} ({target_aid})")

        # Handle the update author age case
        if update_author_age:
            if target_aid in known_first_pub_years:
                first_pub_year = known_first_pub_years[target_aid]
                print(f"Updating first publication year for {target_name} to {first_pub_year}")
                db_exporter.update_author_ages(target_aid, first_pub_year)
            else:
                print(f"No known first publication year for {target_name}, skipping update")
            continue
        
        # Get publication year range from either known values or OpenAlex API
        min_yr = known_first_pub_years.get(target_aid)
        if min_yr is None:
            try:
                min_yr, _ = fetcher.get_publication_range(target_aid)
                print(f"First publication year for {target_name}: {min_yr}")
            except Exception as e:
                print(f"Error getting first publication year for {target_name}: {e}")
                continue
        
        try:
            # Get the latest publication year
            author_info = fetcher.get_author_info(target_aid)
            max_yr = author_info['counts_by_year'][0]['year']
            print(f"Latest publication year for {target_name}: {max_yr}")
        except Exception as e:
            print(f"Error getting latest publication year for {target_name}: {e}")
            # Fallback to current year if we can't get the latest pub year
            max_yr = datetime.now().year
        
        # Store in publication year cache
        author_processor.publication_year_cache[target_aid] = (min_yr, max_yr)

        # Check if database is up to date
        if db_exporter.is_up_to_date(target_aid, min_yr, max_yr):
            print(f"{target_name} is up to date in database")
            continue
        
        # Get existing papers from database
        paper_cache, _ = db_exporter.get_author_cache(target_aid)
        existing_papers = set([(aid, wid) for aid, wid in paper_cache])
        print(f"Found {len(existing_papers)} existing papers in database")
        
        # Process papers for each year
        papers = []

        # Check if we have valid year range
        if min_yr is None or max_yr is None:
            print(f"Skipping {target_name} ({target_aid}) - cannot determine publication years")
            continue  # Skip to the next author
        
        # Process each year
        for yr in range(min_yr, max_yr + 1):
                
            print(f"Processing year {yr}...")
            
            # Get all publications for this year using fetcher
            publications = fetcher.get_publications(target_aid, yr)
            
            if not publications:
                print(f"No publications found for {target_name} in {yr}")
                continue
                
            print(f"Processing {len(publications)} publications for {yr}")
            
            # Track institutions for this year
            ego_institutions_this_year = []
            
            # Process each publication
            for w in publications:
                # Skip non-English works
                if w.get('language') != 'en':
                    continue
                    
                # Extract work ID
                wid = w['id'].split("/")[-1]
                
                # Skip if already in database
                # if (target_aid, wid) in existing_papers:
                #     continue
                
                # Add some noise within year for visualization purpose
                shuffled_date = shuffle_date_within_month(w['publication_date'])
                
                # Process authorships
                author_position = None
                
                for authorship in w['authorships']:
                    coauthor_name = authorship['author']['display_name']
                    
                    # If this is the target author, collect institution and position
                    if authorship['author']['id'].split("/")[-1] == target_aid:
                        ego_institutions_this_year += [i['display_name'] for i in authorship['institutions']]
                        author_position = authorship['author_position']
                
                # Determine target institution through majority vote
                target_institution = None
                if ego_institutions_this_year:
                    target_institution = Counter(ego_institutions_this_year).most_common(1)[0][0]
                
                # Extract paper metadata
                doi = w['ids'].get('doi') if 'ids' in w else None
                fos = w['primary_topic'].get('display_name') if w.get('primary_topic') else None
                coauthors = ', '.join([a['author']['display_name'] for a in w['authorships']])
                
                # Create paper record
                papers.append((
                    target_aid, target_name, wid,
                    shuffled_date, int(w['publication_year']),
                    doi, w['title'], w['type'], fos,
                    coauthors,
                    w['cited_by_count'],
                    author_position,
                    target_institution
                ))
        
        # Save papers to database
        if papers:
            print(f"Saving {len(papers)} papers for {target_name}")
            db_exporter.save_papers(papers)
        else:
            print(f"No new papers to save for {target_name}")
        
        if papers:
            print(f"Processing author information for {target_name}")
            
            # Extract coauthor information from papers
            coauthor_info = []
            for paper in papers:
                pub_year = paper[4]  # paper year
                if paper[9]:  # paper[9] contains author list
                    try:
                        coauthor_names = paper[9].split(", ")
                        for coauthor_name in coauthor_names:
                            if coauthor_name != target_name:
                                # Add to coauthor_info
                                coauthor_info.append({
                                    'name': coauthor_name,
                                    'year': pub_year,
                                    'institution': paper[12]  # Use paper's institution
                                })
                    except Exception as e:
                        print(f"Error extracting coauthors from paper: {e}")
            
            print(f"Extracted {len(coauthor_info)} coauthor records from papers")
            
            # Check if we can find OpenAlex IDs for coauthors
            coauthors_with_ids = []

            for info in coauthor_info:
                try:
                    # Skip if essential data is missing
                    if not info.get('name') or not info.get('year'):
                        print(f"Skipping coauthor due to missing name or year: {info}")
                        continue
                        
                    # Check if author is already cached
                    result = db_exporter.get_author_cache_by_name(info['name'])
                    
                    # Use fetcher to get author by name if not cached
                    if result is None:
                        result = fetcher.get_author_info_by_name(info['name'])
                        
                    # Process result if we found author data
                    if result and ('id' in result or 'aid' in result):
                        # Extract coauthor ID
                        coauthor_id_raw = result.get('id') or result.get('aid')
                        coauthor_id = coauthor_id_raw.split('/')[-1] if isinstance(coauthor_id_raw, str) else str(coauthor_id_raw)
                        
                        # Create coauthor tuple with consistent date formatting
                        pub_date = f"{info['year']}-01-01"
                        
                        coauthor_tuple = (
                            target_aid,                    # ego_aid
                            pub_date,                      # pub_date
                            info['year'],                  # pub_year
                            coauthor_id,                   # coauthor_aid
                            info['name'],                  # coauthor_name
                            "from_paper",                  # acquaintance
                            1,                             # yearly_collabo
                            1,                             # all_times_collabo
                            None,                          # shared_institutions
                            info.get('institution')        # coauthor_institution (safe get)
                        )
                        
                        coauthors_with_ids.append(coauthor_tuple)
                    else:
                        print(f"No valid ID found for coauthor: {info['name']}")
                        
                except KeyError as e:
                    print(f"Missing required field for coauthor {info.get('name', 'Unknown')}: {e}")
                except AttributeError as e:
                    print(f"Invalid data format for coauthor {info.get('name', 'Unknown')}: {e}")
                except Exception as e:
                    print(f"Unexpected error processing coauthor {info.get('name', 'Unknown')}: {e}")

            print(f"Successfully processed {len(coauthors_with_ids)} coauthors with IDs")
            print(f"Found IDs for {len(coauthors_with_ids)} coauthors")

            author_records = author_processor.collect_author_info(
                target_aid,
                target_name,  # Pass the display name from API
                (min_yr, max_yr),
                papers,
                coauthors_with_ids,
                fetcher
            )
            
            # Save author records
            if author_records:
                print(f"Saving {len(author_records)} author records for {target_name}")
                db_exporter.save_authors(author_records)
            else:
                print("No author records to save")

    # Close database connection
    print("\nClosing database connection")
    db_exporter.close()
    print("Done!")


if __name__ == "__main__":
    main()