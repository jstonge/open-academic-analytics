"""
Input OA api and a list of people as targets, output 3 tables:
  - `paper`: tidy at paper level with PRIMARY KEY (`ego_aid`, `wid`).
  - `coauthor`: tidy with PRIMARY KEY (`title(coauthor name/aid; name is weird because dataviz)`, `doi`). _Metadata is in relation to ego_. 
                Are they new acquaintance? Do they share institutions? 
                How many do they have collaborated with ego this year? Since the start of ego's career?
  - `author_info`: tidy at author-level with PRIMARY KEY (`aid`, `year`). For each year, we keep track of author's node metadata, e.g. 
                   institutions, citation count, author age, ... 

Note that we call `ego` the person of interest in a set of coauthors on a paper.
This means the same paper could reappear many times inside the `coauthor` table.
The paper table is tidy for papers, and the author table is tidy for authors.
We use openAlex id (aid and wid) as main keys for authors and works/papers.

Running this script for ego/target is somehow costly in terms of API calls, and constrained by OpenAlex data schema,
so many decisions are made to account for both of these facts.
"""
import calendar
from collections import Counter
from datetime import datetime
from itertools import chain
from pathlib import Path
from tqdm import tqdm
import argparse
import random

import pandas as pd
from pyalex import Works, Authors
import duckdb


def guess_min_pub_year(target_aid):
    """
    A bit messy because we find that OpenAlex often fails to get first author pub year right.
    """
    # target_aid, target_names = row
    # Authors for which we have manually verified their first year
    # It is important to get this right, as it determines age of target.
    # auth_known_first_yr = json.loads(open('./aid2firstyear.json', 'r').read())


        #     work_with_doi = Works().filter(authorships={"author": {"id": target_aid}}, has_doi=True)\
        #                             .sort(publication_date="desc")\
        #                             .get()[0]
            
        #     work_doi = work_with_doi['doi'].replace("https://doi.org/", "")
        #     arxiv = work_doi.split("arxiv.")[-1]

        #     s2orc_url = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{work_doi}"
        #     # headers = {'x-api-key': "Gzcfpx2eIV8GZriqiIvl81hHmPzBfgDr8U3qMscF"}
        #     query_params = {'fields': 'title,year,authors.name'}
        #     response = requests.get(s2orc_url, params=query_params)
            
        #     if response.status_code == 200:
        #         response.json()
        #         break
        
        # s2orc_paper = response.json()['authors']
        # s2orc_id = None
        
        # target_names = [target_names, target_names.split(" ")[0][0]+" "+target_names.split(" ")[-1]] if isinstance(target_names, str) else target_names
        # for a in s2orc_paper:
        #     best_score = process.extractOne(a['name'], target_names)[1]
        #     print(best_score, a)
        #     if best_score > 90:
        #         s2orc_id = a['authorId']
        #         break
            
        # if s2orc_id is not None:
        #     s2orc_url = f"https://api.semanticscholar.org/graph/v1/author/{s2orc_id}/papers?fields=year&limit=999"
        #     query_params = {'fields': 'year'}
        #     response = requests.get(s2orc_url)
            
        #     if response.status_code == 200:
        #         s2orc_papers = response.json()['data']
        #         s2orc_min_year = min([x['year'] for x in s2orc_papers if x['year'] is not None])
        # else:
        #     s2orc_min_year = math.inf

        # return max(oa_min_year), oa_max_year
    return  Works().filter(authorships={"author": {"id": target_aid}})\
                                    .sort(publication_date="asc")\
                                    .get()[0]['publication_year']
      
def most_recent_work(aid):
    return Works().filter(authorships={"author": {"id": aid}})\
                  .sort(publication_date="desc")\
                  .get()[0]

def max_pub_year(aid):
        return Authors()[aid]['counts_by_year'][0]['year']

def author_info_db(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS author (
            aid VARCHAR,
            display_name VARCHAR,
            institution VARCHAR,
            pub_year INT,
            first_pub_year INT,
            last_pub_year INT,
            author_age INT,
            PRIMARY KEY(aid, pub_year)
        )
    """) 

def paper_db(con, aid=None):
    con.execute("""
        CREATE TABLE IF NOT EXISTS paper (
            ego_aid VARCHAR,
            ego_display_name VARCHAR,
            wid VARCHAR,
            pub_date DATE,
            pub_year INT,
            doi VARCHAR,
            title VARCHAR,
            work_type VARCHAR,
            primary_topic VARCHAR,
            authors VARCHAR,
            cited_by_count INT,
            ego_position VARCHAR,
            ego_institution VARCHAR,
            PRIMARY KEY(ego_aid, wid)
        )
    """)

    if aid is not None:
        return (con.execute("SELECT ego_aid, wid FROM paper WHERE ego_aid = ?", (aid,)).fetchall())

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

def determine_home_inst(aid, works):
    # determine target home institution this year
    all_inst_this_year = []
    for w in works:
        for a in w['authorships']:
            if a['author']['id'].split("/")[-1] == aid:
                all_inst_this_year += [i['display_name'] for i in a['institutions']]
    return Counter(all_inst_this_year).most_common(1)[0][0] if len(all_inst_this_year) > 0 else None

def get_work_object_query(target_aid, yr):
    """returns to query to .get() or .paginate()"""
    return Works().filter(publication_year=yr, authorships={"author": { "id": target_aid }})

def get_pub_year_shuffled():
    pass

def range_db(con, aid):
    query_min = "SELECT pub_date FROM paper WHERE ego_aid = ? ORDER BY pub_date DESC LIMIT 1"
    query_max = "SELECT pub_date FROM paper WHERE ego_aid = ? ORDER BY pub_date ASC LIMIT 1"
    min_db = con.execute(query_min, (aid,)).fetchall()
    max_db = con.execute(query_max, (aid,)).fetchall()
    if len(min_db) > 0 and len(max_db) > 0:
        return min_db[0][0], max_db[0][0]

def is_db_up_to_date(con, target_aid, min_yr):
    """if in our DB we have the first year of the author and the last, we consider the DB updated."""
    min_max = range_db(con, target_aid)        
    max_oa =  datetime.strptime(most_recent_work(target_aid)['publication_date'], "%Y-%m-%d").date()
    min_oa = min_yr

    if min_max is None:
        return False
    else:
        return (min_oa >= min_max[1].year) and (max_oa.year <= min_max[0].year)

def parse_args():
    parser = argparse.ArgumentParser("Data Downloader")
    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        help="JSONlines file with urls and hashes",
        required=True,
    )
    parser.add_argument(
        "-o", "--output", type=Path, help="output directory", required=True
    )
    parser.add_argument(
        "-U", "--update", action="store_true", help="update author age"
    )
    return parser.parse_args()

def main():
    args = parse_args()
    # update_author_age = False
    update_author_age = args.update
    
    # Load researchers annotations
    # target_aids = pd.read_csv("../../data/raw/researchers.tsv", sep="\t")
    assert args.input.exists(), "Input file does not exist"

    target_aids = pd.read_csv(args.input, sep="\t")

    # Load the DB
    # con = duckdb.connect("../../data/raw/oa_data_raw.db")
    con = duckdb.connect(str(args.output / "oa_data_raw.db") )
    
    # Get list of openAlex Ids for all authors of interest
    target_aids = target_aids[~target_aids['OpenAlex id'].isna()]
    known_first_pub_years = target_aids[['oa_display_name', 'first_pub_year']].dropna()
    known_first_pub_years = { k: int(v) for k, v in known_first_pub_years.values }
    target_aids['OpenAlex id'] = target_aids['OpenAlex id'].str.upper()
    target_aids = target_aids[['OpenAlex id', 'oa_display_name']].drop_duplicates()

    def preload_publication_years():
            query = "SELECT aid, first_pub_year, last_pub_year FROM author WHERE first_pub_year IS NOT NULL AND last_pub_year IS NOT NULL"
            results = con.execute(query).fetchall()
            for result in results:
                aid, min_year, max_year = result
                publication_year_cache[aid] = (min_year, max_year)
    
    publication_year_cache = {}
    preload_publication_years()

    failed_range = []

    for i, row in tqdm(target_aids.iterrows(), total=len(target_aids)):
        # target_aid, target_name = 'A5018443530', 'Adam K. Anderson'
        target_aid = row['OpenAlex id']
        target_name = row['oa_display_name']
        
        # This flag is to fix first_pub_year, which is often wrong in openAlex.
        if update_author_age:
            dedup_author_df = con.execute("SELECT * FROM author WHERE aid = ?", (target_aid,)).fetch_df()
            
            # If author hasn't been done, we just skip it.
            if len(dedup_author_df) > 0:
                target_name = dedup_author_df.display_name.iloc[0]
                current_min_yr = dedup_author_df.first_pub_year.min()
                
                if (known_first_pub_years.get(target_name) is None) or (known_first_pub_years[target_name] == current_min_yr):
                    continue
                
                min_yr = known_first_pub_years[target_name] if known_first_pub_years.get(target_name) else dedup_author_df.pub_year.min()
                dedup_author_df['first_pub_year'] = min_yr
                dedup_author_df['author_age'] = dedup_author_df.pub_year - dedup_author_df.first_pub_year
                
                query = """
                    INSERT INTO author
                    (aid, display_name, institution, pub_year, first_pub_year, last_pub_year, author_age)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (aid, pub_year) 
                    DO UPDATE SET 
                        author_age = EXCLUDED.author_age,
                        first_pub_year = EXCLUDED.first_pub_year,

                """
                
                con.executemany(query, dedup_author_df.values.tolist()).commit()
                con.execute("DELETE FROM author WHERE aid = ? AND author_age < 0", (target_aid,))
                
                
            continue
        
        if target_aid in publication_year_cache:
            min_yr, max_yr = publication_year_cache[target_aid]
        else:
            min_yr = known_first_pub_years[target_name] if known_first_pub_years.get(target_name) else guess_min_pub_year(target_aid) 
            max_yr = max_pub_year(target_aid)
            
        # Create cache DB if not exists, else return the cache
        cache_paper = paper_db(con, target_aid)
        
        if is_db_up_to_date(con, target_aid, min_yr):
            print(f"{target_name} is up to date")
            continue
        
        print(f"Updating {target_name}" if update_author_age else f"Doing {target_name}")

        # Global values
        papers = [] # List(Dict) for each paper
        authors = {} # Dict((aid, year) -> authorData)
        
        # Year -> targetName, will be useful.
        yr2age = { yr:i for i,yr in enumerate(range(min_yr, max_yr+1)) }
        coauthName2aid = {} # Dict(Name => OpenAlex ID) for the year
        time_collabo = {} # Dict(Name => (Collabo,)) for the year

        for yr in yr2age.keys():
            # each year, ego can have multiple institutions.
            # we keep track of them, and take a majority vote at the 
            # end of each year
            # yr=2009
            ego_institutions_this_year = []
            
            q = get_work_object_query(target_aid, yr)
            target_institution = None # initialize target_inst
            
            for w in chain(*q.paginate(per_page=200)):
                
                
                if w['language'] != 'en':
                        continue

                # Add some noise within year for visualization purpose    
                shuffled_date = shuffle_date_within_month(w['publication_date'])
                
                for a in w['authorships']:
                        coauthor_name = a['author']['display_name']
                        if coauthor_name == target_name:
                            ego_institutions_this_year += [i['display_name'] for i in a['institutions']]
                            target_position = a['author_position']
                        else:
                            institutions = a['institutions'] if 'institutions' in a else []
                            
                            # Increment collaboration count for the current year
                            author_yearly_data = time_collabo.get(coauthor_name, {'count': 0, 'institutions': {}})
                            author_yearly_data['count'] += 1
                            
                            # Increment institution count for the current year
                            for inst in institutions:
                                inst_name = inst['display_name']
                                author_yearly_data['institutions'][inst_name] = author_yearly_data['institutions'].get(inst_name, 0) + 1
                            
                            time_collabo[coauthor_name] = author_yearly_data
                            
                            if coauthName2aid.get(coauthor_name) is None:
                                coauthor_aid = a['author']['id'].split("/")[-1]
                                coauthName2aid[coauthor_name] = coauthor_aid

                # Majority vote to determine target_institution
                if len(ego_institutions_this_year) > 0:
                    target_institution = Counter(ego_institutions_this_year).most_common(1)[0][0] 
                else:
                    target_institution = None
                
                wid = w['id'].split("/")[-1]

                if (target_aid, wid) not in cache_paper:
                    doi = w['ids']['doi'] if 'doi' in w['ids'] else None
                    fos = w['primary_topic'].get('display_name') if w.get('primary_topic') else None
                    coauthors = ', '.join([_['author']['display_name'] for _ in w['authorships']])
                    
                    papers.append((
                        target_aid, target_name, wid,
                        shuffled_date, int(w['publication_year']),
                        doi, w['title'], w['type'], fos,
                        coauthors,
                        w['cited_by_count'],
                        target_position,
                        target_institution
                    ))

            # At the end of each year, do yearly collaboration stats.
            # we need to wait the end of a year to do all that.
            if len(time_collabo) > 0:            
                
                for coauthor_name, coauthor_data in time_collabo.items():
                    coauthor_aid = coauthName2aid[coauthor_name]
                    max_institution = None
                    
                    if coauthor_data['institutions'] and target_institution:
                        max_institution = max(coauthor_data['institutions'], key=coauthor_data['institutions'].get)

                    try:
                        if authors.get((coauthor_aid, yr)) is None:

                            if coauthor_name == target_name:
                                publication_year_cache[target_aid] = (min_yr, max_yr)
                            
                            elif coauthor_aid not in publication_year_cache:
                                
                                if coauthor_aid not in failed_range:
                                    min_year = known_first_pub_years[coauthor_name] if known_first_pub_years.get(coauthor_name) else guess_min_pub_year(coauthor_aid)
                                    max_year = max_pub_year(coauthor_aid) 
                                    publication_year_cache[coauthor_aid] = (min_year, max_year)
                                else:
                                    publication_year_cache[coauthor_aid] = (None, None)

                            
                            min_yr_c, max_yr_c = publication_year_cache[coauthor_aid]

                    
                    except: # we just ignore that for now
                            print(f"{coauthor_aid} failed to have range of year")
                            failed_range.append(coauthor_aid)
                            pass
                    
                    coauthor_dat = (
                        coauthor_aid, coauthor_name, max_institution, 
                        yr, min_yr_c , max_yr_c,
                        (yr - min_yr_c) if min_yr_c is not None else None
                        )

                    authors[(coauthor_aid, yr)] = coauthor_dat

            # ADD EGO TO AUTHOR TABLE
            
            if authors.get((target_aid, yr)) is None:
                authors[(target_aid, yr)] = (
                    target_aid, target_name, target_institution,
                    yr, min_yr, max_yr, yr - min_yr
                )

                
        # WRITE TO DATABASE --------------------------------------------
        
        
        if len(papers) > 0:   
            con.executemany( """
                        INSERT INTO paper
                        (ego_aid, ego_display_name, wid, pub_date, pub_year, doi, title, work_type, primary_topic, authors, cited_by_count, ego_position, ego_institution)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (ego_aid, wid)
                        DO UPDATE SET
                            work_type = COALESCE(EXCLUDED.work_type, paper.work_type),
                            primary_topic = COALESCE(EXCLUDED.primary_topic, paper.primary_topic)
            """, [_ for _ in papers])

        # On conlict, we update first_pub_year and institution if NULL.
        query = """
            INSERT INTO author
            (aid, display_name, institution, pub_year, first_pub_year, last_pub_year, author_age)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (aid, pub_year) 
            DO UPDATE SET
                institution = COALESCE(EXCLUDED.institution, author.institution),
                first_pub_year = COALESCE(EXCLUDED.first_pub_year, author.first_pub_year)
        """
        
        con.executemany(query, [_ for _ in authors.values()])

    # Commit and close DB connection
    con.commit()
    con.close()

if __name__ == "__main__":
    main()
