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
import duckdb
import pandas as pd

def coauthor_db(con, aid=None):
    con.execute("""
        CREATE TABLE IF NOT EXISTS coauthor2 (
            ego_aid VARCHAR,
            pub_date DATE,
            pub_year INT,
            coauthor_aid VARCHAR,
            coauthor_name VARCHAR,
            acquaintance VARCHAR,
            yearly_collabo INT,
            all_times_collabo INT,
            shared_institutions VARCHAR,
            coauthor_institution VARCHAR,
            PRIMARY KEY(ego_aid, coauthor_aid, pub_year)
        )
    """)

    if aid is not None:
        return (con.execute("SELECT ego_aid, coauthor_name, pub_year FROM coauthor2 WHERE ego_aid = ?", (aid,)).fetchall())

def shuffle_date_within_month(date):
    # Parse the date string to a datetime object
    # date = datetime.strptime(date_str, "%Y-%m-%d")

    # Get the number of days in the month of the given date
    _, num_days_in_month = calendar.monthrange(date.year, date.month)

    # Generate a random day within the same month
    random_day = random.randint(1, num_days_in_month)

    # Create a new date with the randomly chosen day
    shuffled_date = date.replace(day=random_day)

    # Return the date in the desired format
    return shuffled_date.strftime("%Y-%m-%d")

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
    return parser.parse_args()

def main():
    args = parse_args()
    # con = duckdb.connect("../../data/raw/oa_data_raw.db")
    # df_pap = pd.read_parquet("../../docs/data/paper.parquet")
    
    
    con = duckdb.connect(str(args.output / "oa_data_raw.db") )
    df_auth = con.sql("SELECT * from author").fetchdf()
    df_pap = pd.read_parquet(args.input / "paper.parquet")

    # Get list of authors

    targets = df_pap[['ego_aid', 'name']].drop_duplicates()

    # Create some lookup to make it faster

    target2info = df_auth[['aid', 'pub_year', 'institution', 'author_age']]\
                        .set_index(['aid', 'pub_year']).apply(tuple, axis=1).to_dict()
    
    coaut2info = df_auth[['display_name', 'pub_year', 'institution', 'aid']]\
                        .set_index(['display_name', 'pub_year']).apply(tuple, axis=1).to_dict()
                        
    
    for i, row in tqdm(targets.iterrows(), total=len(targets)):
        
        # target_aid, target_name = 'A5074708105', 'Alison K. Brody'
        target_aid, target_name = row['ego_aid'], row['name']

        years = con.execute("SELECT DISTINCT pub_year from df_pap WHERE ego_aid = ? ORDER BY pub_year", (target_aid,)).fetchall()

        cache_coauthor = coauthor_db(con, target_aid)

        # Global values
        coauthors = []

        set_all_collabs = set()  # Set of all collaborators across time
        all_time_collabo = {}  # Dict(Name => (Collabo,)) across time
        set_collabs_of_collabs_never_worked_with = set() # useful to know when two authors know each other


        for yr in years:
            # yr=[2009]
            
            # Yearly values to keep track
            dates_in_year = []  # List to keep track of dates for papers in this year
            new_collabs_this_year = set()
            collabs_of_collabs_time_t = set()
            coauthName2aid = {} # Dict(Name => OpenAlex ID) for the year
            time_collabo = {} # Dict(Name => (Collabo,)) for the year

            target_info = target2info.get((target_aid, yr[0]))
            if target_info is None:
                print(f"Missing info for {target_name} in {yr[0]}")
                continue
            else:
                target_institution, auth_age = target_info
            
            work_query = "SELECT * FROM df_pap WHERE name = ? AND pub_year = ?"
            works = duckdb.execute(work_query, (target_name, yr[0])).fetchdf()

            for i, w in works.iterrows():

                # Add some noise within year for visualization purpose
                shuffled_date = shuffle_date_within_month(w['pub_date'])
                dates_in_year.append(shuffled_date)

                # Now for each collaborator on that paper, check their institutions and collaboration count.
                for coauthor_name in w['authors'].split(", "):

                    if coauthor_name != w['name']:

                        # Increment collaboration count for the current year
                        author_yearly_data = time_collabo.get(coauthor_name, {'count': 0, 'institutions': {}})
                        author_yearly_data['count'] += 1

                        coauthor_info = coaut2info.get((coauthor_name, yr[0]))
                        
                        # Not supposed to be; if we have name we should have at least aid
                        if coauthor_info is None:
                            time_collabo.pop(coauthor_name, None)
                            continue

                        inst_name, coauthor_aid = coauthor_info if coauthor_info is not None else (None, None)
                        
                        author_yearly_data['institutions'][inst_name] = author_yearly_data['institutions'].get(inst_name, 0) + 1

                        time_collabo[coauthor_name] = author_yearly_data
                        all_time_collabo[coauthor_name] = all_time_collabo.get(coauthor_name, 0) + 1

                        if coauthName2aid.get(coauthor_name) is None:
                            coauthName2aid[coauthor_name] = coauthor_aid

                        # Add new collaborators to the set for all years
                        if coauthor_name not in set_all_collabs:
                            new_collabs_this_year.add(coauthor_name)

            set_collabs_of_collabs_never_worked_with.update(
                    collabs_of_collabs_time_t - new_collabs_this_year - set_all_collabs - set([target_name])
                    )

            # At the end of each year, do yearly collaboration stats.
            # we need to wait the end of a year to do all that.
            if len(time_collabo) > 0:

                for coauthor_name, coauthor_data in time_collabo.items():

                    if (target_aid, coauthor_name, yr[0]) in cache_coauthor:
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
                    author_date = random.choice(dates_in_year) if dates_in_year else str(yr[0])
                    shuffled_auth_age = "1"+author_date.replace(author_date.split("-")[0], str(auth_age).zfill(3))
                    # Impossible leap year
                    shuffled_auth_age = shuffled_auth_age.replace("29", "28") if shuffled_auth_age.endswith("29") else shuffled_auth_age

                    # Find whether coauthor shares institution with target
                    shared_inst = None
                    max_institution = None

                    if coauthor_data['institutions'] and target_institution:
                        max_institution = max(coauthor_data['institutions'], key=coauthor_data['institutions'].get)
                        if max_institution == target_institution:
                            shared_inst = max_institution

                    coauthors.append((
                        target_aid,
                        author_date, int(author_date[0:4]),  # Use one of the dates from this year's papers
                        coauthName2aid[coauthor_name], coauthor_name, subtype,
                        coauthor_data['count'], all_time_collabo[coauthor_name],
                        shared_inst, max_institution
                    ))

                set_all_collabs.update(new_collabs_this_year)

        # WRITE TO DATABASE --------------------------------------------


        if len(coauthors) > 0:
            # we might have duplicate papers for ego, we don't want to insert them again
            con.executemany( """
                    INSERT INTO coauthor2
                    (ego_aid, pub_date, pub_year, coauthor_aid, coauthor_name, acquaintance, yearly_collabo, all_times_collabo, shared_institutions, coauthor_institution)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (ego_aid, coauthor_aid, pub_year)
                    DO NOTHING
            """, coauthors)


    # Commit and close DB connection
    con.commit()
    con.close()

if __name__ == "__main__":
    main()
