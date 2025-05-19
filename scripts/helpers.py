import requests
import json
import pyalex
from pathlib import Path
import re
import os

# s2orc_token=os.environ['S2ORC_TOK'] 
s2orc_token='8mH99xWXoi60vMDfkSJtb6zVhSLiSgNP8ewg3nlZy'


# put your own please
pyalex.config.email = "jstonge1@uvm.edu"


def write_paper_s2orc(fname, out_dir):
    # fname=all_fnames[4]
    fout = Path(out_dir / f"{fname.stem}.jsonl")
    if not fout.exists(): 
        print(fname)
        paper_dat=read_jsonl(fname)
        if paper_dat is not None:
            ids=[]
            for p in paper_dat:
                # p=paper_dat[0]
                if 'doi' in p['ids']:
                    ids+=["DOI:"+re.sub("https://doi.org/", "", p['ids']['doi'])]
                elif 'mag' in p['ids']:
                    ids+=["MAG:"+p['ids']['mag']]
                else:
                    print(f"no doi or mag for {p['title']}")
                    continue
                
            paper_detail=get_paper_data(ids)
            
            with open(fout, "w") as f:
                json.dump(paper_detail, f)
        else:
            print(f"no data for {fname.stem}")


def flatten(l):
    return [item for sublist in l for item in sublist]


def get_paper_data(ids):
  url = f'https://api.semanticscholar.org/graph/v1/paper/batch'

  # Define which details about the paper you would like to receive in the response
  params = {
      'fields': 'externalIds,title,year,citationCount,influentialCitationCount,venue,s2FieldsOfStudy,embedding.specter_v2'
      }

  # Send the API request and store the response in a variable
  response = requests.post(url, 
                           headers= {"x-api-key" : f'{s2orc_token}' },
                           params=params, 
                           json={'ids': ids})
  if response.status_code == 200:
    return response.json()
  else:
    print(response.status_code)
    return None


def read_jsonl(fname):
    out=[]
    with open(fname, 'r') as file:
        # Read each line in the file
        for line in file:
            # Parse the JSON string and add the resulting dictionary to the list
            out.append(json.loads(line))
    return out


def write_jsonl(fname, out):
    with open(fname, 'a') as file:
        for entry in out:
            json_line = json.dumps(entry)
            file.write(json_line + '\n')


def replace_new_min_year(con, aid, name, new_min_year):
    """CLEANING oa_data_raw.db. (OLD)"""
    con.sql(f"UPDATE author SET first_pub_year={new_min_year} WHERE aid = '{aid}'")
    
    con.sql(f"DELETE FROM author WHERE aid = '{aid}' AND pub_year < {new_min_year}")
    con.sql(f"DELETE FROM author WHERE display_name = '{name}' AND pub_year < {new_min_year}")
    
    con.sql(f"DELETE FROM paper WHERE aid = '{aid}' AND pub_year < {new_min_year}")
    con.sql(f"DELETE FROM paper WHERE target = '{name}' AND pub_year < {new_min_year}")
    
    con.sql(f"DELETE FROM coauthor WHERE aid = '{aid}' AND pub_year < {new_min_year} ")
    con.sql(f"DELETE FROM coauthor WHERE target = '{name}' AND pub_year < {new_min_year}")
    
    # update age
    con.sql(f"UPDATE author SET author_age = pub_year-first_pub_year WHERE aid = '{aid}'")
    
    con.sql("")
    # con.sql(f"UPDATE paper SET author_age = pub_year-first_pub_year WHERE display_name = '{name}'")
    
    con.commit()

def clean_db_from(con, aid, name):
    """CLEANING oa_data_raw.db. (OLD)"""
    con.execute("DELETE FROM paper WHERE aid = ?", (aid,))
    con.execute("DELETE FROM paper WHERE target = ?", (name,))
    
    # In coauthor table it, we just get rid of the target rows
    # We don't want to remove that persons from the coauthorship of others.
    con.execute("DELETE FROM coauthor WHERE target = ?", (name,))
    
    # In author, we remove all all rows for that person
    con.execute("DELETE FROM author WHERE aid = ?", (aid,))
    con.execute("DELETE FROM author WHERE display_name = ?", (name,))
    
    con.commit()


def generate_neh_query(
    code="", 
    yr=0, 
    ob="Institution name", 
    xor="ASC"
    ):
    query = f"https://apps.neh.gov/PublicQuery/Default.aspx?q=1&a=0&n=0&o=0&ot=0&k=0&f=0&s=1&sv={code}&cd=0&p=0&d=0&at=1&atv=2&y=1&yf={yr}&yt={yr+1}&prd=0&cov=0&prz=0&wp=0&sp=0&ca=0&arp=0&ob={ob}&or={xor}"
    return query.strip()
