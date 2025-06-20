# Open Academic Analytics

### Data schema

<img width="899" alt="Screenshot 2025-05-27 at 10 24 30 AM" src="https://github.com/user-attachments/assets/eed6117a-0c4f-4de8-9a8d-89bc422da929" />

#### Description

1. `paper` - Individual publication records

- Primary key: (ego_aid, wid)
- Contains paper metadata like title, DOI, citations, coauthors
- Populated by timeline-paper.py

2. `author` - Author career progression data

- Primary key: (aid, pub_year)
- Tracks authors by year with age, institution, career span
- Populated by timeline-paper.py

3. `coauthor2` - Collaboration relationships

- Primary key: (ego_aid, coauthor_aid, pub_year)
- Contains collaboration metadata like frequency, relationship type, shared institutions
- Populated by timeline-coauthor.py

#### Data flow

1. `timeline-paper.py` fetches papers from OpenAlex API → populates `paper` and `author` tables
1. `timeline-coauthor.py` reads paper data → analyzes collaborations → populates `coauthor2` table
1. Preprocessing scripts read all tables → output clean parquet files for analysis

See [Makefile](./Makefile) for more.


#### Lab notes

 - Extracting coauthorship metadata is tricky because we need to know information stored in different API endpoints. For instance, extracting `author_age` requires to know when was the time of first publication for each coauthor.