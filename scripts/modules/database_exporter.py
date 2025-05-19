"""
Database handling module for the coauthorship analysis pipeline
"""
import logging
from datetime import datetime

import duckdb

logger = logging.getLogger(__name__)

class DatabaseExporter:
    """Class to handle all database operations"""
    
    def __init__(self, db_path):
        """
        Initialize database connection.
        
        Args:
            db_path (str or Path): Path to the database file
        """
        self.db_path = str(db_path)
        self.con = duckdb.connect(self.db_path)
        self._setup_tables()
    
    def _setup_tables(self):
        """Set up all required database tables if they don't exist"""
        # Paper table
        self.con.execute("""
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
        
        # Coauthor table
        self.con.execute("""
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
        
        # Author info table
        self.con.execute("""
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
    
    def get_author_cache(self, author_id):
        """
        Get existing database records for a given author ID.
        
        Args:
            author_id (str): OpenAlex author ID
            
        Returns:
            tuple: (paper_cache, coauthor_cache)
        """
        paper_cache = self.con.execute(
            "SELECT ego_aid, wid FROM paper WHERE ego_aid = ?", 
            (author_id,)
        ).fetchall()
        
        coauthor_cache = self.con.execute(
            "SELECT ego_aid, coauthor_aid, pub_year FROM coauthor2 WHERE ego_aid = ?", 
            (author_id,)
        ).fetchall()
        
        return paper_cache, coauthor_cache
    
    def is_up_to_date(self, author_id, min_year, max_year):
        """
        Check if the database records for an author are up to date.
        
        Args:
            author_id (str): OpenAlex author ID
            min_year (int): First publication year
            max_year (int): Latest publication year
            
        Returns:
            bool: True if up to date, False otherwise
        """
        # Get date range from database
        min_query = "SELECT MIN(pub_year) FROM paper WHERE ego_aid = ?"
        max_query = "SELECT MAX(pub_year) FROM paper WHERE ego_aid = ?"
        
        min_db = self.con.execute(min_query, (author_id,)).fetchone()
        max_db = self.con.execute(max_query, (author_id,)).fetchone()
        
        # If no data in DB or NULL values, not up to date
        if not min_db or not max_db or min_db[0] is None or max_db[0] is None:
            return False
        
        # Check if ranges match
        return min_db[0] <= min_year and max_db[0] >= max_year
    
    def save_papers(self, papers):
        """
        Save paper data to the database.
        
        Args:
            papers (list): List of paper tuples
        """
        if not papers:
            return
            
        try:
            self.con.executemany("""
                INSERT INTO paper
                (ego_aid, ego_display_name, wid, pub_date, pub_year, doi, title, work_type, 
                primary_topic, authors, cited_by_count, ego_position, ego_institution)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (ego_aid, wid)
                DO UPDATE SET
                    work_type = COALESCE(EXCLUDED.work_type, paper.work_type),
                    primary_topic = COALESCE(EXCLUDED.primary_topic, paper.primary_topic)
            """, papers)
            self.con.commit()
        except Exception as e:
            logger.error(f"Error saving papers to database: {str(e)}")
            self.con.rollback()
    
    def save_coauthors(self, coauthors):
        """
        Save coauthor data to the database.
        
        Args:
            coauthors (list): List of coauthor tuples
        """
        if not coauthors:
            return
            
        try:
            self.con.executemany("""
                INSERT INTO coauthor2
                (ego_aid, pub_date, pub_year, coauthor_aid, coauthor_name, acquaintance, 
                yearly_collabo, all_times_collabo, shared_institutions, coauthor_institution)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (ego_aid, coauthor_aid, pub_year)
                DO NOTHING
            """, coauthors)
            self.con.commit()
        except Exception as e:
            logger.error(f"Error saving coauthors to database: {str(e)}")
            self.con.rollback()
    
    def save_authors(self, authors):
        """
        Save author data to the database.
        
        Args:
            authors (list): List of author tuples
        """
        if not authors:
            return
            
        try:
            self.con.executemany("""
                INSERT INTO author
                (aid, display_name, institution, pub_year, first_pub_year, last_pub_year, author_age)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (aid, pub_year) 
                DO UPDATE SET
                    institution = COALESCE(EXCLUDED.institution, author.institution),
                    first_pub_year = COALESCE(EXCLUDED.first_pub_year, author.first_pub_year)
            """, authors)
            self.con.commit()
        except Exception as e:
            logger.error(f"Error saving authors to database: {str(e)}")
            self.con.rollback()
    
    def update_author_ages(self, author_id, first_pub_year):
        """
        Update author ages based on a known first publication year.
        
        Args:
            author_id (str): OpenAlex author ID
            first_pub_year (int): Corrected first publication year
        """
        try:
            # Get all author records
            author_df = self.con.execute(
                "SELECT * FROM author WHERE aid = ?", 
                (author_id,)
            ).fetch_df()
            
            if len(author_df) == 0:
                return
            
            # Update first_pub_year and recalculate author_age
            author_df['first_pub_year'] = first_pub_year
            author_df['author_age'] = author_df.pub_year - author_df.first_pub_year
            
            # Update database
            self.con.executemany("""
                INSERT INTO author
                (aid, display_name, institution, pub_year, first_pub_year, last_pub_year, author_age)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (aid, pub_year) 
                DO UPDATE SET 
                    author_age = EXCLUDED.author_age,
                    first_pub_year = EXCLUDED.first_pub_year
            """, author_df.values.tolist())
            
            # Delete records with negative author_age
            self.con.execute(
                "DELETE FROM author WHERE aid = ? AND author_age < 0", 
                (author_id,)
            )
            self.con.commit()
        except Exception as e:
            logger.error(f"Error updating author ages: {str(e)}")
            self.con.rollback()
    
    def close(self):
        """Close database connection"""
        self.con.close()