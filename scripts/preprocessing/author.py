"""
Author Data Preprocessor

Processes author records from the database to create a standardized dataset
with normalized age representations for temporal visualization.

INPUT: oa_data_raw.db (author table with career progression data)
OUTPUT: author.parquet (processed author dataset with visualization-ready fields)

Key Features:
- Extracts author career data including age, institution, publication history
- Creates standardized age representation (age_std) for timeline visualization
- Normalizes author age data with random date components for smooth animations
- Prepares author metadata for integration with paper and coauthor datasets
- Validates data integrity and handles missing values
"""

# Standard library imports
import sys
import os
import argparse
from pathlib import Path

# Third-party imports
import numpy as np
import pandas as pd

# Local imports
ROOT_DIR = os.path.abspath(os.curdir)
sys.path.append(ROOT_DIR)
from scripts.modules.database_exporter import DatabaseExporter

# Database configuration
DEFAULT_DB_NAME = "oa_data_raw.db"
OUTPUT_FILE = "author.parquet"

# Age standardization settings
AGE_STD_PREFIX = "1"  # Prefix for age standardization
AGE_PADDING_WIDTH = 3  # Zero-pad age to 3 digits
MONTH_RANGE = (1, 12)  # Random month range
DAY_RANGE = (1, 28)    # Random day range (avoid leap year issues)

# Data validation
REQUIRED_COLUMNS = ['aid', 'author_age']

# Age Standardization Format:
# age_std = "1{author_age:03d}-{random_month:02d}-{random_day:02d}"
# 
# Examples:
# - Author age 5  -> "1005-03-15" (career year 5, random month/day)
# - Author age 25 -> "1025-11-08" (career year 25, random month/day)
#
# Purpose: Enables timeline visualization where:
# - Year component represents career stage
# - Month/day provide smooth animation transitions
# - Consistent format works with D3.js date parsing


def parse_args():
    """
    Parse command line arguments for author preprocessing.
    
    Returns:
        argparse.Namespace: Parsed arguments with input and output paths
    """
    parser = argparse.ArgumentParser(
        "Author Data Preprocessor",
        description="Process author records for visualization and analysis"
    )
    parser.add_argument(
        "-i", "--input", type=Path, required=True,
        help="Input directory containing oa_data_raw.db database"
    )
    parser.add_argument(
        "-o", "--output", type=Path, required=True,
        help="Output directory for processed author.parquet file"
    )
    return parser.parse_args()


def generate_random_date_components(size):
    """
    Generate random month and day components for date standardization.
    
    Args:
        size (int): Number of random date components to generate
        
    Returns:
        tuple: (months, days) as zero-padded string arrays
    """
    months = np.char.zfill(
        np.random.randint(MONTH_RANGE[0], MONTH_RANGE[1] + 1, size).astype(str), 
        2
    )
    days = np.char.zfill(
        np.random.randint(DAY_RANGE[0], DAY_RANGE[1] + 1, size).astype(str), 
        2
    )
    return months, days


def create_age_standardization(df):
    """
    Create standardized age representation for timeline visualization.
    
    The age_std format enables smooth temporal animations in frontend:
    - Format: "1{age:03d}-{month:02d}-{day:02d}"
    - Year component represents career stage (1000 + author_age)
    - Month/day provide random variation for animation smoothness
    
    Args:
        df (pd.DataFrame): DataFrame with author_age column
        
    Returns:
        pd.DataFrame: DataFrame with added age_std column
    """
    print("Creating standardized age representation for visualization...")
    
    try:
        # Generate random date components
        months, days = generate_random_date_components(len(df))
        
        # Create age_std with consistent formatting
        df["age_std"] = (
            AGE_STD_PREFIX + 
            df.author_age.astype(str).str.replace(".0", "").map(lambda x: x.zfill(AGE_PADDING_WIDTH)) + 
            "-" + months + "-" + days
        )
        
        print("Successfully created age_std column using vectorized approach")
        
    except Exception as e:
        print(f"Error in vectorized approach: {e}")
        print("Falling back to row-by-row processing...")
        
        # Fallback approach with better error handling
        df["age_std"] = df.apply(
            lambda row: (
                f"{AGE_STD_PREFIX}{str(int(row.author_age)).zfill(AGE_PADDING_WIDTH)}-"
                f"{np.random.randint(MONTH_RANGE[0], MONTH_RANGE[1] + 1):02d}-"
                f"{np.random.randint(DAY_RANGE[0], DAY_RANGE[1] + 1):02d}"
            ) if not pd.isna(row.author_age) else None, 
            axis=1
        )
        
        print("Created age_std column with fallback approach")
    
    # Handle leap year edge case (Feb 29 -> Feb 28)
    df["age_std"] = df.age_std.map(
        lambda x: x.replace("29", "28") if x and x.endswith("29") else x
    )
    
    return df


def validate_data_quality(df):
    """
    Validate data quality and report issues.
    
    Args:
        df (pd.DataFrame): Author DataFrame to validate
        
    Returns:
        pd.DataFrame: Validated DataFrame
    """
    print("Validating data quality...")
    
    # Check for required columns
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        print(f"Warning: Missing required columns: {missing_cols}")
        print(f"Available columns: {df.columns.tolist()}")
        return df
    
    # Report data quality metrics
    total_records = len(df)
    missing_age = df.author_age.isna().sum()
    missing_aid = df.aid.isna().sum()
    
    print(f"Data Quality Report:")
    print(f"  - Total records: {total_records:,}")
    print(f"  - Missing author_age: {missing_age:,} ({missing_age/total_records*100:.1f}%)")
    print(f"  - Missing aid: {missing_aid:,} ({missing_aid/total_records*100:.1f}%)")
    
    if missing_age > 0:
        print(f"  - Records with valid age: {total_records - missing_age:,}")
    
    # Check age distribution
    if not df.author_age.isna().all():
        print(f"  - Age range: {df.author_age.min():.0f} to {df.author_age.max():.0f} years")
        print(f"  - Mean age: {df.author_age.mean():.1f} years")
    
    return df


def main():
    """
    Main processing pipeline:
    1. Load author data from database
    2. Validate required columns exist
    3. Create standardized age representation (age_std) for visualization:
       - Format: "1{age:03d}-{month:02d}-{day:02d}"
       - Enables smooth temporal animations in frontend
    4. Handle missing values and data type conversion
    5. Export processed dataset to parquet format
    """
    args = parse_args()
    
    # Initialize database connection
    db_path = args.input / DEFAULT_DB_NAME
    print(f"Connecting to database at {db_path}")
    db_exporter = DatabaseExporter(str(db_path))
    
    # Load author data from database
    print("Querying author data from database...")
    df = db_exporter.con.sql("SELECT * FROM author").fetchdf()
    print(f"Retrieved {len(df):,} author records")
    
    # Validate data quality
    df = validate_data_quality(df)
    
    # Create standardized age representation
    df = create_age_standardization(df)
    
    # Validate age_std creation
    valid_age_std = df.age_std.notna().sum()
    print(f"Successfully created age_std for {valid_age_std:,} records")
    
    # Save processed data
    output_path = args.output / OUTPUT_FILE
    print(f"Saving {len(df):,} processed author records to {output_path}")
    df.to_parquet(output_path)
    print("Author preprocessing completed successfully!")
    
    # Print processing summary
    print("\n=== Processing Summary ===")
    print(f"Total authors processed: {len(df):,}")
    print(f"Unique author IDs: {df.aid.nunique():,}")
    if 'institution' in df.columns:
        print(f"Unique institutions: {df.institution.nunique():,}")
    if 'pub_year' in df.columns:
        print(f"Year range: {df.pub_year.min()}-{df.pub_year.max()}")
    print(f"Records with age_std: {valid_age_std:,}")
    
    # Close database connection
    db_exporter.close()


if __name__ == "__main__":
    main()