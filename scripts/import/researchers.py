
import pandas as pd
from pathlib import Path
import argparse
# from scripts.modules.data_fetcher import OpenAlexFetcher

def parse_args():
    parser = argparse.ArgumentParser("Data Downloader")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Output directory for researcher data",
        required=True,
    )
    return parser.parse_args()


def main():
    args = parse_args()
    OUTPUT_DIR = args.output

    # Read data from parquet
    d = pd.read_parquet("data/raw/uvm_profs_2023.parquet")

    # Note: We're keeping the original column selection logic
    # but mapping to the new column name format
    # The "OpenAlex id" column is now "oa_uid"
    cols = ['oa_display_name', 'is_prof', 'group_size', 'perceived_as_male', 
            'host_dept (; delimited if more than one)', 'has_research_group', 
            'oa_uid', 'group_url', 'first_pub_year'] 
 
    # Export to TSV
    d.to_csv(OUTPUT_DIR / "researchers.tsv", sep="\t", index=False)

if __name__ == "__main__":
    main()