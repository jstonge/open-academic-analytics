import pandas as pd
from pathlib import Path
import argparse

def parse_args():
    parser = argparse.ArgumentParser("Data Downloader")
    parser.add_argument(
        "-o",
        "--output",
        type= Path,
        help="JSONlines file with urls and hashes",
        required=True,
    )
    return parser.parse_args()


def main():
    # ANNOT_DIR = Path("../docs/data/raw/")
    args = parse_args()
    OUTPUT_DIR = args.output

    # Pretty bad way to do it. I need to remember how to use the Gdrive API.
    d = pd.read_csv("/Users/jstonge1/Downloads/Finding Principal Investigators (PIs) - PIs with a research group(14).tsv", sep="\t") 
    d2 = pd.read_csv("/Users/jstonge1/Downloads/Finding Principal Investigators (PIs) - nsf-gov_researchExperienceSites(13).tsv", sep="\t")
    d2 = d2[~d2.oa_display_name.isna()]  
    d3 = pd.read_csv("/Users/jstonge1/Downloads/Finding Principal Investigators (PIs) - uvm(1).tsv", sep="\t")

    cols = ['oa_display_name', 'is_prof', 'group_size', 'perceived_as_male', 
            'host_dept (; delimited if more than one)',   'has_research_group', 
            'OpenAlex id', 'group_url', 'first_pub_year'] 

    d = pd.concat([d[cols],d2[cols], d3[cols]], axis=0) 
    d.to_csv(OUTPUT_DIR / "researchers.tsv", sep="\t", index=False)

if __name__ == "__main__":
    main()