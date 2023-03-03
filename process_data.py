import csv
import sys
import json
import argparse
import hashlib
import base64
import fileinput

from pathlib import Path
from decimal import Decimal

"""
* Write a simple program that would:
  - Iterate over all files
  - Creates a unique hash representing the lineage
  - Provides stats (going to leave this for you to pick a few that you think would be useful) based on family,
    genus and species levels (see lineage rank)
  - Write stats in a JSON file, and upload back to the bucket using shell and gsutil
"""

TAXONOMIC_RANK = [
    'superkingdom',
    'phylum',
    'class',
    'order',
    'family',
    'genus',
    'species'
]


def generate_hash(lineage: dict) -> str:
    """
    Notes:
    * I'm not quite sure how this hash is used, but my assumption is
    that it does not have to be easily memorizable by humans, and that we don't
    have to worry about adversarial parties intentionally causing hash
    collisions. Thus, I'm using an MD5 of a string of the lineage in taxonomic
    rank for simplicity and resistance to accidental collision
    MD5 is not guaranteed to be included in all distributions of hashlib, but
    is widely available
    * I am unsure if species names may contain non-ASCII characters. I'm
    assuming they can, and am arbitrarily converting them to an utf-8 byte
    representation. The hash string contains only base64 characters
    * I am letting this crash on missing data, as I expect every species to
    have complete taxonomic data
    """
    identifier = "|".join([lineage[k] for k in TAXONOMIC_RANK])
    m = hashlib.md5()
    m.update(identifier.encode("utf-8"))
    return base64.b64encode(m.digest()).decode('utf-8')


def reify_row(row_dict: dict) -> None:
    """
    Converts fields in a raw CSV row_dict into more useful types
    Parses lineage data into a dict, and string representations of numbers to
    numerical types
    Taking a let-it-crash philosophy on missing or incorrectly-typed data,
    as with the rest of this program
    """
    row_dict['lineage_rank'] = json.loads(row_dict['lineage_rank'])
    row_dict['read_count'] = int(row_dict['read_count'])
    row_dict['relative_abundance'] = Decimal(row_dict['relative_abundance'])
    row_dict['total_filtered_reads'] = int(row_dict['total_filtered_reads'])


def main() -> int:
    parser = argparse.ArgumentParser(prog='process_data.py',
                                     description='processes data from a directory')
    parser.add_argument("-d",
                        "--src-dir",
                        help="Source directory of the data")
    args = parser.parse_args()
    dest_dir = args.src_dir
    if dest_dir is None:
        dest_dir = "."

    filepaths = [f for f in Path(dest_dir).iterdir() if f.suffix == ".csv"]
    with fileinput.input(files=filepaths) as f:
        it = csv.DictReader(f)
        for entry in it:
            # DictReader will automatically consume the first line of the first
            # file to generate the dict fields, but treat the rest of the
            # iterator as data rows. When processing subsequent
            # files, we need to skip the header
            if f.filelineno() == 1:
                continue

            reify_row(entry)
            print(entry)
            print(generate_hash(entry['lineage_rank']))

    return 1


if __name__ == '__main__':
    sys.exit(main())
