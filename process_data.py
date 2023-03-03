import csv
import sys
import json
import argparse
import hashlib
import base64

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
    * Again, I am letting this crash on missing data
    """
    identifier = "|".join([lineage[k] for k in TAXONOMIC_RANK])
    m = hashlib.md5()
    m.update(identifier.encode("utf-8"))
    return base64.b64encode(m.digest()).decode('utf-8')


def parse_csv(filepath: Path) -> list[dict]:
    """
    Notes:
    * There's not really any error handling here, and it relies on the CSV and
    data being well formed. I don't really make any attempts at recovery, since
    we probably want to fail instead of silently ingesting malformed data
    """
    with open(filepath) as csvfile:
        h = csv.DictReader(csvfile)
        entries = []
        for entry in h:
            lineage_rank = entry.get('lineage_rank')
            if lineage_rank:
                entry['lineage_rank'] = json.loads(lineage_rank)
            print(generate_hash(entry['lineage_rank']))
            entries.append(entry)
        return entries


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

    for filepath in Path(dest_dir).iterdir():
        if filepath.suffix == ".csv":
            parse_csv(filepath)
    return 1


if __name__ == '__main__':
    sys.exit(main())
