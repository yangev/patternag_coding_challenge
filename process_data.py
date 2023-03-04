import csv
import sys
import json
import argparse
import fileinput
import subprocess

from pathlib import Path
from decimal import Decimal


TAXONOMIC_RANK = [
    'superkingdom',
    'phylum',
    'class',
    'order',
    'family',
    'genus',
    'species'
]


def generate_lineage_identifier(lineage: dict) -> str:
    """
    Notes:
    I am working under these requirements:
    * We should be able to determine from the hash value if it matches a
    component e.g. phylum: cyanobacteria. This also means that collisions
    should be avoided entirely
    * Inputs are reasonably short, and the range of possible values, even at
    the species level, is relatively small
    * I noticed that in this data, values are prefixed by level, e.g.
    d__Bacteria. I don't want to assume anything about the values, though
    I decided to go with |-separated values ordered by rank
    Alternatives considered:
    * Well known hash functions will generate an output larger than the size of
    the input. Truncating the values risks collisions
    * Hand-rolled hash functions risk collisions
    * Compressing the input makes it annoying to search by component. Many
    places where IO is a bottleneck already compress the data anyway (eg http)
    * Enumerations would be viable, but also require managing and transporting
    enumeration structs
    """
    identifier = "|".join([lineage[k] for k in TAXONOMIC_RANK])
    return identifier


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


def process_row(data: dict, entry: dict) -> None:
    """
    Notes:
    * I have no experience with scientific python, but I'd imagine that it would
    be faster and easier to use it for aggregating tabular data like this. But
    I'm doing this imperatively
    * Though I don't know what actually makes sense to aggregate. I don't know
    the distances of the sample sites. It wouldn't make sense to aggregate
    stats from sites far apart from each other
    * I don't know how to interpret read_count, relative_abundance, or
    total_filtered reads.
    Naively, it also doesn't seem correct to sum numerical stats for the same
    species but different orthologs.
    * Therefore, I'm just tallying, per species, the samples where it was
    found, and the number of unique orthologs per site.
    """
    identifier = generate_lineage_identifier(entry['lineage_rank'])
    if not data.get(identifier):
        data[identifier] = {}
    sample_id = entry['sample_id']
    if not data[identifier].get(sample_id):
        data[identifier][sample_id] = set()
    data[identifier][sample_id].add(entry['kegg_ortholog'])


def process_species_data(species_data: dict) -> dict:
    """
    Collects the unique species per genus per sample
    Collects the unique species per family per sample
    """
    genus_data = {}
    family_data = {}
    for lineage_id in species_data.keys():
        components = lineage_id.split('|')
        species, genus, family = components[-1], components[-2], components[-3]
        if not genus_data.get(genus):
            genus_data[genus] = {}
        if not family_data.get(family):
            family_data[family] = {}
        for sample in species_data[lineage_id].keys():
            if not genus_data[genus].get(sample):
                genus_data[genus][sample] = set()
            if not family_data[family].get(sample):
                family_data[family][sample] = set()
            genus_data[genus][sample].add(species)
            family_data[family][sample].add(species)

    output_data = {}
    output_data["species_data"] = {sp: {sa: len(ko) for sa, ko in samples.items()}
                                   for sp, samples in species_data.items()}
    output_data["genus_data"] = {ge: {sa: len(sp) for sa, sp in samples.items()}
                                 for ge, samples in genus_data.items()}
    output_data["family_data"] = {fm: {sa: len(sp) for sa, sp in samples.items()}
                                  for fm, samples in family_data.items()}
    return output_data


def upload_to_bucket(bucket_name, output_file):
    subprocess.run(['gsutil', 'cp', output_file, bucket_name])


def main() -> int:
    parser = argparse.ArgumentParser(prog='process_data.py',
                                     description='processes data from a directory')
    parser.add_argument("-d",
                        "--src-dir",
                        help="Source directory of the data")
    parser.add_argument("-f",
                        "--output-file",
                        help="writes the processed data to a file")
    parser.add_argument("-b",
                        "--bucket-name",
                        help="Upload to GCP bucket. If omitted, skip uploading --output-file must also be set")

    args = parser.parse_args()
    src_dir, output_file, bucket_name = args.src_dir, args.output_file, args.bucket_name
    if src_dir is None:
        src_dir = "."

    if not output_file and bucket_name:
        print("must set --output-file if --bucket-name is set", file=sys.stderr)
        return 0

    species_data = {}

    filepaths = [f for f in Path(src_dir).iterdir() if f.suffix == ".csv"]
    with fileinput.input(files=filepaths) as f:
        it = csv.DictReader(f)
        for row_dict in it:
            # DictReader will automatically consume the first line of the first
            # file to generate the dict fields, but treat the rest of the
            # iterator as data rows. When processing subsequent
            # files, we need to skip the header
            if f.filelineno() == 1:
                continue

            reify_row(row_dict)
            process_row(species_data, row_dict)
    output_data = process_species_data(species_data)
    as_json = json.dumps(output_data)
    if output_file:
        with open(output_file, 'w') as f:
            print(as_json, file=f)
    if bucket_name:
        upload_to_bucket(bucket_name, output_file)

    return 1


if __name__ == '__main__':
    sys.exit(main())
