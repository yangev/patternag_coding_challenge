import pytest
from process_data import generate_lineage_identifier, process_row, process_species_data
from decimal import Decimal


class TestGenerateLineageIdentifier:
    def test_data_in_taxonomic_order(self):
        lineage = {
            'superkingdom': 'kids',
            'phylum': 'playing',
            'class': 'chess',
            'order': 'on',
            'family': 'freeways',
            'genus': 'get',
            'species': 'squashed'
        }
        row_dict = {}
        row_dict['lineage_rank'] = lineage
        lineage_id = generate_lineage_identifier(lineage)
        species = lineage_id.split('|')
        assert species[-1] == 'squashed'


class TestProcessRow:
    lineage = {
        'superkingdom': 'kids',
        'phylum': 'playing',
        'class': 'chess',
        'order': 'on',
        'family': 'freeways',
        'genus': 'get',
        'species': 'squashed'
    }
    lineage_2 = {
        'superkingdom': 'kids',
        'phylum': 'playing',
        'class': 'chess',
        'order': 'on',
        'family': 'freeways',
        'genus': 'get',
        'species': 'squished'
    }

    base_row = {'sample_id': 'sa-1',
                'kegg_ortholog': 'ko1',
                'lineage_rank': lineage,
                'read_count': 1,
                'relative_abundance': Decimal('0.1'),
                'total_filtered_reads': 20,
                'rank': 'species',
                'taxon_name': 's__taxon'}

    def test_process_single_row(self):
        lineage_id = generate_lineage_identifier(TestProcessRow.base_row['lineage_rank'])
        species_data = {}
        process_row(species_data, TestProcessRow.base_row)
        assert 'ko1' in species_data[lineage_id]['sa-1']

    def test_process_single_row_same_sample_diff_species(self):
        lineage_id_1 = generate_lineage_identifier(TestProcessRow.lineage)
        species_data = {}
        process_row(species_data, TestProcessRow.base_row)
        lineage_id_2 = generate_lineage_identifier(TestProcessRow.lineage_2)
        row_2 = {k: v for k, v in TestProcessRow.base_row.items()}
        row_2['lineage_rank'] = TestProcessRow.lineage_2
        process_row(species_data, row_2)
        assert 'ko1' in species_data[lineage_id_1]['sa-1']
        assert 'ko1' in species_data[lineage_id_2]['sa-1']

    def test_process_single_row_same_sample_diff_orthlog(self):
        lineage_id_1 = generate_lineage_identifier(TestProcessRow.lineage)
        species_data = {}
        process_row(species_data, TestProcessRow.base_row)
        row_2 = {k: v for k, v in TestProcessRow.base_row.items()}
        row_2['kegg_ortholog'] = 'ko2'
        process_row(species_data, row_2)
        assert 'ko1' in species_data[lineage_id_1]['sa-1']
        assert 'ko2' in species_data[lineage_id_1]['sa-1']

    def test_process_single_row_diff_sample(self):
        lineage_id_1 = generate_lineage_identifier(TestProcessRow.lineage)
        species_data = {}
        process_row(species_data, TestProcessRow.base_row)
        row_2 = {k: v for k, v in TestProcessRow.base_row.items()}
        row_2['sample_id'] = 'sa-2'
        process_row(species_data, row_2)
        assert 'ko1' in species_data[lineage_id_1]['sa-1']
        assert 'ko1' in species_data[lineage_id_1]['sa-2']


@pytest.fixture
def species_data():
    lineage = {
        'superkingdom': 'kids',
        'phylum': 'playing',
        'class': 'chess',
        'order': 'on',
        'family': 'freeways',
        'genus': 'get',
        'species': 'squashed'
    }
    lineage_2 = {
        'superkingdom': 'kids',
        'phylum': 'playing',
        'class': 'chess',
        'order': 'on',
        'family': 'freeways',
        'genus': 'got',
        'species': 'squished'
    }
    base_row = {'sample_id': 'sa-1',
                'kegg_ortholog': 'ko1',
                'lineage_rank': lineage,
                'read_count': 1,
                'relative_abundance': Decimal('0.1'),
                'total_filtered_reads': 20,
                'rank': 'species',
                'taxon_name': 's__taxon'}
    base_row_2 = {'sample_id': 'sa-2',
                  'kegg_ortholog': 'ko1',
                  'lineage_rank': lineage_2,
                  'read_count': 1,
                  'relative_abundance': Decimal('0.1'),
                  'total_filtered_reads': 20,
                  'rank': 'species',
                  'taxon_name': 's__taxon'}
    base_row_3 = {'sample_id': 'sa-2',
                  'kegg_ortholog': 'ko2',
                  'lineage_rank': lineage,
                  'read_count': 1,
                  'relative_abundance': Decimal('0.1'),
                  'total_filtered_reads': 20,
                  'rank': 'species',
                  'taxon_name': 's__taxon'}
    species_data = {}
    process_row(species_data, base_row)
    process_row(species_data, base_row_2)
    process_row(species_data, base_row_3)
    yield species_data


class TestProcessSpeciesData:
    def test_process_species_data(self, species_data):
        agg_data = process_species_data(species_data)
        assert agg_data['family_data']['freeways']['sa-1'] == 1
        assert agg_data['family_data']['freeways']['sa-2'] == 2
        assert agg_data['genus_data']['got']['sa-2'] == 1
        assert agg_data['genus_data']['get']['sa-1'] == 1
