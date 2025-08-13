from utils import *

def output_external_ids_process_row(row):
    output_external_ids_data_from_row = []
    openalex_output_id = safe_get_column(row, 'id')
    output_hg_id = generate_hg_id(openalex_output_id)

    if safe_get_column(row, 'ids.doi'):
        output_external_ids_data_from_row.append({'output_id': output_hg_id, 'id_type': 'doi', 'external_id': str(safe_get_column(row, 'ids.doi')).replace('https://doi.org/', '')})
    if safe_get_column(row, 'ids.mag'):
        output_external_ids_data_from_row.append({'output_id': output_hg_id, 'id_type': 'mag', 'external_id': str(safe_get_column(row, 'ids.mag'))})
    if safe_get_column(row, 'ids.pmid'):
        output_external_ids_data_from_row.append({'output_id': output_hg_id, 'id_type': 'pmid', 'external_id': str(safe_get_column(row, 'ids.pmid'))})
    if safe_get_column(row, 'ids.pmcid'):
        output_external_ids_data_from_row.append({'output_id': output_hg_id, 'id_type': 'pmcid', 'external_id': str(safe_get_column(row, 'ids.pmcid'))})
        
    return output_external_ids_data_from_row
