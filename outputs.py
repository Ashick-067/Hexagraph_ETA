from utils import *;

def outputs_process_row(row):
     output_data = {}
     openalex_output_id = safe_get_column(row, 'id')
     output_hg_id = generate_hg_id(openalex_output_id)

    # --- Transform Outputs Data ---
     output_data = {
        'hg_id': output_hg_id,
        'openalex_id': openalex_output_id,
        'doi': safe_get_column(row, 'doi', '').replace('https://doi.org/', '') if safe_get_column(row, 'doi') else None,
        'title': safe_get_column(row, 'title'),
        'display_name': safe_get_column(row, 'display_name'),
        'publication_year': safe_parse_int(safe_get_column(row, 'publication_year')),
        'publication_date': parse_date(safe_get_column(row, 'publication_date')),
        'language': safe_get_column(row, 'language'),
        'type': safe_get_column(row, 'type'),
        'type_crossref': safe_get_column(row, 'type_crossref'),
        'cited_by_count': safe_parse_int(safe_get_column(row, 'cited_by_count')),
        'abstract': safe_get_column(row, 'abstract'),
        'is_retracted': safe_parse_boolean(safe_get_column(row, 'is_retracted')), # Apply safe_parse_boolean
        'is_paratext': safe_parse_boolean(safe_get_column(row, 'is_paratext')),   # Apply safe_parse_boolean
        'locations_count': safe_parse_int(safe_get_column(row, 'locations_count')),
        'datasets_json': parse_jsonb_string(safe_get_column(row, 'datasets')),
        'versions_json': parse_jsonb_string(safe_get_column(row, 'versions')),
        'referenced_works_count': safe_parse_int(safe_get_column(row, 'referenced_works_count')),
        'cited_by_api_url': safe_get_column(row, 'cited_by_api_url'),
        'is_authors_truncated': safe_parse_boolean(safe_get_column(row, 'is_authors_truncated')), # Apply safe_parse_boolean
        'apc_list_value': safe_parse_numeric(safe_get_column(row, 'apc_list.value')),
        'apc_list_currency': safe_get_column(row, 'apc_list.currency'),
        'apc_list_value_usd': safe_parse_numeric(safe_get_column(row, 'apc_list.value_usd')),
        'apc_paid_value': safe_parse_numeric(safe_get_column(row, 'apc_paid.value')),
        'apc_paid_currency': safe_get_column(row, 'apc_paid.currency'),
        'apc_paid_value_usd': safe_parse_numeric(safe_get_column(row, 'apc_paid.value_usd')),
        'open_access_is_oa': safe_parse_boolean(safe_get_column(row, 'open_access.is_oa')), # Apply safe_parse_boolean
        'open_access_oa_status': safe_get_column(row, 'open_access.oa_status'),
        'open_access_oa_url': safe_get_column(row, 'open_access.oa_url'),
        'open_access_any_repository_has_fulltext': safe_parse_boolean(safe_get_column(row, 'open_access.any_repository_has_fulltext')), # Apply safe_parse_boolean
        'citation_normalized_percentile_value': safe_parse_numeric(safe_get_column(row, 'citation_normalized_percentile.value')),
        'citation_normalized_percentile_is_in_top_1_percent': safe_parse_boolean(safe_get_column(row, 'citation_normalized_percentile.is_in_top_1_percent')), # Apply safe_parse_boolean
        'citation_normalized_percentile_is_in_top_10_percent': safe_parse_boolean(safe_get_column(row, 'citation_normalized_percentile.is_in_top_10_percent')), # Apply safe_parse_boolean
        'cited_by_percentile_year_min': safe_parse_int(safe_get_column(row, 'cited_by_percentile_year.min')),
        'cited_by_percentile_year_max': safe_parse_int(safe_get_column(row, 'cited_by_percentile_year.max')),
        'biblio_volume': safe_get_column(row, 'biblio.volume'),
        'biblio_issue': safe_get_column(row, 'biblio.issue'),
        'biblio_first_page': safe_get_column(row, 'biblio.first_page'),
        'biblio_last_page': safe_get_column(row, 'biblio.last_page'),
        'updated_date': parse_timestamp(safe_get_column(row, 'updated_date')),
        'created_date': parse_date(safe_get_column(row, 'created_date')),
        'countries_distinct_count': safe_parse_int(safe_get_column(row, 'countries_distinct_count')),
        'institutions_distinct_count': safe_parse_int(safe_get_column(row, 'institutions_distinct_count'))
    }

     return output_data