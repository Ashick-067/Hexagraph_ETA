from utils import *

def source_process_row(row):
    sources_data_from_row = []
    
    loc_is_oas = parse_list_string(safe_get_column(row, 'locations.is_oa'), delimiter='|')
    loc_landing_page_urls = parse_list_string(safe_get_column(row, 'locations.landing_page_url'), delimiter='|')
    loc_pdf_urls = parse_list_string(safe_get_column(row, 'locations.pdf_url'), delimiter='|')
    loc_licenses = parse_list_string(safe_get_column(row, 'locations.license'), delimiter='|')
    loc_license_ids = parse_list_string(safe_get_column(row, 'locations.license_id'), delimiter='|')
    loc_versions = parse_list_string(safe_get_column(row, 'locations.version'), delimiter='|')
    loc_is_accepteds = parse_list_string(safe_get_column(row, 'locations.is_accepted'), delimiter='|')
    loc_is_publisheds = parse_list_string(safe_get_column(row, 'locations.is_published'), delimiter='|')
    loc_source_ids = parse_list_string(safe_get_column(row, 'locations.source.id'), delimiter='|')
    loc_source_display_names = parse_list_string(safe_get_column(row, 'locations.source.display_name'), delimiter='|')
    loc_source_issn_ls = parse_list_string(safe_get_column(row, 'locations.source.issn_l'), delimiter='|')
    loc_source_issns = parse_list_string(safe_get_column(row, 'locations.source.issn'), delimiter='|')
    loc_source_is_oas = parse_list_string(safe_get_column(row, 'locations.source.is_oa'), delimiter='|')
    loc_source_is_in_doajs = parse_list_string(safe_get_column(row, 'locations.source.is_in_doaj'), delimiter='|')
    loc_source_is_indexed_in_scopus = parse_list_string(safe_get_column(row, 'locations.source.is_indexed_in_scopus'), delimiter='|')
    loc_source_is_cores = parse_list_string(safe_get_column(row, 'locations.source.is_core'), delimiter='|')
    loc_source_host_organizations = parse_list_string(safe_get_column(row, 'locations.source.host_organization'), delimiter='|')
    loc_source_host_organization_names = parse_list_string(safe_get_column(row, 'locations.source.host_organization_name'), delimiter='|')
    loc_source_host_organization_lineages = parse_list_string(safe_get_column(row, 'locations.source.host_organization_lineage'), delimiter='|')
    loc_source_host_organization_lineage_names = parse_list_string(safe_get_column(row, 'locations.source.host_organization_lineage_names'), delimiter='|')
    loc_source_types = parse_list_string(safe_get_column(row, 'locations.source.type'), delimiter='|')

    max_loc_len = max(len(loc_is_oas), len(loc_landing_page_urls), len(loc_pdf_urls), len(loc_source_ids))

    for i in range(max_loc_len):
        source_hg_id = None
        openalex_source_id = loc_source_ids[i] if i < len(loc_source_ids) else None

        if openalex_source_id:
            source_hg_id = generate_hg_id(openalex_source_id)
            sources_data_from_row.append({
                'hg_id': source_hg_id,
                'openalex_id': openalex_source_id,
                'display_name': loc_source_display_names[i] if i < len(loc_source_display_names) else None,
                'issn_l': loc_source_issn_ls[i] if i < len(loc_source_issn_ls) else None,
                'issn': parse_list_string(loc_source_issns[i], delimiter=';') if i < len(loc_source_issns) else [],
                'type': loc_source_types[i] if i < len(loc_source_types) else None,
                'is_oa': safe_parse_boolean(loc_source_is_oas[i]) if i < len(loc_source_is_oas) else None, # Apply safe_parse_boolean
                'is_in_doaj': safe_parse_boolean(loc_source_is_in_doajs[i]) if i < len(loc_source_is_in_doajs) else None, # Apply safe_parse_boolean
                'is_indexed_in_scopus': safe_parse_boolean(loc_source_is_indexed_in_scopus[i]) if i < len(loc_source_is_indexed_in_scopus) else None, # Apply safe_parse_boolean
                'is_core': safe_parse_boolean(loc_source_is_cores[i]) if i < len(loc_source_is_cores) else None, # Apply safe_parse_boolean
                'host_organization': loc_source_host_organizations[i] if i < len(loc_source_host_organizations) else None,
                'host_organization_name': loc_source_host_organization_names[i] if i < len(loc_source_host_organization_names) else None,
                'host_organization_lineage': parse_list_string(loc_source_host_organization_lineages[i]) if i < len(loc_source_host_organization_lineages) else [],
                'host_organization_lineage_names': parse_list_string(loc_source_host_organization_lineage_names[i]) if i < len(loc_source_host_organization_lineages) else []
            })
    return  sources_data_from_row       
