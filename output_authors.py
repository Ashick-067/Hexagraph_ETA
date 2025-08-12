from utils import *

def output_authors_process_row(row):
    output_authors_data_from_row = []
    openalex_output_id = safe_get_column(row, 'id')
    output_hg_id = generate_hg_id(openalex_output_id)

    #     # --- Authors and Output_Authors Data ---
    author_ids = parse_list_string(safe_get_column(row, 'authorships.author.id'))
    author_positions = parse_list_string(safe_get_column(row, 'authorships.author_position'))
    is_correspondings = parse_list_string(safe_get_column(row, 'authorships.is_corresponding'))
    raw_author_names = parse_list_string(safe_get_column(row, 'authorships.raw_author_name'))
    
    raw_affiliation_strings_raw_list = parse_list_string(safe_get_column(row, 'authorships.raw_affiliation_strings'))
    raw_affiliation_strings_processed = []
    for aff_str_item in raw_affiliation_strings_raw_list:
        parsed_aff_json = parse_jsonb_string(aff_str_item)
        if isinstance(parsed_aff_json, list):
            raw_affiliation_strings_processed.append(parsed_aff_json)
        else:
            # Ensure that even if parsed_aff_json is a dict, it's wrapped in a list for TEXT[]
            raw_affiliation_strings_processed.append([aff_str_item])
    
    # Process authorships, but defer institution collection to a dedicated section
    for i in range(len(author_ids)):
        author_openalex_id = author_ids[i] if i < len(author_ids) else None
        if not author_openalex_id:
            continue

        author_hg_id = generate_hg_id(author_openalex_id)
        

        output_authors_data_from_row.append({
            'output_id': output_hg_id,
            'author_id': author_hg_id,
            'author_position': author_positions[i] if i < len(author_positions) else None,
            'is_corresponding': safe_parse_boolean(is_correspondings[i]) if i < len(is_correspondings) else False, # Apply safe_parse_boolean
            'raw_author_name': raw_author_names[i] if i < len(raw_author_names) else None,
            'raw_affiliation_strings': raw_affiliation_strings_processed[i] if i < len(raw_affiliation_strings_processed) else []
        })
    return output_authors_data_from_row