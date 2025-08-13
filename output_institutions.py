from utils import *
def output_institution_process_row(row):
    output_institutions_data_from_row = []
    # 1. From authorships.affiliations
    authorships_affiliations_raw = safe_get_column(row, 'authorships.institutions')
    parsed_affiliations_list = parse_list_string(authorships_affiliations_raw) 
    openalex_output_id = safe_get_column(row, 'id')
    output_hg_id = generate_hg_id(openalex_output_id)

    for aff_json_str in parsed_affiliations_list:
        parsed_aff_obj = parse_jsonb_string(aff_json_str.replace("'", "\""))
        
        if parsed_aff_obj and isinstance(parsed_aff_obj, dict):
            # Prioritize direct institution details if present
            inst_openalex_id = parsed_aff_obj.get('id')

            if inst_openalex_id:
                inst_hg_id = generate_hg_id(inst_openalex_id)
                
                output_institutions_data_from_row.append({
                    'output_id': output_hg_id,
                    'institution_id': inst_hg_id
                })
            
            # Fallback for older or different format where only 'institution_ids' is nested
            # This handles cases where raw_affiliation_string might be present and institution_ids is nested
            elif 'institution_ids' in parsed_aff_obj and isinstance(parsed_aff_obj.get('institution_ids'), list):
                for nested_inst_id in parsed_aff_obj['institution_ids']:
                    if nested_inst_id:
                        nested_inst_hg_id = generate_hg_id(nested_inst_id)
                        
                        output_institutions_data_from_row.append({
                            'output_id': output_hg_id,
                            'institution_id': nested_inst_hg_id
                        })
                        
    # 2. From corresponding_institution_ids (if it exists and is a direct OpenAlex ID)
    corresponding_inst_ids_raw = safe_get_column(row, 'corresponding_institution_ids')
    corresponding_inst_ids = parse_list_string(corresponding_inst_ids_raw) # Assuming it's a pipe-separated list of IDs

    for inst_openalex_id in corresponding_inst_ids:
        if inst_openalex_id:
            inst_hg_id = generate_hg_id(inst_openalex_id)
           
            output_institutions_data_from_row.append({
                'output_id': output_hg_id,
                'institution_id': inst_hg_id
            })

    return output_institutions_data_from_row