from utils import *;
import pandas as pd

def institutions_process_row(row):
    institutions_data_from_row = []
    authorships_affiliations_raw = safe_get_column(row, 'authorships.institutions')
    parsed_affiliations_list = parse_list_string(authorships_affiliations_raw) 

    for aff_json_str in parsed_affiliations_list:
        parsed_aff_obj = parse_jsonb_string(aff_json_str.replace("'", "\""))
        
        if parsed_aff_obj and isinstance(parsed_aff_obj, dict):
            inst_openalex_id = parsed_aff_obj.get('id')
            inst_display_name = parsed_aff_obj.get('display_name')
            inst_ror = parsed_aff_obj.get('ror')
            inst_country_code = parsed_aff_obj.get('country_code')
            inst_type = parsed_aff_obj.get('type')

           # Correctly handle lineage which might be a list or a string needing parsing
            inst_lineage_raw = parsed_aff_obj.get('lineage')
            if isinstance(inst_lineage_raw, list):
                inst_lineage = [str(item).strip() for item in inst_lineage_raw if pd.notna(item)]
            elif isinstance(inst_lineage_raw, str):
                inst_lineage = parse_list_string(inst_lineage_raw)
            else:
                inst_lineage = [] # Default if not found or invalid type
            if inst_openalex_id :
                inst_hg_id = generate_hg_id(inst_openalex_id)
                institutions_data_from_row.append({
                    'hg_id': inst_hg_id,
                    'openalex_id': inst_openalex_id,
                    'display_name': inst_display_name,
                    'ror': inst_ror,
                    'country_code': inst_country_code,
                    'type': inst_type,
                    'lineage': inst_lineage
                })
            elif 'institution_ids' in parsed_aff_obj and isinstance(parsed_aff_obj.get('institution_ids'), list):
                for nested_inst_id in parsed_aff_obj['institution_ids']:
                    if nested_inst_id:
                        nested_inst_hg_id = generate_hg_id(nested_inst_id)
                        institutions_data_from_row.append({
                            'hg_id': nested_inst_hg_id,
                            'openalex_id': nested_inst_id,
                            # 'display_name': None, # Not available at this level
                            # 'ror': None,
                            # 'country_code': None,
                            # 'type': None,
                            # 'lineage': []
                        })  
    corresponding_inst_ids_raw = safe_get_column(row, 'corresponding_institution_ids')
    corresponding_inst_ids = parse_list_string(corresponding_inst_ids_raw) # Assuming it's a pipe-separated list of IDs

    for inst_openalex_id in corresponding_inst_ids:
        if inst_openalex_id:
            inst_hg_id = generate_hg_id(inst_openalex_id)
            institutions_data_from_row.append({
                'hg_id': inst_hg_id,
                'openalex_id': inst_openalex_id,
                # 'display_name': None, # Placeholder, as this column only contains the ID
                # 'ror': None,
                # 'country_code': None,
                # 'type': None,
                # 'lineage': []
            })                
    return institutions_data_from_row  

