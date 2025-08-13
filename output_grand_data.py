from utils import *

def outputs_grand_data_process_row(row):
        output_grants_data_from_row = []
        openalex_output_id = safe_get_column(row, 'id')
        output_hg_id = generate_hg_id(openalex_output_id)
        grant_award_ids = parse_list_string(safe_get_column(row, 'grants.award_id'))
        grant_funders = parse_list_string(safe_get_column(row, 'grants.funder'))
        grant_funder_display_names = parse_list_string(safe_get_column(row, 'grants.funder_display_name'))

        for i in range(len(grant_award_ids)):
            grant_award_id = grant_award_ids[i] if i < len(grant_award_ids) else None
            if not grant_award_id:
                continue
            grant_hg_id = generate_hg_id(grant_award_id)
            # grants_data_from_row.append({
            #     'hg_id': grant_hg_id,
            #     'award_id': grant_award_id,
            #     'funder': grant_funders[i] if i < len(grant_funders) else None,
            #     'funder_display_name': grant_funder_display_names[i] if i < len(grant_funder_display_names) else None
            # })
            output_grants_data_from_row.append({
                'output_id': output_hg_id,
                'grant_id': grant_hg_id
            })
        return output_grants_data_from_row            
