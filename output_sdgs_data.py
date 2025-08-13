from utils import *

def outputs_sdgs_data_process_row(row):
    output_sdgs_data_from_row = []
    openalex_output_id = safe_get_column(row, 'id')
    output_hg_id = generate_hg_id(openalex_output_id)
    openalex_sdg_ids = parse_list_string(safe_get_column(row, 'sustainable_development_goals.id'))
    sdg_display_names = parse_list_string(safe_get_column(row, 'sustainable_development_goals.display_name'))
    sdg_scores = [safe_parse_numeric(s) for s in parse_list_string(safe_get_column(row, 'sustainable_development_goals.score'))]

    for i in range(len(openalex_sdg_ids)):
        openalex_sdg_id = openalex_sdg_ids[i] if i < len(openalex_sdg_ids) else None
        if not openalex_sdg_id:
            continue
        sdg_hg_id = generate_hg_id(openalex_sdg_id)
        current_sdg_score = sdg_scores[i] if i < len(sdg_scores) else None
        output_sdgs_data_from_row.append({
            'output_id': output_hg_id,
            'sdg_id': sdg_hg_id,
            'score': current_sdg_score
        })
    return output_sdgs_data_from_row    