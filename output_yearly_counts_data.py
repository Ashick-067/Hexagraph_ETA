from utils import *

def outputs_yearly_counts_data_process_row(row):
    output_yearly_counts_data_from_row = []
    openalex_output_id = safe_get_column(row, 'id')
    output_hg_id = generate_hg_id(openalex_output_id)
    years_str = safe_get_column(row, 'counts_by_year.year')
    cited_by_counts_str = safe_get_column(row, 'counts_by_year.cited_by_count')

    years = parse_list_string(years_str)
    cited_by_counts = [safe_parse_int(c) for c in parse_list_string(cited_by_counts_str)]

    for i in range(len(years)):
        year = years[i] if i < len(years) else None
        cited_count = cited_by_counts[i] if i < len(cited_by_counts) else None

        if year is not None and cited_count is not None:
            output_yearly_counts_data_from_row.append({
                'output_id': output_hg_id,
                'year': year,
                'cited_by_count': cited_count
            })
    return output_yearly_counts_data_from_row        