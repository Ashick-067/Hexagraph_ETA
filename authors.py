from utils import *;

def authors_process_row(row):
    authors_data_from_row = [] 
    author_ids = parse_list_string(safe_get_column(row, 'authorships.author.id'))
    author_display_names = parse_list_string(safe_get_column(row, 'authorships.author.display_name'))
    author_orcids = parse_list_string(safe_get_column(row, 'authorships.author.orcid'))
    author_positions = parse_list_string(safe_get_column(row, 'authorships.author_position'))
    is_correspondings = parse_list_string(safe_get_column(row, 'authorships.is_corresponding'))
    raw_author_names = parse_list_string(safe_get_column(row, 'authorships.raw_author_name'))
    authors_updated_dates = parse_timestamp(safe_get_column(row, 'updated_date'))
    authors_created_dates = parse_date(safe_get_column(row, 'created_date'))

    for i in range(len(author_ids)):
        author_openalex_id = author_ids[i] if i < len(author_ids) else None
        if not author_openalex_id:
            continue

        author_hg_id = generate_hg_id(author_openalex_id)
        
        authors_data_from_row.append({
            'hg_id': author_hg_id,
            'openalex_id': author_openalex_id,
            'display_name': author_display_names[i] if i < len(author_display_names) else None,
            'orcid': author_orcids[i] if i < len(author_orcids) else None,
            'updated_date': authors_updated_dates,
            'created_date': authors_created_dates
        })

    return authors_data_from_row
        # collected_authors = {}
        # for a in authors_data_from_row:
        #         if a.get('hg_id'):
        #             collected_authors[a['hg_id']] = a
        # print(f"\nFinished processing authors rows from CSV.")
        # print("Preparing data for load...")
        # final_authors_data = list(collected_authors.values())
        # return final_authors_data
