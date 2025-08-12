from utils import *;

def add_concepts_and_links(
        concept_ids_str, display_names_str, scores_str, wikidatas_str, levels_str, concept_type, 
        target_output_join_list=None,
        join_has_score=False,
        descriptor_uis_str=None, qualifier_uis_str=None, is_major_topics_str=None
    ):
        concepts_data_from_row = []
        concept_ids = parse_list_string(concept_ids_str)

        
        display_names = parse_list_string(display_names_str)
        scores_list_raw = parse_list_string(scores_str)
        scores = [safe_parse_numeric(s) for s in scores_list_raw]
        
        wikidatas = parse_list_string(wikidatas_str) if wikidatas_str else []
        levels = [safe_parse_int(l) for l in parse_list_string(levels_str)] if levels_str else []
        
        descriptor_uis = parse_list_string(descriptor_uis_str) if descriptor_uis_str else []
        qualifier_uis = parse_list_string(qualifier_uis_str) if qualifier_uis_str else []
        is_major_topics = [safe_parse_boolean(b) for b in parse_list_string(is_major_topics_str)] if is_major_topics_str else [] # Apply safe_parse_boolean


        max_len = len(concept_ids)
        if concept_type == 'mesh':
            max_len = max(max_len, len(descriptor_uis))

        for i in range(max_len):
            openalex_concept_id = concept_ids[i] if i < len(concept_ids) else None
            concept_display_name = display_names[i] if i < len(display_names) else None
            concept_score = scores[i] if i < len(scores) else None
            concept_wikidata = wikidatas[i] if i < len(wikidatas) else None
            concept_level = levels[i] if i < len(levels) else None

            if concept_type == 'mesh':
                mesh_descriptor_ui = descriptor_uis[i] if i < len(descriptor_uis) else None
                if not mesh_descriptor_ui: continue
                openalex_concept_id = f"mesh:{mesh_descriptor_ui}"
                concept_display_name = concept_display_name if concept_display_name else None
                
            if not openalex_concept_id and not concept_display_name:
                continue
            
            concept_hg_id = generate_hg_id(openalex_concept_id or concept_display_name)

            concept_record = {
                'hg_id': concept_hg_id,
                'openalex_id': openalex_concept_id,
                'wikidata': concept_wikidata,
                'display_name': concept_display_name,
                'level': concept_level,
                'score': concept_score,
                'type': concept_type
            }
            concepts_data_from_row.append(concept_record)

            # if target_output_join_list is not None:
            #     join_record = {'output_id': output_hg_id, 'concept_id': concept_hg_id}
            #     if join_has_score:
            #         join_record['score'] = concept_score
            #     if concept_type == 'mesh':
            #         join_record['descriptor_ui'] = mesh_descriptor_ui
            #         join_record['qualifier_ui'] = qualifier_uis[i] if i < len(qualifier_uis) else None
            #         join_record['is_major_topic'] = safe_parse_boolean(is_major_topics[i]) if i < len(is_major_topics) else None # Apply safe_parse_boolean
            #     target_output_join_list.append(join_record)

        return concepts_data_from_row


   
    # Concepts (general)
def concept_process_row(row):
    concepts = []
     # Topics array
    concepts.extend(add_concepts_and_links(
        safe_get_column(row, 'topics.id'),
        safe_get_column(row, 'topics.display_name'),
        safe_get_column(row, 'topics.score'),
        None, safe_get_column(row, 'topics.level'),
        'topic', None, join_has_score=True
    ))

#     # Keywords array
    concepts.extend(add_concepts_and_links(
        safe_get_column(row, 'keywords.id'),
        safe_get_column(row, 'keywords.display_name'),
        safe_get_column(row, 'keywords.score'),
        None, None, 'keyword', None, join_has_score=True
    ))

#     # Mesh terms
    concepts.extend(add_concepts_and_links(
        None,
        safe_get_column(row, 'mesh.descriptor_name'),
        None, None, None,
        'mesh', None, join_has_score=False,
        descriptor_uis_str=safe_get_column(row, 'mesh.descriptor_ui'),
        qualifier_uis_str=safe_get_column(row, 'mesh.qualifier_ui'),
        is_major_topics_str=safe_get_column(row, 'mesh.is_major_topic')
    ))

 #   field
    concepts.extend(add_concepts_and_links(
            safe_get_column(row, 'primary_topic.field.id'),
            safe_get_column(row, 'primary_topic.field.display_name'),
            None, None, None, 'field', None
    ))

#   subfield
    concepts.extend(add_concepts_and_links(
        safe_get_column(row, 'primary_topic.subfield.id'),
        safe_get_column(row, 'primary_topic.subfield.display_name'),
        None, None, None, 'subfield', None
    ))

    #domain
    concepts.extend(add_concepts_and_links(
            safe_get_column(row, 'primary_topic.domain.id'),
            safe_get_column(row, 'primary_topic.domain.display_name'),
            None, None, None, 'domain', None
    ))


   
#    concepts
    concepts.extend(add_concepts_and_links(
            safe_get_column(row, 'concepts.id'),
            safe_get_column(row, 'concepts.display_name'),
            safe_get_column(row, 'concepts.score'),
            safe_get_column(row, 'concepts.wikidata'),
            safe_get_column(row, 'concepts.level'),
            'concept', None
    ))  
    return  concepts