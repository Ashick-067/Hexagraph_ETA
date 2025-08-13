import pandas as pd
import psycopg2
from psycopg2 import extras
import os
import uuid
from dotenv import load_dotenv
import os

from utils import *
from authors import *
from concepts import *
from institutions import *
from outputs import *
from grands_data import *
from sdgs_data import *
from sources import*
from output_authors import *
from output_external_ids import *
from output_institutions import*
from output_grand_data import *
from output_keywords import *
from output_sdgs_data import *
from output_yearly_counts_data import *
from output_concepts import*
from output_topics import*
from output_location import *
from output_mesh_terms import *

load_dotenv(".env")

DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'database': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'port': os.getenv("DB_PORT"),
}
# Path to your CSV file
CSV_FILE_PATH = 'OpenAlex Data.csv' # Changed to CSV

# --- UUID Namespace for deterministic hg_id generation ---
# Use a constant namespace UUID to ensure the same OpenAlex ID always gets the same hg_id
NAMESPACE_UUID = uuid.UUID('d472c674-32d8-4f2a-b6b6-3a7b9c9f2b3b') 

# --- Database Schema (Expanded based on the provided field list and EPISTEME vision) ---
# This is a more comprehensive schema.
# NOTE: For development, if you change schema and encounter errors, you might need to
# manually drop and recreate tables in your PostgreSQL database.
# The `DROP TABLE IF EXISTS ... CASCADE;` statements below will clear existing data.
# BE CAREFUL with these statements in a production environment as they will DELETE ALL DATA.
CREATE_TABLE_QUERIES = [
    """
    DROP TABLE IF EXISTS output_external_ids CASCADE;
    DROP TABLE IF EXISTS output_yearly_counts CASCADE;
    DROP TABLE IF EXISTS output_grants CASCADE;
    DROP TABLE IF EXISTS grants_data CASCADE;
    DROP TABLE IF EXISTS output_sdgs CASCADE;
    DROP TABLE IF EXISTS sdgs_data CASCADE;
    DROP TABLE IF EXISTS locations_data CASCADE;
    DROP TABLE IF EXISTS output_mesh_terms CASCADE;
    DROP TABLE IF EXISTS output_keywords CASCADE;
    DROP TABLE IF EXISTS output_topics CASCADE;
    DROP TABLE IF EXISTS output_concepts CASCADE;
    DROP TABLE IF EXISTS concepts CASCADE;
    DROP TABLE IF EXISTS output_institutions CASCADE;
    DROP TABLE IF EXISTS institutions CASCADE;
    DROP TABLE IF EXISTS output_authors CASCADE;
    DROP TABLE IF EXISTS authors CASCADE;
    DROP TABLE IF EXISTS sources CASCADE;
    DROP TABLE IF EXISTS outputs CASCADE;
    """,
    """
    CREATE TABLE IF NOT EXISTS outputs (
        hg_id TEXT PRIMARY KEY,
        openalex_id TEXT UNIQUE,
        doi TEXT UNIQUE,
        title TEXT,
        display_name TEXT,
        publication_year INTEGER,
        publication_date DATE,
        language TEXT,
        type TEXT,
        type_crossref TEXT,
        cited_by_count INTEGER,
        abstract TEXT,
        is_retracted BOOLEAN,
        is_paratext BOOLEAN,
        locations_count INTEGER,
        datasets_json JSONB,
        versions_json JSONB,
        referenced_works_count INTEGER,
        cited_by_api_url TEXT,
        is_authors_truncated BOOLEAN,
        apc_list_value NUMERIC(10, 2),
        apc_list_currency TEXT,
        apc_list_value_usd NUMERIC(10, 2),
        apc_paid_value NUMERIC(10, 2),
        apc_paid_currency TEXT,
        apc_paid_value_usd NUMERIC(10, 2),
        open_access_is_oa BOOLEAN,
        open_access_oa_status TEXT,
        open_access_oa_url TEXT,
        open_access_any_repository_has_fulltext BOOLEAN,
        citation_normalized_percentile_value NUMERIC(5, 2),
        citation_normalized_percentile_is_in_top_1_percent BOOLEAN,
        citation_normalized_percentile_is_in_top_10_percent BOOLEAN,
        cited_by_percentile_year_min INTEGER,
        cited_by_percentile_year_max INTEGER,
        biblio_volume TEXT,
        biblio_issue TEXT,
        biblio_first_page TEXT,
        biblio_last_page TEXT,
        updated_date TIMESTAMP,
        created_date TIMESTAMP,
        countries_distinct_count INTEGER,
        institutions_distinct_count INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS authors (
        hg_id TEXT PRIMARY KEY,
        openalex_id TEXT UNIQUE,
        display_name TEXT,
        orcid TEXT, -- Removed UNIQUE constraint to allow multiple NULLs
        updated_date TIMESTAMP,
        created_date TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS sources (
        hg_id TEXT PRIMARY KEY,
        openalex_id TEXT UNIQUE,
        display_name TEXT,
        issn_l TEXT, -- Removed UNIQUE constraint to allow multiple NULLs
        issn TEXT[], -- Array of ISSNs
        type TEXT,
        is_oa BOOLEAN,
        is_in_doaj BOOLEAN,
        is_indexed_in_scopus BOOLEAN,
        is_core BOOLEAN,
        host_organization TEXT,
        host_organization_name TEXT,
        host_organization_lineage TEXT[],
        host_organization_lineage_names TEXT[]
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS output_authors (
        output_id TEXT REFERENCES outputs(hg_id),
        author_id TEXT REFERENCES authors(hg_id),
        author_position TEXT,
        is_corresponding BOOLEAN,
        raw_author_name TEXT,
        raw_affiliation_strings TEXT[], -- Array of strings
        PRIMARY KEY (output_id, author_id, author_position)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS institutions (
        hg_id TEXT PRIMARY KEY,
        openalex_id TEXT UNIQUE,
        display_name TEXT,
        ror TEXT UNIQUE,
        country_code TEXT,
        type TEXT,
        lineage TEXT[] -- Array of institution IDs
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS output_institutions ( -- For author affiliations on an output
        output_id TEXT REFERENCES outputs(hg_id),
        institution_id TEXT REFERENCES institutions(hg_id),
        PRIMARY KEY (output_id, institution_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS concepts (
        hg_id TEXT PRIMARY KEY,
        openalex_id TEXT UNIQUE,
        wikidata TEXT, -- Removed UNIQUE constraint to allow multiple identical non-NULLs if needed, or multiple NULLs
        display_name TEXT,
        level INTEGER,
        score NUMERIC(5, 2), -- For concepts/topics
        type TEXT -- To distinguish primary_topic, topics, keywords, mesh
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS output_concepts ( -- For primary_topic
        output_id TEXT REFERENCES outputs(hg_id),
        concept_id TEXT REFERENCES concepts(hg_id),
        PRIMARY KEY (output_id, concept_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS output_topics ( -- For topics array
        output_id TEXT REFERENCES outputs(hg_id),
        concept_id TEXT REFERENCES concepts(hg_id),
        score NUMERIC(5, 2),
        PRIMARY KEY (output_id, concept_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS output_keywords ( -- For keywords array
        output_id TEXT REFERENCES outputs(hg_id),
        concept_id TEXT REFERENCES concepts(hg_id),
        score NUMERIC(5, 2),
        PRIMARY KEY (output_id, concept_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS output_mesh_terms ( -- For mesh array
        output_id TEXT REFERENCES outputs(hg_id),
        concept_id TEXT REFERENCES concepts(hg_id), -- hg_id generated from mesh.descriptor_ui
        descriptor_ui TEXT,
        qualifier_ui TEXT,
        is_major_topic BOOLEAN,
        PRIMARY KEY (output_id, concept_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS locations_data ( -- Details about a specific location instance (e.g., PDF link)
        id SERIAL PRIMARY KEY,
        output_id TEXT REFERENCES outputs(hg_id),
        is_oa BOOLEAN,
        landing_page_url TEXT,
        pdf_url TEXT,
        license TEXT,
        license_id TEXT,
        version TEXT,
        is_accepted BOOLEAN,
        is_published BOOLEAN,
        source_id TEXT REFERENCES sources(hg_id),
        location_type TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS sdgs_data (
        hg_id TEXT PRIMARY KEY,
        openalex_id TEXT UNIQUE,
        display_name TEXT,
        score NUMERIC(5, 2)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS output_sdgs (
        output_id TEXT REFERENCES outputs(hg_id),
        sdg_id TEXT REFERENCES sdgs_data(hg_id),
        score NUMERIC(5, 2),
        PRIMARY KEY (output_id, sdg_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS grants_data (
        hg_id TEXT PRIMARY KEY,
        award_id TEXT UNIQUE,
        funder TEXT,
        funder_display_name TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS output_grants (
        output_id TEXT REFERENCES outputs(hg_id),
        grant_id TEXT REFERENCES grants_data(hg_id),
        PRIMARY KEY (output_id, grant_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS output_yearly_counts (
        output_id TEXT REFERENCES outputs(hg_id),
        year INTEGER,
        cited_by_count INTEGER,
        PRIMARY KEY (output_id, year)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS output_external_ids (
        output_id TEXT REFERENCES outputs(hg_id),
        id_type TEXT,
        external_id TEXT,
        PRIMARY KEY (output_id, id_type)
    );
    """
]

# Define which columns in each table are array types.
# This is used for converting tuples back to lists before insertion.
ARRAY_COLUMNS = {
    'sources': ['issn', 'host_organization_lineage', 'host_organization_lineage_names'],
    'output_authors': ['raw_affiliation_strings'],
    'institutions': ['lineage']
}

# --- Helper Functions ---

# def generate_hg_id(openalex_id_string):
#     """Generates a deterministic UUID (hg_id) from an OpenAlex ID string."""
#     if not openalex_id_string:
#         return None
#     s_openalex_id_string = str(openalex_id_string).strip()
#     return str(uuid.uuid5(NAMESPACE_UUID, s_openalex_id_string))

def create_tables(cursor, conn):
    """Creates necessary tables in the PostgreSQL database if they don't exist."""
    print("Checking and creating tables...")
    for query in CREATE_TABLE_QUERIES:
        try:
            # Execute each query separately
            cursor.execute(query)
            # Only print feedback for non-empty queries for clarity
            if query.strip():
                print(f"Executed table creation query for: {query.splitlines()[0].strip()}")
        except Exception as e:
            print(f"Error creating table with query: {query.splitlines()[0].strip()}: {e}")
    conn.commit()
    print("All table creation queries executed and committed.")

# def safe_get_column(row, column_name, default=None):
#     """Safely retrieves a column value from a pandas Series (row), handling NaNs and missing columns."""
#     if column_name in row.index and pd.notna(row[column_name]):
#         return row[column_name]
#     return default

# def safe_parse_int(value):
#     """Safely parses a value to an integer, returning None on failure."""
#     if pd.isna(value) or str(value).strip() == '':
#         return None
#     try:
#         return int(float(str(value).strip()))
#     except (ValueError, TypeError):
#         return None

# def safe_parse_numeric(value):
#     """Safely parses a value to a numeric (float), returning None on failure. Handles lists by taking first element."""
#     if pd.isna(value) or str(value).strip() == '':
#         return None
    
#     if isinstance(value, list) and value:
#         value = value[0]

#     try:
#         return float(str(value).strip())
#     except (ValueError, TypeError):
#         return None

# def safe_parse_boolean(value):
#     """Safely parses a value to a boolean, returning None for empty strings or unparseable values."""
#     if pd.isna(value) or str(value).strip() == '':
#         return None
#     s_value = str(value).strip().lower()
#     if s_value == 'true':
#         return True
#     elif s_value == 'false':
#         return False
#     return None # For anything else, return None (which maps to SQL NULL)


# def parse_date(date_val):
#     """Parses a date value from Excel/CSV (DD-MM-YYYY orYYYY-MM-DD) or datetime object."""
#     if pd.isna(date_val):
#         return None
#     if isinstance(date_val, datetime):
#         return date_val.date()
#     s_date_val = str(date_val).strip()
#     try:
#         return datetime.strptime(s_date_val, '%d-%m-%Y').date()
#     except ValueError:
#         try:
#             return datetime.strptime(s_date_val, '%Y-%m-%d').date()
#         except ValueError:
#             return None

# def parse_timestamp(ts_val):
#     """Parses a timestamp value from Excel/CSV (ISO format) or datetime object."""
#     if pd.isna(ts_val):
#         return None
#     if isinstance(ts_val, datetime):
#         return ts_val
#     s_ts_val = str(ts_val).strip()
#     try:
#         return datetime.fromisoformat(s_ts_val.replace('Z', '+00:00'))
#     except ValueError:
#         return None

# def parse_jsonb_string(json_string):
#     """Parses a string that might contain a JSON object or array."""
#     if pd.isna(json_string) or str(json_string).strip() == '':
#         return None
#     try:
#         return json.loads(str(json_string))
#     except (json.JSONDecodeError, TypeError):
#         return None

# def parse_list_string(list_string, delimiter='|'):
#     """
#     Parses a string representation of a list.
#     Tries JSON list, then pipe-separated, then comma-separated.
#     """
#     if pd.isna(list_string) or str(list_string).strip() == '':
#         return []
#     s_list_string = str(list_string).strip()

#     # 1. Try to parse as JSON list
#     try:
#         parsed = json.loads(s_list_string)
#         if isinstance(parsed, list):
#             return [str(item).strip() for item in parsed if pd.notna(item)]
#     except (json.JSONDecodeError, TypeError):
#         pass

#     # 2. If not JSON, try splitting by the specified delimiter (default '|')
#     if delimiter in s_list_string:
#         return [item.strip() for item in s_list_string.split(delimiter) if item.strip()]
    
#     # 3. Fallback to comma-separated if no pipe
#     if ',' in s_list_string:
#         return [item.strip() for item in s_list_string.split(',') if item.strip()]

#     return [s_list_string] if s_list_string else []

# def make_hashable_for_set(obj):
#     """
#     Recursively converts mutable objects (dicts, lists) into immutable ones (frozensets, tuples)
#     to allow the top-level object (usually a dict) to be hashed for set operations.
#     """
#     if isinstance(obj, dict):
#         return frozenset((k, make_hashable_for_set(v)) for k, v in sorted(obj.items()))
#     elif isinstance(obj, list):
#         return tuple(make_hashable_for_set(elem) for elem in obj)
#     else:
#         return obj # Base case: immutable type (e.g., string, int, float, None)

# def process_excel_row(row):
#     """
#     Processes a single row from the CSV DataFrame and extracts data
#     for various EPISTEME tables.
#     """
#     output_data = {}
#     authors_data_from_row = [] 
#     sources_data_from_row = [] 
#     institutions_data_from_row = [] 
#     concepts_data_from_row = [] 
#     sdgs_data_from_row = [] 
#     grants_data_from_row = [] 

#     output_authors_data_from_row = []
#     output_institutions_data_from_row = []
#     output_concepts_data_from_row = []
#     output_topics_data_from_row = []
#     output_keywords_data_from_row = []
#     output_mesh_terms_data_from_row = []
#     locations_records_from_row = []
#     output_sdgs_data_from_row = []
#     output_grants_data_from_row = []
#     output_yearly_counts_data_from_row = []
#     output_external_ids_data_from_row = []

#     openalex_output_id = safe_get_column(row, 'id')
#     if not openalex_output_id:
#         return (output_data, authors_data_from_row, sources_data_from_row, output_authors_data_from_row,
#                 institutions_data_from_row, output_institutions_data_from_row, concepts_data_from_row,
#                 output_concepts_data_from_row, output_topics_data_from_row, output_keywords_data_from_row,
#                 output_mesh_terms_data_from_row, locations_records_from_row, sdgs_data_from_row, output_sdgs_data_from_row,
#                 grants_data_from_row, output_grants_data_from_row, output_yearly_counts_data_from_row, output_external_ids_data_from_row)

#     output_hg_id = generate_hg_id(openalex_output_id)

#     # --- Transform Outputs Data ---
#     output_data = {
#         'hg_id': output_hg_id,
#         'openalex_id': openalex_output_id,
#         'doi': safe_get_column(row, 'doi', '').replace('https://doi.org/', '') if safe_get_column(row, 'doi') else None,
#         'title': safe_get_column(row, 'title'),
#         'display_name': safe_get_column(row, 'display_name'),
#         'publication_year': safe_parse_int(safe_get_column(row, 'publication_year')),
#         'publication_date': parse_date(safe_get_column(row, 'publication_date')),
#         'language': safe_get_column(row, 'language'),
#         'type': safe_get_column(row, 'type'),
#         'type_crossref': safe_get_column(row, 'type_crossref'),
#         'cited_by_count': safe_parse_int(safe_get_column(row, 'cited_by_count')),
#         'abstract': safe_get_column(row, 'abstract'),
#         'is_retracted': safe_parse_boolean(safe_get_column(row, 'is_retracted')), # Apply safe_parse_boolean
#         'is_paratext': safe_parse_boolean(safe_get_column(row, 'is_paratext')),   # Apply safe_parse_boolean
#         'locations_count': safe_parse_int(safe_get_column(row, 'locations_count')),
#         'datasets_json': parse_jsonb_string(safe_get_column(row, 'datasets')),
#         'versions_json': parse_jsonb_string(safe_get_column(row, 'versions')),
#         'referenced_works_count': safe_parse_int(safe_get_column(row, 'referenced_works_count')),
#         'cited_by_api_url': safe_get_column(row, 'cited_by_api_url'),
#         'is_authors_truncated': safe_parse_boolean(safe_get_column(row, 'is_authors_truncated')), # Apply safe_parse_boolean
#         'apc_list_value': safe_parse_numeric(safe_get_column(row, 'apc_list.value')),
#         'apc_list_currency': safe_get_column(row, 'apc_list.currency'),
#         'apc_list_value_usd': safe_parse_numeric(safe_get_column(row, 'apc_list.value_usd')),
#         'apc_paid_value': safe_parse_numeric(safe_get_column(row, 'apc_paid.value')),
#         'apc_paid_currency': safe_get_column(row, 'apc_paid.currency'),
#         'apc_paid_value_usd': safe_parse_numeric(safe_get_column(row, 'apc_paid.value_usd')),
#         'open_access_is_oa': safe_parse_boolean(safe_get_column(row, 'open_access.is_oa')), # Apply safe_parse_boolean
#         'open_access_oa_status': safe_get_column(row, 'open_access.oa_status'),
#         'open_access_oa_url': safe_get_column(row, 'open_access.oa_url'),
#         'open_access_any_repository_has_fulltext': safe_parse_boolean(safe_get_column(row, 'open_access.any_repository_has_fulltext')), # Apply safe_parse_boolean
#         'citation_normalized_percentile_value': safe_parse_numeric(safe_get_column(row, 'citation_normalized_percentile.value')),
#         'citation_normalized_percentile_is_in_top_1_percent': safe_parse_boolean(safe_get_column(row, 'citation_normalized_percentile.is_in_top_1_percent')), # Apply safe_parse_boolean
#         'citation_normalized_percentile_is_in_top_10_percent': safe_parse_boolean(safe_get_column(row, 'citation_normalized_percentile.is_in_top_10_percent')), # Apply safe_parse_boolean
#         'cited_by_percentile_year_min': safe_parse_int(safe_get_column(row, 'cited_by_percentile_year.min')),
#         'cited_by_percentile_year_max': safe_parse_int(safe_get_column(row, 'cited_by_percentile_year.max')),
#         'biblio_volume': safe_get_column(row, 'biblio.volume'),
#         'biblio_issue': safe_get_column(row, 'biblio.issue'),
#         'biblio_first_page': safe_get_column(row, 'biblio.first_page'),
#         'biblio_last_page': safe_get_column(row, 'biblio.last_page'),
#         'updated_date': parse_timestamp(safe_get_column(row, 'updated_date')),
#         'created_date': parse_date(safe_get_column(row, 'created_date')),
#         'countries_distinct_count': safe_parse_int(safe_get_column(row, 'countries_distinct_count')),
#         'institutions_distinct_count': safe_parse_int(safe_get_column(row, 'institutions_distinct_count'))
#     }

#     # --- Output External IDs ---
#     output_external_ids_data_from_row = []
#     if safe_get_column(row, 'ids.doi'):
#         output_external_ids_data_from_row.append({'output_id': output_hg_id, 'id_type': 'doi', 'external_id': str(safe_get_column(row, 'ids.doi')).replace('https://doi.org/', '')})
#     if safe_get_column(row, 'ids.mag'):
#         output_external_ids_data_from_row.append({'output_id': output_hg_id, 'id_type': 'mag', 'external_id': str(safe_get_column(row, 'ids.mag'))})
#     if safe_get_column(row, 'ids.pmid'):
#         output_external_ids_data_from_row.append({'output_id': output_hg_id, 'id_type': 'pmid', 'external_id': str(safe_get_column(row, 'ids.pmid'))})
#     if safe_get_column(row, 'ids.pmcid'):
#         output_external_ids_data_from_row.append({'output_id': output_hg_id, 'id_type': 'pmcid', 'external_id': str(safe_get_column(row, 'ids.pmcid'))})

#     # --- Authors and Output_Authors Data ---
#     author_ids = parse_list_string(safe_get_column(row, 'authorships.author.id'))
#     author_display_names = parse_list_string(safe_get_column(row, 'authorships.author.display_name'))
#     author_orcids = parse_list_string(safe_get_column(row, 'authorships.author.orcid'))
#     author_positions = parse_list_string(safe_get_column(row, 'authorships.author_position'))
#     is_correspondings = parse_list_string(safe_get_column(row, 'authorships.is_corresponding'))
#     raw_author_names = parse_list_string(safe_get_column(row, 'authorships.raw_author_name'))
#     authors_updated_dates = parse_timestamp(safe_get_column(row, 'updated_date'))
#     authors_created_dates = parse_date(safe_get_column(row, 'created_date'))
    
#     raw_affiliation_strings_raw_list = parse_list_string(safe_get_column(row, 'authorships.raw_affiliation_strings'))
#     raw_affiliation_strings_processed = []
#     for aff_str_item in raw_affiliation_strings_raw_list:
#         parsed_aff_json = parse_jsonb_string(aff_str_item)
#         if isinstance(parsed_aff_json, list):
#             raw_affiliation_strings_processed.append(parsed_aff_json)
#         else:
#             # Ensure that even if parsed_aff_json is a dict, it's wrapped in a list for TEXT[]
#             raw_affiliation_strings_processed.append([aff_str_item])
    
#     # Process authorships, but defer institution collection to a dedicated section
#     for i in range(len(author_ids)):
#         author_openalex_id = author_ids[i] if i < len(author_ids) else None
#         if not author_openalex_id:
#             continue

#         author_hg_id = generate_hg_id(author_openalex_id)
        
#         authors_data_from_row.append({
#             'hg_id': author_hg_id,
#             'openalex_id': author_openalex_id,
#             'display_name': author_display_names[i] if i < len(author_display_names) else None,
#             'orcid': author_orcids[i] if i < len(author_orcids) else None,
#             'updated_date': authors_updated_dates,
#             'created_date': authors_created_dates
#         })

#         output_authors_data_from_row.append({
#             'output_id': output_hg_id,
#             'author_id': author_hg_id,
#             'author_position': author_positions[i] if i < len(author_positions) else None,
#             'is_corresponding': safe_parse_boolean(is_correspondings[i]) if i < len(is_correspondings) else False, # Apply safe_parse_boolean
#             'raw_author_name': raw_author_names[i] if i < len(raw_author_names) else None,
#             'raw_affiliation_strings': raw_affiliation_strings_processed[i] if i < len(raw_affiliation_strings_processed) else []
#         })

#     # --- Process Institutions Data (from authorships.affiliations and corresponding_institution_ids) ---
    
    # # 1. From authorships.affiliations
    # authorships_affiliations_raw = safe_get_column(row, 'authorships.institutions')
    # parsed_affiliations_list = parse_list_string(authorships_affiliations_raw) 

    # for aff_json_str in parsed_affiliations_list:
    #     parsed_aff_obj = parse_jsonb_string(aff_json_str.replace("'", "\""))
        
    #     if parsed_aff_obj and isinstance(parsed_aff_obj, dict):
    #         # Prioritize direct institution details if present
    #         inst_openalex_id = parsed_aff_obj.get('id')
    #         inst_display_name = parsed_aff_obj.get('display_name')
    #         inst_ror = parsed_aff_obj.get('ror')
    #         inst_country_code = parsed_aff_obj.get('country_code')
    #         inst_type = parsed_aff_obj.get('type')
            
    #         # Correctly handle lineage which might be a list or a string needing parsing
    #         inst_lineage_raw = parsed_aff_obj.get('lineage')
    #         if isinstance(inst_lineage_raw, list):
    #             inst_lineage = [str(item).strip() for item in inst_lineage_raw if pd.notna(item)]
    #         elif isinstance(inst_lineage_raw, str):
    #             inst_lineage = parse_list_string(inst_lineage_raw)
    #         else:
    #             inst_lineage = [] # Default if not found or invalid type


    #         if inst_openalex_id:
    #             inst_hg_id = generate_hg_id(inst_openalex_id)
    #             institutions_data_from_row.append({
    #                 'hg_id': inst_hg_id,
    #                 'openalex_id': inst_openalex_id,
    #                 'display_name': inst_display_name,
    #                 'ror': inst_ror,
    #                 'country_code': inst_country_code,
    #                 'type': inst_type,
    #                 'lineage': inst_lineage
    #             })
    #             output_institutions_data_from_row.append({
    #                 'output_id': output_hg_id,
    #                 'institution_id': inst_hg_id
    #             })
            
    #         # Fallback for older or different format where only 'institution_ids' is nested
    #         # This handles cases where raw_affiliation_string might be present and institution_ids is nested
    #         elif 'institution_ids' in parsed_aff_obj and isinstance(parsed_aff_obj.get('institution_ids'), list):
    #             for nested_inst_id in parsed_aff_obj['institution_ids']:
    #                 if nested_inst_id:
    #                     nested_inst_hg_id = generate_hg_id(nested_inst_id)
    #                     institutions_data_from_row.append({
    #                         'hg_id': nested_inst_hg_id,
    #                         'openalex_id': nested_inst_id,
    #                         'display_name': None, # Not available at this level
    #                         'ror': None,
    #                         'country_code': None,
    #                         'type': None,
    #                         'lineage': []
    #                     })
    #                     output_institutions_data_from_row.append({
    #                         'output_id': output_hg_id,
    #                         'institution_id': nested_inst_hg_id
    #                     })
                        
    # # 2. From corresponding_institution_ids (if it exists and is a direct OpenAlex ID)
    # corresponding_inst_ids_raw = safe_get_column(row, 'corresponding_institution_ids')
    # corresponding_inst_ids = parse_list_string(corresponding_inst_ids_raw) # Assuming it's a pipe-separated list of IDs

    # for inst_openalex_id in corresponding_inst_ids:
    #     if inst_openalex_id:
    #         inst_hg_id = generate_hg_id(inst_openalex_id)
    #         institutions_data_from_row.append({
    #             'hg_id': inst_hg_id,
    #             'openalex_id': inst_openalex_id,
    #             'display_name': None, # Placeholder, as this column only contains the ID
    #             'ror': None,
    #             'country_code': None,
    #             'type': None,
    #             'lineage': []
    #         })
    #         output_institutions_data_from_row.append({
    #             'output_id': output_hg_id,
    #             'institution_id': inst_hg_id
    #         })

#     # --- Transform Sources and Locations Data ---
#     loc_is_oas = parse_list_string(safe_get_column(row, 'locations.is_oa'), delimiter='|')
#     loc_landing_page_urls = parse_list_string(safe_get_column(row, 'locations.landing_page_url'), delimiter='|')
#     loc_pdf_urls = parse_list_string(safe_get_column(row, 'locations.pdf_url'), delimiter='|')
#     loc_licenses = parse_list_string(safe_get_column(row, 'locations.license'), delimiter='|')
#     loc_license_ids = parse_list_string(safe_get_column(row, 'locations.license_id'), delimiter='|')
#     loc_versions = parse_list_string(safe_get_column(row, 'locations.version'), delimiter='|')
#     loc_is_accepteds = parse_list_string(safe_get_column(row, 'locations.is_accepted'), delimiter='|')
#     loc_is_publisheds = parse_list_string(safe_get_column(row, 'locations.is_published'), delimiter='|')
#     loc_source_ids = parse_list_string(safe_get_column(row, 'locations.source.id'), delimiter='|')
#     loc_source_display_names = parse_list_string(safe_get_column(row, 'locations.source.display_name'), delimiter='|')
#     loc_source_issn_ls = parse_list_string(safe_get_column(row, 'locations.source.issn_l'), delimiter='|')
#     loc_source_issns = parse_list_string(safe_get_column(row, 'locations.source.issn'), delimiter='|')
#     loc_source_is_oas = parse_list_string(safe_get_column(row, 'locations.source.is_oa'), delimiter='|')
#     loc_source_is_in_doajs = parse_list_string(safe_get_column(row, 'locations.source.is_in_doaj'), delimiter='|')
#     loc_source_is_indexed_in_scopus = parse_list_string(safe_get_column(row, 'locations.source.is_indexed_in_scopus'), delimiter='|')
#     loc_source_is_cores = parse_list_string(safe_get_column(row, 'locations.source.is_core'), delimiter='|')
#     loc_source_host_organizations = parse_list_string(safe_get_column(row, 'locations.source.host_organization'), delimiter='|')
#     loc_source_host_organization_names = parse_list_string(safe_get_column(row, 'locations.source.host_organization_name'), delimiter='|')
#     loc_source_host_organization_lineages = parse_list_string(safe_get_column(row, 'locations.source.host_organization_lineage'), delimiter='|')
#     loc_source_host_organization_lineage_names = parse_list_string(safe_get_column(row, 'locations.source.host_organization_lineage_names'), delimiter='|')
#     loc_source_types = parse_list_string(safe_get_column(row, 'locations.source.type'), delimiter='|')

#     max_loc_len = max(len(loc_is_oas), len(loc_landing_page_urls), len(loc_pdf_urls), len(loc_source_ids))

#     for i in range(max_loc_len):
#         source_hg_id = None
#         openalex_source_id = loc_source_ids[i] if i < len(loc_source_ids) else None

#         if openalex_source_id:
#             source_hg_id = generate_hg_id(openalex_source_id)
#             sources_data_from_row.append({
#                 'hg_id': source_hg_id,
#                 'openalex_id': openalex_source_id,
#                 'display_name': loc_source_display_names[i] if i < len(loc_source_display_names) else None,
#                 'issn_l': loc_source_issn_ls[i] if i < len(loc_source_issn_ls) else None,
#                 'issn': parse_list_string(loc_source_issns[i], delimiter=';') if i < len(loc_source_issns) else [],
#                 'type': loc_source_types[i] if i < len(loc_source_types) else None,
#                 'is_oa': safe_parse_boolean(loc_source_is_oas[i]) if i < len(loc_source_is_oas) else None, # Apply safe_parse_boolean
#                 'is_in_doaj': safe_parse_boolean(loc_source_is_in_doajs[i]) if i < len(loc_source_is_in_doajs) else None, # Apply safe_parse_boolean
#                 'is_indexed_in_scopus': safe_parse_boolean(loc_source_is_indexed_in_scopus[i]) if i < len(loc_source_is_indexed_in_scopus) else None, # Apply safe_parse_boolean
#                 'is_core': safe_parse_boolean(loc_source_is_cores[i]) if i < len(loc_source_is_cores) else None, # Apply safe_parse_boolean
#                 'host_organization': loc_source_host_organizations[i] if i < len(loc_source_host_organizations) else None,
#                 'host_organization_name': loc_source_host_organization_names[i] if i < len(loc_source_host_organization_names) else None,
#                 'host_organization_lineage': parse_list_string(loc_source_host_organization_lineages[i]) if i < len(loc_source_host_organization_lineages) else [],
#                 'host_organization_lineage_names': parse_list_string(loc_source_host_organization_lineage_names[i]) if i < len(loc_source_host_organization_lineages) else []
#             })
        
#         landing_page = loc_landing_page_urls[i] if i < len(loc_landing_page_urls) else None
#         pdf_url = loc_pdf_urls[i] if i < len(loc_pdf_urls) else None

#         if landing_page or pdf_url:
#             location_record = {
#                 'output_id': output_hg_id,
#                 'is_oa': safe_parse_boolean(loc_is_oas[i]) if i < len(loc_is_oas) else None, # Apply safe_parse_boolean
#                 'landing_page_url': landing_page,
#                 'pdf_url': pdf_url,
#                 'license': loc_licenses[i] if i < len(loc_licenses) else None,
#                 'license_id': loc_license_ids[i] if i < len(loc_license_ids) else None,
#                 'version': loc_versions[i] if i < len(loc_versions) else None,
#                 'is_accepted': safe_parse_boolean(loc_is_accepteds[i]) if i < len(loc_is_accepteds) else None, # Apply safe_parse_boolean
#                 'is_published': safe_parse_boolean(loc_is_publisheds[i]) if i < len(loc_is_publisheds) else None, # Apply safe_parse_boolean
#                 'source_id': source_hg_id,
#                 'location_type': 'other'
#             }
#             locations_records_from_row.append(location_record)

#     openalex_primary_source_id = safe_get_column(row, 'primary_location.source.id')
#     primary_source_hg_id = None
#     if openalex_primary_source_id:
#         primary_source_hg_id = generate_hg_id(openalex_primary_source_id)
#         sources_data_from_row.append({
#             'hg_id': primary_source_hg_id,
#             'openalex_id': openalex_primary_source_id,
#             'display_name': safe_get_column(row, 'primary_location.source.display_name'),
#             'issn_l': safe_get_column(row, 'primary_location.source.issn_l'),
#             'issn': parse_list_string(safe_get_column(row, 'primary_location.source.issn'), delimiter=';'),
#             'type': safe_get_column(row, 'primary_location.source.type'),
#             'is_oa': safe_parse_boolean(safe_get_column(row, 'primary_location.source.is_oa')), # Apply safe_parse_boolean
#             'is_in_doaj': safe_parse_boolean(safe_get_column(row, 'primary_location.source.is_in_doaj')), # Apply safe_parse_boolean
#             'is_indexed_in_scopus': safe_parse_boolean(safe_get_column(row, 'primary_location.source.is_indexed_in_scopus')), # Apply safe_parse_boolean
#             'is_core': safe_parse_boolean(safe_get_column(row, 'primary_location.source.is_core')), # Apply safe_parse_boolean
#             'host_organization': safe_get_column(row, 'primary_location.source.host_organization'),
#             'host_organization_name': safe_get_column(row, 'primary_location.source.host_organization_name'),
#             'host_organization_lineage': parse_list_string(safe_get_column(row, 'primary_location.source.host_organization_lineage')),
#             'host_organization_lineage_names': parse_list_string(safe_get_column(row, 'primary_location.source.host_organization_lineage_names'))
#         })
#     primary_location_record = {
#         'output_id': output_hg_id,
#         'is_oa': safe_parse_boolean(safe_get_column(row, 'primary_location.is_oa')), # Apply safe_parse_boolean
#         'landing_page_url': safe_get_column(row, 'primary_location.landing_page_url'),
#         'pdf_url': safe_get_column(row, 'primary_location.pdf_url'),
#         'license': safe_get_column(row, 'primary_location.license'),
#         'license_id': safe_get_column(row, 'primary_location.license_id'),
#         'version': safe_get_column(row, 'primary_location.version'),
#         'is_accepted': safe_parse_boolean(safe_get_column(row, 'primary_location.is_accepted')), # Apply safe_parse_boolean
#         'is_published': safe_parse_boolean(safe_get_column(row, 'primary_location.is_published')), # Apply safe_parse_boolean
#         'source_id': primary_source_hg_id,
#         'location_type': 'primary'
#     }
#     if primary_location_record['landing_page_url'] or primary_location_record['pdf_url']:
#         locations_records_from_row.append(primary_location_record)

#     openalex_best_oa_source_id = safe_get_column(row, 'best_oa_location.source.id')
#     best_oa_source_hg_id = None
#     if openalex_best_oa_source_id:
#         best_oa_source_hg_id = generate_hg_id(openalex_best_oa_source_id)
#         sources_data_from_row.append({
#             'hg_id': best_oa_source_hg_id,
#             'openalex_id': openalex_best_oa_source_id,
#             'display_name': safe_get_column(row, 'best_oa_location.source.display_name'),
#             'issn_l': safe_get_column(row, 'best_oa_location.source.issn_l'),
#             'issn': parse_list_string(safe_get_column(row, 'best_oa_location.source.issn'), delimiter=';'),
#             'type': safe_get_column(row, 'best_oa_location.source.type'),
#             'is_oa': safe_parse_boolean(safe_get_column(row, 'best_oa_location.source.is_oa')), # Apply safe_parse_boolean
#             'is_in_doaj': safe_parse_boolean(safe_get_column(row, 'best_oa_location.source.is_in_doaj')), # Apply safe_parse_boolean
#             'is_indexed_in_scopus': safe_parse_boolean(safe_get_column(row, 'best_oa_location.source.is_indexed_in_scopus')), # Apply safe_parse_boolean
#             'is_core': safe_parse_boolean(safe_get_column(row, 'best_oa_location.source.is_core')), # Apply safe_parse_boolean
#             'host_organization': safe_get_column(row, 'best_oa_location.source.host_organization'),
#             'host_organization_name': safe_get_column(row, 'best_oa_location.source.host_organization_name'),
#             'host_organization_lineage': parse_list_string(safe_get_column(row, 'best_oa_location.source.host_organization_lineage')),
#             'host_organization_lineage_names': parse_list_string(safe_get_column(row, 'best_oa_location.source.host_organization_lineage_names'))
#         })
#     best_oa_location_record = {
#         'output_id': output_hg_id,
#         'is_oa': safe_parse_boolean(safe_get_column(row, 'best_oa_location.is_oa')), # Apply safe_parse_boolean
#         'landing_page_url': safe_get_column(row, 'best_oa_location.landing_page_url'),
#         'pdf_url': safe_get_column(row, 'best_oa_location.pdf_url'),
#         'license': safe_get_column(row, 'best_oa_location.license'),
#         'license_id': safe_get_column(row, 'best_oa_location.license_id'),
#         'version': safe_get_column(row, 'best_oa_location.version'),
#         'is_accepted': safe_parse_boolean(loc_is_accepteds[i]) if i < len(loc_is_accepteds) else None, # Apply safe_parse_boolean
#         'is_published': safe_parse_boolean(loc_is_publisheds[i]) if i < len(loc_is_publisheds) else None, # Apply safe_parse_boolean
#         'source_id': best_oa_source_hg_id,
#         'location_type': 'best_oa'
#     }
#     if best_oa_location_record['landing_page_url'] or best_oa_location_record['pdf_url']:
#         locations_records_from_row.append(best_oa_location_record)


#     # --- Transform Concepts (Primary Topic, Topics, Keywords, Mesh) ---
    # def add_concepts_and_links(
    #     concept_ids_str, display_names_str, scores_str, wikidatas_str, levels_str, concept_type, 
    #     target_output_join_list=None,
    #     join_has_score=False,
    #     descriptor_uis_str=None, qualifier_uis_str=None, is_major_topics_str=None
    # ):
    #     concept_ids = parse_list_string(concept_ids_str)
    #     display_names = parse_list_string(display_names_str)
    #     scores_list_raw = parse_list_string(scores_str)
    #     scores = [safe_parse_numeric(s) for s in scores_list_raw]
        
    #     wikidatas = parse_list_string(wikidatas_str) if wikidatas_str else []
    #     levels = [safe_parse_int(l) for l in parse_list_string(levels_str)] if levels_str else []
        
    #     descriptor_uis = parse_list_string(descriptor_uis_str) if descriptor_uis_str else []
    #     qualifier_uis = parse_list_string(qualifier_uis_str) if qualifier_uis_str else []
    #     is_major_topics = [safe_parse_boolean(b) for b in parse_list_string(is_major_topics_str)] if is_major_topics_str else [] # Apply safe_parse_boolean


    #     max_len = len(concept_ids)
    #     if concept_type == 'mesh':
    #         max_len = max(max_len, len(descriptor_uis))

    #     for i in range(max_len):
    #         openalex_concept_id = concept_ids[i] if i < len(concept_ids) else None
    #         concept_display_name = display_names[i] if i < len(display_names) else None
    #         concept_score = scores[i] if i < len(scores) else None
    #         concept_wikidata = wikidatas[i] if i < len(wikidatas) else None
    #         concept_level = levels[i] if i < len(levels) else None

    #         if concept_type == 'mesh':
    #             mesh_descriptor_ui = descriptor_uis[i] if i < len(descriptor_uis) else None
    #             if not mesh_descriptor_ui: continue
    #             openalex_concept_id = f"mesh:{mesh_descriptor_ui}"
    #             concept_display_name = concept_display_name if concept_display_name else None
                
    #         if not openalex_concept_id and not concept_display_name:
    #             continue
            
    #         concept_hg_id = generate_hg_id(openalex_concept_id or concept_display_name)

    #         concept_record = {
    #             'hg_id': concept_hg_id,
    #             'openalex_id': openalex_concept_id,
    #             'wikidata': concept_wikidata,
    #             'display_name': concept_display_name,
    #             'level': concept_level,
    #             'score': concept_score,
    #             'type': concept_type
    #         }
    #         concepts_data_from_row.append(concept_record)

    #         if target_output_join_list is not None:
    #             join_record = {'output_id': output_hg_id, 'concept_id': concept_hg_id}
    #             if join_has_score:
    #                 join_record['score'] = concept_score
    #             if concept_type == 'mesh':
    #                 join_record['descriptor_ui'] = mesh_descriptor_ui
    #                 join_record['qualifier_ui'] = qualifier_uis[i] if i < len(qualifier_uis) else None
    #                 join_record['is_major_topic'] = safe_parse_boolean(is_major_topics[i]) if i < len(is_major_topics) else None # Apply safe_parse_boolean
    #             target_output_join_list.append(join_record)


#     # Primary Topic and its hierarchy
#     add_concepts_and_links(
#         safe_get_column(row, 'primary_topic.id'),
#         safe_get_column(row, 'primary_topic.display_name'),
#         safe_get_column(row, 'primary_topic.score'),
#         safe_get_column(row, 'primary_topic.wikidata'),
#         safe_get_column(row, 'primary_topic.level'),
#         'primary_topic', output_concepts_data_from_row, join_has_score=False
#     )
#     add_concepts_and_links(
#         safe_get_column(row, 'primary_topic.subfield.id'),
#         safe_get_column(row, 'primary_topic.subfield.display_name'),
#         None, None, None, 'subfield', None
#     )
#     add_concepts_and_links(
#         safe_get_column(row, 'primary_topic.field.id'),
#         safe_get_column(row, 'primary_topic.field.display_name'),
#         None, None, None, 'field', None
#     )
#     add_concepts_and_links(
#         safe_get_column(row, 'primary_topic.domain.id'),
#         safe_get_column(row, 'primary_topic.domain.display_name'),
#         None, None, None, 'domain', None
#     )

    # Topics array
    # add_concepts_and_links(
    #     safe_get_column(row, 'topics.id'),
    #     safe_get_column(row, 'topics.display_name'),
    #     safe_get_column(row, 'topics.score'),
    #     None, safe_get_column(row, 'topics.level'),
    #     'topic', output_topics_data_from_row, join_has_score=True
    # )

#     # Keywords array
#     add_concepts_and_links(
#         safe_get_column(row, 'keywords.id'),
#         safe_get_column(row, 'keywords.display_name'),
#         safe_get_column(row, 'keywords.score'),
#         None, None, 'keyword', output_keywords_data_from_row, join_has_score=True
#     )
    
#     # Concepts (general)
#     add_concepts_and_links(
#         safe_get_column(row, 'concepts.id'),
#         safe_get_column(row, 'concepts.display_name'),
#         safe_get_column(row, 'concepts.score'),
#         safe_get_column(row, 'concepts.wikidata'),
#         safe_get_column(row, 'concepts.level'),
#         'concept', None
#     )

#     # Mesh terms
#     add_concepts_and_links(
#         None,
#         safe_get_column(row, 'mesh.descriptor_name'),
#         None, None, None,
#         'mesh', output_mesh_terms_data_from_row, join_has_score=False,
#         descriptor_uis_str=safe_get_column(row, 'mesh.descriptor_ui'),
#         qualifier_uis_str=safe_get_column(row, 'mesh.qualifier_ui'),
#         is_major_topics_str=safe_get_column(row, 'mesh.is_major_topic')
#     )

#     # --- Transform Sustainable Development Goals (SDGs) ---
#     openalex_sdg_ids = parse_list_string(safe_get_column(row, 'sustainable_development_goals.id'))
#     sdg_display_names = parse_list_string(safe_get_column(row, 'sustainable_development_goals.display_name'))
#     sdg_scores = [safe_parse_numeric(s) for s in parse_list_string(safe_get_column(row, 'sustainable_development_goals.score'))]

#     for i in range(len(openalex_sdg_ids)):
#         openalex_sdg_id = openalex_sdg_ids[i] if i < len(openalex_sdg_ids) else None
#         if not openalex_sdg_id:
#             continue
#         sdg_hg_id = generate_hg_id(openalex_sdg_id)
#         current_sdg_score = sdg_scores[i] if i < len(sdg_scores) else None

#         sdgs_data_from_row.append({
#             'hg_id': sdg_hg_id,
#             'openalex_id': openalex_sdg_id,
#             'display_name': sdg_display_names[i] if i < len(sdg_display_names) else None,
#             'score': current_sdg_score
#         })
#         output_sdgs_data_from_row.append({
#             'output_id': output_hg_id,
#             'sdg_id': sdg_hg_id,
#             'score': current_sdg_score
#         })

#     # --- Transform Grants Data ---
    # grant_award_ids = parse_list_string(safe_get_column(row, 'grants.award_id'))
    # grant_funders = parse_list_string(safe_get_column(row, 'grants.funder'))
    # grant_funder_display_names = parse_list_string(safe_get_column(row, 'grants.funder_display_name'))

    # for i in range(len(grant_award_ids)):
    #     grant_award_id = grant_award_ids[i] if i < len(grant_award_ids) else None
    #     if not grant_award_id:
    #         continue
    #     grant_hg_id = generate_hg_id(grant_award_id)
    #     grants_data_from_row.append({
    #         'hg_id': grant_hg_id,
    #         'award_id': grant_award_id,
    #         'funder': grant_funders[i] if i < len(grant_funders) else None,
    #         'funder_display_name': grant_funder_display_names[i] if i < len(grant_funder_display_names) else None
    #     })
    #     output_grants_data_from_row.append({
    #         'output_id': output_hg_id,
    #         'grant_id': grant_hg_id
    #     })
    
#     # --- Transform Counts By Year ---
    # years_str = safe_get_column(row, 'counts_by_year.year')
    # cited_by_counts_str = safe_get_column(row, 'counts_by_year.cited_by_count')

    # years = parse_list_string(years_str)
    # cited_by_counts = [safe_parse_int(c) for c in parse_list_string(cited_by_counts_str)]

    # for i in range(len(years)):
    #     year = years[i] if i < len(years) else None
    #     cited_count = cited_by_counts[i] if i < len(cited_by_counts) else None

    #     if year is not None and cited_count is not None:
    #         output_yearly_counts_data_from_row.append({
    #             'output_id': output_hg_id,
    #             'year': year,
    #             'cited_by_count': cited_count
    #         })

#     return (output_data, authors_data_from_row, sources_data_from_row, output_authors_data_from_row,
#             institutions_data_from_row, output_institutions_data_from_row, concepts_data_from_row,
#             output_concepts_data_from_row, output_topics_data_from_row, output_keywords_data_from_row,
#             output_mesh_terms_data_from_row, locations_records_from_row, sdgs_data_from_row, output_sdgs_data_from_row,
#             grants_data_from_row, output_grants_data_from_row, output_yearly_counts_data_from_row, output_external_ids_data_from_row)

def revert_tuples_to_lists(data_list, table_name):
    """
    Converts specific tuple values back to lists for PostgreSQL array columns.
    """
    converted_data = []
    array_cols = ARRAY_COLUMNS.get(table_name, [])
    
    for record in data_list:
        new_record = {}
        for k, v in record.items():
            if k in array_cols and isinstance(v, tuple):
                new_record[k] = list(v) # Convert tuple back to list for array column
            else:
                new_record[k] = v
        converted_data.append(new_record)
    return converted_data


def load_data(conn, cursor, all_collected_data):
    """
    Loads collected and transformed data into PostgreSQL tables using UPSERT logic.
    all_collected_data is a tuple of sets/lists containing unique records for each table.
    """
    (outputs_to_load, authors_to_load, sources_to_load, institutions_to_load,
     concepts_to_load, sdgs_to_load, grants_to_load,
     output_authors_to_load, output_institutions_to_load, output_concepts_to_load,
     output_topics_to_load, output_keywords_to_load, output_mesh_terms_to_load,
     locations_to_load, output_sdgs_to_load, output_grants_to_load,
     output_yearly_counts_to_load, output_external_ids_to_load) = all_collected_data

    print("\n--- Starting Data Load ---")

    try:
        if outputs_to_load:
            output_columns = list(outputs_to_load[0].keys()) if outputs_to_load else []
            output_values = [[o.get(col) for col in output_columns] for o in outputs_to_load]
            upsert_output_query = f"""
                INSERT INTO outputs ({', '.join(output_columns)})
                VALUES %s
                ON CONFLICT (hg_id) DO UPDATE SET
                    {', '.join([f"{col} = EXCLUDED.{col}" for col in output_columns if col != 'hg_id'])};
            """
            extras.execute_values(cursor, upsert_output_query, output_values)
            print(f"Loaded {len(outputs_to_load)} unique outputs.")
        else:
            print("No outputs data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading outputs: {e}")
        if outputs_to_load:
            print(f"First problematic output data: {outputs_to_load[0]}")
            problem_value = next((o.get(col) for col in output_columns if "Year" in str(o.get(col)) or "DOI" in str(o.get(col))), None)
            print(f"Potentially problematic value in outputs: {problem_value}")


    try:
        if authors_to_load:
            # Revert tuples to lists for array columns for authors (if any)
            authors_to_load_final = revert_tuples_to_lists(authors_to_load, 'authors')

            if authors_to_load_final:
                author_columns = list(authors_to_load_final[0].keys())
                author_values = [[a.get(col) for col in author_columns] for a in authors_to_load_final]
                upsert_author_query = f"""
                    INSERT INTO authors ({', '.join(author_columns)})
                    VALUES %s
                    ON CONFLICT (hg_id) DO UPDATE SET
                        {', '.join([f"{col} = EXCLUDED.{col}" for col in author_columns if col != 'hg_id'])};
                """
                extras.execute_values(cursor, upsert_author_query, author_values)
                print(f"Loaded {len(authors_to_load_final)} unique authors.")
        else:
            print("No authors data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading authors: {e}")
        if authors_to_load: print(f"First problematic author data: {authors_to_load[0]}")


    try:
        if sources_to_load:
            # Revert tuples to lists for array columns in sources
            sources_to_load_final = revert_tuples_to_lists(sources_to_load, 'sources')

            if sources_to_load_final:
                source_columns = list(sources_to_load_final[0].keys())
                source_values = [[s.get(col) for col in source_columns] for s in sources_to_load_final]
                upsert_source_query = f"""
                    INSERT INTO sources ({', '.join(source_columns)})
                    VALUES %s
                    ON CONFLICT (hg_id) DO UPDATE SET
                        {', '.join([f"{col} = EXCLUDED.{col}" for col in source_columns if col != 'hg_id'])};
                """
                extras.execute_values(cursor, upsert_source_query, source_values)
                print(f"Loaded {len(sources_to_load_final)} unique sources.")
        else:
            print("No sources data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading sources: {e}")
        if sources_to_load: print(f"First problematic source data: {sources_to_load[0]}")


    try:
        if institutions_to_load:
            # Revert tuples to lists for array columns in institutions
            institutions_to_load_final = revert_tuples_to_lists(institutions_to_load, 'institutions')

            if institutions_to_load_final:
                inst_columns = list(institutions_to_load_final[0].keys())
                inst_values = [[i.get(col) for col in inst_columns] for i in institutions_to_load_final]
                upsert_institution_query = f"""
                    INSERT INTO institutions ({', '.join(inst_columns)})
                    VALUES %s
                    ON CONFLICT (hg_id) DO UPDATE SET
                        {', '.join([f"{col} = EXCLUDED.{col}" for col in inst_columns if col != 'hg_id'])};
                """
                extras.execute_values(cursor, upsert_institution_query, inst_values)
                print(f"Loaded {len(institutions_to_load_final)} unique institutions.")
        else:
            print("No institutions data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading institutions: {e}")
        if institutions_to_load: print(f"First problematic institution data: {institutions_to_load[0]}")


    try:
        if concepts_to_load:
            # Concepts table does not have array columns, no revert needed.
            if concepts_to_load:
                concept_columns = list(concepts_to_load[0].keys())
                concept_values = [[c.get(col) for col in concept_columns] for c in concepts_to_load]
                upsert_concept_query = f"""
                    INSERT INTO concepts ({', '.join(concept_columns)})
                    VALUES %s
                    ON CONFLICT (hg_id) DO UPDATE SET
                        {', '.join([f"{col} = EXCLUDED.{col}" for col in concept_columns if col != 'hg_id'])};
                """
                extras.execute_values(cursor, upsert_concept_query, concept_values)
                print(f"Loaded {len(concepts_to_load)} unique concepts.")
        else:
            print("No concepts data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading concepts: {e}")
        if concepts_to_load: print(f"First problematic concept data: {concepts_to_load[0]}")


    try:
        if sdgs_to_load:
            # SDGs table does not have array columns, no revert needed.
            if sdgs_to_load:
                sdg_columns = list(sdgs_to_load[0].keys()) # Correctly get columns from the first SDG dict
                sdg_values = [[s.get(col) for col in sdg_columns] for s in sdgs_to_load] # Corrected inner loop to use sdg_columns
                upsert_sdg_query = f"""
                    INSERT INTO sdgs_data ({', '.join(sdg_columns)})
                    VALUES %s
                    ON CONFLICT (hg_id) DO UPDATE SET
                        {', '.join([f"{col} = EXCLUDED.{col}" for col in sdg_columns if col != 'hg_id'])};
                """
                extras.execute_values(cursor, upsert_sdg_query, sdg_values)
                print(f"Loaded {len(sdgs_to_load)} unique SDGs.")
        else:
            print("No SDGs data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading SDGs: {e}")
        if sdgs_to_load: print(f"First problematic SDG data: {sdgs_to_load[0]}")


    try:
        if grants_to_load:
            # Grants table does not have array columns, no revert needed.
            if grants_to_load:
                grant_columns = list(grants_to_load[0].keys())
                grant_values = [[g.get(col) for col in grant_columns] for g in grants_to_load]
                upsert_grant_query = f"""
                    INSERT INTO grants_data ({', '.join(grant_columns)})
                    VALUES %s
                    ON CONFLICT (hg_id) DO UPDATE SET
                        {', '.join([f"{col} = EXCLUDED.{col}" for col in grant_columns if col != 'hg_id'])};
                """
                extras.execute_values(cursor, upsert_grant_query, grant_values)
                print(f"Loaded {len(grants_to_load)} unique grants.")
        else:
            print("No grants data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading grants: {e}")
        if grants_to_load: print(f"First problematic grant data: {grants_to_load[0]}")


    # --- Load Join/Dependent Tables ---

    try:
        if output_authors_to_load:
            # Join tables are already sets of frozensets, convert to list of dicts first
            output_authors_list_of_dicts = [dict(fs) for fs in output_authors_to_load]
            output_authors_final = revert_tuples_to_lists(output_authors_list_of_dicts, 'output_authors')

            output_author_columns = list(output_authors_final[0].keys()) if output_authors_final else []
            output_author_values = [[wa.get(col) for col in output_author_columns] for wa in output_authors_final]
            insert_output_authors_query = f"""
                INSERT INTO output_authors ({', '.join(output_author_columns)})
                VALUES %s
                ON CONFLICT (output_id, author_id, author_position) DO NOTHING;
            """
            extras.execute_values(cursor, insert_output_authors_query, output_author_values)
            print(f"Loaded {len(output_authors_final)} output-author relationships.")
        else:
            print("No output-authors data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading output_authors: {e}")
        if output_authors_to_load: print(f"First problematic output_authors data: {output_authors_to_load[0]}")


    try:
        if output_institutions_to_load:
            output_institutions_list_of_dicts = [dict(fs) for fs in output_institutions_to_load]
            output_institutions_final = revert_tuples_to_lists(output_institutions_list_of_dicts, 'output_institutions') # No array columns, but pass for consistency.

            output_institution_columns = list(output_institutions_final[0].keys()) if output_institutions_final else []
            output_institution_values = [[wi.get(col) for col in output_institution_columns] for wi in output_institutions_final]
            insert_output_institutions_query = f"""
                INSERT INTO output_institutions ({', '.join(output_institution_columns)})
                VALUES %s
                ON CONFLICT (output_id, institution_id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_output_institutions_query, output_institution_values)
            print(f"Loaded {len(output_institutions_final)} output-institution relationships.")
        else:
            print("No output-institutions data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading output_institutions: {e}")
        if output_institutions_to_load: print(f"First problematic output_institutions data: {output_institutions_to_load[0]}")


    try:
        if output_concepts_to_load:
            output_concepts_list_of_dicts = [dict(fs) for fs in output_concepts_to_load]
            # No array columns, no revert needed
            output_concept_columns = list(output_concepts_list_of_dicts[0].keys()) if output_concepts_list_of_dicts else []
            output_concept_values = [[wc.get(col) for col in output_concept_columns] for wc in output_concepts_list_of_dicts]
            insert_output_concepts_query = f"""
                INSERT INTO output_concepts ({', '.join(output_concept_columns)})
                VALUES %s
                ON CONFLICT (output_id, concept_id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_output_concepts_query, output_concept_values)
            print(f"Loaded {len(output_concepts_list_of_dicts)} output-primary_concept relationships.")
        else:
            print("No output-primary_concept data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading output_concepts: {e}")
        if output_concepts_to_load: print(f"First problematic output_concepts data: {output_concepts_to_load[0]}")
        
    
    try:
        if output_topics_to_load:
            output_topics_list_of_dicts = [dict(fs) for fs in output_topics_to_load]
            # No array columns, no revert needed
            output_topic_columns = list(output_topics_list_of_dicts[0].keys()) if output_topics_list_of_dicts else []
            output_topic_values = [[wt.get(col) for col in output_topic_columns] for wt in output_topics_list_of_dicts]
            insert_output_topics_query = f"""
                INSERT INTO output_topics ({', '.join(output_topic_columns)})
                VALUES %s
                ON CONFLICT (output_id, concept_id) DO UPDATE SET score = EXCLUDED.score;
            """
            extras.execute_values(cursor, insert_output_topics_query, output_topic_values)
            print(f"Loaded {len(output_topics_list_of_dicts)} output-topic relationships.")
        else:
            print("No output-topics data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading output_topics: {e}")
        if output_topics_to_load: print(f"First problematic output_topics data: {output_topics_to_load[0]}")


    try:
        if output_keywords_to_load:
            output_keywords_list_of_dicts = [dict(fs) for fs in output_keywords_to_load]
            # No array columns, no revert needed
            output_keyword_columns = list(output_keywords_list_of_dicts[0].keys()) if output_keywords_list_of_dicts else []
            output_keyword_values = [[wk.get(col) for col in output_keyword_columns] for wk in output_keywords_list_of_dicts]
            insert_output_keywords_query = f"""
                INSERT INTO output_keywords ({', '.join(output_keyword_columns)})
                VALUES %s
                ON CONFLICT (output_id, concept_id) DO UPDATE SET score = EXCLUDED.score;
            """
            extras.execute_values(cursor, insert_output_keywords_query, output_keyword_values)
            print(f"Loaded {len(output_keywords_list_of_dicts)} output-keyword relationships.")
        else:
            print("No output-keywords data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading output_keywords: {e}")
        if output_keywords_to_load: print(f"First problematic output_keywords data: {output_keywords_to_load[0]}")


    try:
        if output_mesh_terms_to_load:
            output_mesh_terms_list_of_dicts = [dict(fs) for fs in output_mesh_terms_to_load]
            # No array columns, no revert needed
            output_mesh_columns = list(output_mesh_terms_list_of_dicts[0].keys()) if output_mesh_terms_list_of_dicts else []
            output_mesh_values = [[wm.get(col) for col in output_mesh_columns] for wm in output_mesh_terms_list_of_dicts]
            insert_output_mesh_query = f"""
                INSERT INTO output_mesh_terms ({', '.join(output_mesh_columns)})
                VALUES %s
                ON CONFLICT (output_id, concept_id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_output_mesh_query, output_mesh_values)
            print(f"Loaded {len(output_mesh_terms_list_of_dicts)} output-mesh_term relationships.")
        else:
            print("No output-mesh_terms data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading output_mesh_terms: {e}")
        if output_mesh_terms_to_load: print(f"First problematic output_mesh_terms data: {output_mesh_terms_to_load[0]}")


    try:
        if locations_to_load:
            # No array columns, no revert needed.
            location_columns = list(locations_to_load[0].keys()) if locations_to_load else []
            location_values = [[lr.get(col) for col in location_columns] for lr in locations_to_load]
            insert_locations_query = f"""
                INSERT INTO locations_data ({', '.join(location_columns)})
                VALUES %s;
            """
            extras.execute_values(cursor, insert_locations_query, location_values)
            print(f"Loaded {len(locations_to_load)} location records.")
        else:
            print("No locations data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading locations_data: {e}")
        if locations_to_load: print(f"First problematic locations_data: {locations_to_load[0]}")


    try:
        if output_sdgs_to_load:
            output_sdgs_list_of_dicts = [dict(fs) for fs in output_sdgs_to_load]
            # No array columns, no revert needed.
            output_sdg_columns = list(output_sdgs_list_of_dicts[0].keys()) if output_sdgs_list_of_dicts else []
            output_sdg_values = [[ws.get(col) for col in output_sdg_columns] for ws in output_sdgs_list_of_dicts]
            insert_output_sdgs_query = f"""
                INSERT INTO output_sdgs ({', '.join(output_sdg_columns)})
                VALUES %s
                ON CONFLICT (output_id, sdg_id) DO UPDATE SET score = EXCLUDED.score;
            """
            extras.execute_values(cursor, insert_output_sdgs_query, output_sdg_values)
            print(f"Loaded {len(output_sdgs_list_of_dicts)} output-SDG relationships.")
        else:
            print("No output-SDGs data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading output_sdgs: {e}")
        if output_sdgs_to_load: print(f"First problematic output_sdgs data: {output_sdgs_to_load[0]}")


    try:
        if output_grants_to_load:
            output_grants_list_of_dicts = [dict(fs) for fs in output_grants_to_load]
            # No array columns, no revert needed.
            output_grant_columns = list(output_grants_list_of_dicts[0].keys()) if output_grants_list_of_dicts else []
            output_grant_values = [[wg.get(col) for col in output_grant_columns] for wg in output_grants_list_of_dicts]
            insert_output_grants_query = f"""
                INSERT INTO output_grants ({', '.join(output_grant_columns)})
                VALUES %s
                ON CONFLICT (output_id, grant_id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_output_grants_query, output_grant_values)
            print(f"Loaded {len(output_grants_list_of_dicts)} output-grant relationships.")
        else:
            print("No output-grants data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading output_grants: {e}")
        if output_grants_to_load: print(f"First problematic output_grants data: {output_grants_to_load[0]}")


    try:
        if output_yearly_counts_to_load:
            output_yearly_counts_list_of_dicts = [dict(fs) for fs in output_yearly_counts_to_load]
            # No array columns, no revert needed.
            output_yearly_count_columns = list(output_yearly_counts_list_of_dicts[0].keys()) if output_yearly_counts_list_of_dicts else []
            output_yearly_count_values = [[wyc.get(col) for col in output_yearly_count_columns] for wyc in output_yearly_counts_list_of_dicts]
            insert_output_yearly_counts_query = f"""
                INSERT INTO output_yearly_counts ({', '.join(output_yearly_count_columns)})
                VALUES %s
                ON CONFLICT (output_id, year) DO UPDATE SET cited_by_count = EXCLUDED.cited_by_count;
            """
            extras.execute_values(cursor, insert_output_yearly_counts_query, output_yearly_count_values)
            print(f"Loaded {len(output_yearly_counts_list_of_dicts)} output-yearly counts.")
        else:
            print("No output-yearly counts data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading output_yearly_counts: {e}")
        if output_yearly_counts_to_load: print(f"First problematic output_yearly_counts data: {output_yearly_counts_to_load[0]}")


    try:
        if output_external_ids_to_load:
            output_external_ids_list_of_dicts = [dict(fs) for fs in output_external_ids_to_load]
            # No array columns, no revert needed.
            output_external_id_columns = list(output_external_ids_list_of_dicts[0].keys()) if output_external_ids_list_of_dicts else []
            output_external_id_values = [[wei.get(col) for col in output_external_id_columns] for wei in output_external_ids_list_of_dicts]
            insert_output_external_ids_query = f"""
                INSERT INTO output_external_ids ({', '.join(output_external_id_columns)})
                VALUES %s
                ON CONFLICT (output_id, id_type) DO UPDATE SET external_id = EXCLUDED.external_id;
            """
            extras.execute_values(cursor, insert_output_external_ids_query, output_external_id_values)
            print(f"Loaded {len(output_external_ids_list_of_dicts)} output-external IDs.")
        else:
            print("No output-external IDs data to load.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR loading output_external_ids: {e}")
        if output_external_ids_to_load: print(f"First problematic output_external_ids data: {output_external_ids_to_load[0]}")

    conn.commit()
    print("\nAll data load operations completed and committed.")


# --- Main ETL Process ---

def run_etl():
    """Main function to run the ETL process from CSV."""
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: CSV file not found at {CSV_FILE_PATH}")
        print("Please ensure the CSV file is in the same directory as the script or provide the full path.")
        return

    try:
        print(f"Reading data from CSV file: {CSV_FILE_PATH}...")
        # Using encoding='latin1' as a common fallback for CSV issues.
        df = pd.read_csv(CSV_FILE_PATH, keep_default_na=False, dtype=str, encoding='latin1') 
        df.replace('nan', '', inplace=True)
        print(f"Successfully read {len(df)} rows from CSV.")

    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    conn = None
    try:
        print("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Connected to database.")

        create_tables(cursor, conn)

        # Use dictionaries keyed by hg_id for main entities to ensure uniqueness
        collected_outputs = {}
        collected_authors = {}
        collected_sources = {}
        collected_institutions = {}
        collected_concepts = {}
        collected_sdgs = {}
        collected_grants = {}

        # Join tables directly use sets of frozensets (which are hashable) for uniqueness of relationships
        collected_output_authors = set() 
        collected_output_institutions = set()
        collected_output_concepts = set()
        collected_output_topics = set()
        collected_output_keywords = set()
        collected_output_mesh_terms = set()
        collected_locations = [] # Locations have SERIAL PK, no deduplication needed this way
        collected_output_sdgs = set()
        collected_output_grants = set()
        collected_output_yearly_counts = set()
        collected_output_external_ids = set()

        processed_rows = 0
        print("\n--- Starting Data Processing from CSV Rows ---")
        for index, row in df.iterrows():
            # transformed_data = process_excel_row(row)
            authors_data_from_row = authors_process_row(row)
            concepts_data_from_row = concept_process_row(row)
            institutions_data_from_row = institutions_process_row(row)
            grants_data_from_row = grand_data_process_row(row)
            output_data = outputs_process_row(row)
            sdgs_data_from_row = sdgs_process_row(row)
            sources_data_from_row = source_process_row(row)

            output_mesh_terms_data_from_row = output_mesh_terms_process_row(row)
            output_sdgs_data_from_row= outputs_sdgs_data_process_row(row)    
            output_grants_data_from_row= outputs_grand_data_process_row(row)
            output_authors_data_from_row = output_authors_process_row(row)
            output_external_ids_data_from_row = output_external_ids_process_row(row)
            output_institutions_data_from_row = output_institution_process_row(row)
            output_keywords_data_from_row = output_keywords_process_row(row)
            output_yearly_counts_data_from_row = outputs_yearly_counts_data_process_row(row)
            output_concepts_data_from_row = output_concepts_process_row(row)
            output_topics_data_from_row = output_topics_process_row(row)
            locations_records_from_row = outputs_location_process_row(row)

            # if not transformed_data[0]:
            #     continue

            # output_data, authors_data_from_row, sources_data_from_row, output_authors_data_from_row, \
            # institutions_data_from_row, output_institutions_data_from_row, concepts_data_from_row, \
            # output_concepts_data_from_row, output_topics_data_from_row, output_keywords_data_from_row, \
            # output_mesh_terms_data_from_row, locations_records_from_row, sdgs_data_from_row, output_sdgs_data_from_row, \
            # grants_data_from_row, output_grants_data_from_row, output_yearly_counts_data_from_row, output_external_ids_data_from_row = transformed_data

            # Explicitly define output_hg_id after unpacking output_data
            output_hg_id = output_data.get('hg_id')

            # Collect main entities, ensuring uniqueness using their hg_id
            if output_data and output_hg_id:
                collected_outputs[output_hg_id] = output_data

            for a in authors_data_from_row:
                if a.get('hg_id'):
                    collected_authors[a['hg_id']] = a

            for s in sources_data_from_row:
                if s.get('hg_id'):
                    collected_sources[s['hg_id']] = s

            # Institutions are collected here, but their `display_name`, `ror`, etc.,
            # might not be fully populated until a later join/enrichment step if
            # the raw data in CSV only provides IDs for these.
            # The crucial part is to get the `hg_id` and `openalex_id` correct.
            for i_data in institutions_data_from_row:
                if i_data.get('hg_id'):
                    # Update existing entry or add new one
                    collected_institutions.setdefault(i_data['hg_id'], {}).update(i_data)


            for c_data in concepts_data_from_row:
                if c_data.get('hg_id'):
                    collected_concepts[c_data['hg_id']] = c_data

            for s_data in sdgs_data_from_row:
                if s_data.get('hg_id'):
                    collected_sdgs[s_data['hg_id']] = s_data
            
            for g_data in grants_data_from_row:
                if g_data.get('hg_id'):
                    collected_grants[g_data['hg_id']] = g_data


            # Collect join table data (using sets of frozensets for uniqueness)
            # IMPORTANT: Only add join records if their corresponding output_id exists in collected_outputs
            if output_hg_id in collected_outputs: # Check if the parent output was successfully collected
                for wa in output_authors_data_from_row:
                    collected_output_authors.add(make_hashable_for_set(wa))
                for wi in output_institutions_data_from_row:
                    collected_output_institutions.add(make_hashable_for_set(wi))
                for wc in output_concepts_data_from_row:
                    collected_output_concepts.add(make_hashable_for_set(wc))
                for wt in output_topics_data_from_row:
                    collected_output_topics.add(make_hashable_for_set(wt))
                for wk in output_keywords_data_from_row:
                    collected_output_keywords.add(make_hashable_for_set(wk))
                for wm in output_mesh_terms_data_from_row:
                    collected_output_mesh_terms.add(make_hashable_for_set(wm))
                for loc in locations_records_from_row:
                    collected_locations.append(loc) 
                for ws in output_sdgs_data_from_row:
                    collected_output_sdgs.add(make_hashable_for_set(ws))
                for wg in output_grants_data_from_row:
                    collected_output_grants.add(make_hashable_for_set(wg))
                for wyc in output_yearly_counts_data_from_row:
                    collected_output_yearly_counts.add(make_hashable_for_set(wyc))
                for wei in output_external_ids_data_from_row:
                    collected_output_external_ids.add(make_hashable_for_set(wei))
            else:
                print(f"Skipping join data for output_id {output_hg_id} as it was not collected successfully.")


            processed_rows += 1
            if processed_rows % 100 == 0:
                print(f"Processed {processed_rows} rows from CSV...")

        print(f"\nFinished processing all {processed_rows} rows from CSV.")
        print("Preparing data for load...")

        # Convert collected dictionaries to lists for loading
        final_outputs_data = list(collected_outputs.values())
        final_authors_data = list(collected_authors.values())
        final_sources_data = list(collected_sources.values())
        final_institutions_data = list(collected_institutions.values())
        final_concepts_data = list(collected_concepts.values())
        final_sdgs_data = list(collected_sdgs.values())
        final_grants_data = list(collected_grants.values())

        # For join tables, convert frozensets back to dicts. `revert_tuples_to_lists` will handle array columns.
        final_output_authors_data = [dict(fs) for fs in collected_output_authors]
        final_output_institutions_data = [dict(fs) for fs in collected_output_institutions]
        final_output_concepts_data = [dict(fs) for fs in collected_output_concepts]
        final_output_topics_data = [dict(fs) for fs in collected_output_topics]
        final_output_keywords_data = [dict(fs) for fs in collected_output_keywords]
        final_output_mesh_terms_data = [dict(fs) for fs in collected_output_mesh_terms]
        final_locations_data = collected_locations # Already a list of dicts
        final_output_sdgs_data = [dict(fs) for fs in collected_output_sdgs]
        final_output_grants_data = [dict(fs) for fs in collected_output_grants]
        final_output_yearly_counts_data = [dict(fs) for fs in collected_output_yearly_counts]
        final_output_external_ids_data = [dict(fs) for fs in collected_output_external_ids]
        
        all_data_for_load = (
            final_outputs_data, final_authors_data, final_sources_data, final_institutions_data,
            final_concepts_data, final_sdgs_data, final_grants_data,
            final_output_authors_data, final_output_institutions_data, final_output_concepts_data,
            final_output_topics_data, final_output_keywords_data, final_output_mesh_terms_data,
            final_locations_data, final_output_sdgs_data, final_output_grants_data,
            final_output_yearly_counts_data, final_output_external_ids_data
        )

        load_data(conn, cursor, all_data_for_load)

        print(f"\n--- ETL Process Summary ---")
        print(f"Total rows from CSV processed: {processed_rows}")
        print(f"Unique Outputs collected for load: {len(final_outputs_data)}")
        print(f"Unique Authors collected for load: {len(final_authors_data)}")
        print(f"Unique Sources collected for load: {len(final_sources_data)}")
        print(f"Unique Institutions collected for load: {len(final_institutions_data)}")
        print(f"Unique Concepts collected for load: {len(final_concepts_data)}")
        print(f"Unique SDGs collected for load: {len(final_sdgs_data)}")
        print(f"Unique Grants collected for load: {len(final_grants_data)}")
        print(f"Output-Authors relationships collected for load: {len(final_output_authors_data)}")
        print(f"Output-Institutions relationships collected for load: {len(final_output_institutions_data)}")
        print(f"Output-Concepts (primary) relationships collected for load: {len(final_output_concepts_data)}")
        print(f"Output-Topics relationships collected for load: {len(final_output_topics_data)}")
        print(f"Output-Keywords relationships collected for load: {len(final_output_keywords_data)}")
        print(f"Output-Mesh relationships collected for load: {len(final_output_mesh_terms_data)}")
        print(f"Location records collected for load: {len(final_locations_data)}")
        print(f"Output-SDG relationships collected for load: {len(final_output_sdgs_data)}")
        print(f"Output-Grants relationships collected for load: {len(final_output_grants_data)}")
        print(f"Output-Yearly Counts collected for load: {len(final_output_yearly_counts_data)}")
        print(f"Output-External IDs collected for load: {len(final_output_external_ids_data)}")
        print("ETL process completed successfully.")

    except Exception as e:
        print(f"An unexpected error occurred during ETL: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    run_etl()