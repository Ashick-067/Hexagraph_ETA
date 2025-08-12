from datetime import datetime
import pandas as pd
import json
import uuid

def safe_get_column(row, column_name, default=None):
    """Safely retrieves a column value from a pandas Series (row), handling NaNs and missing columns."""
    if column_name in row.index and pd.notna(row[column_name]):
        return row[column_name]
    return default

def parse_list_string(list_string, delimiter='|'):
    """
    Parses a string representation of a list.
    Tries JSON list, then pipe-separated, then comma-separated.
    """
    if pd.isna(list_string) or str(list_string).strip() == '':
        return []
    s_list_string = str(list_string).strip()

    # 1. Try to parse as JSON list
    try:
        parsed = json.loads(s_list_string)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if pd.notna(item)]
    except (json.JSONDecodeError, TypeError):
        pass

    # 2. If not JSON, try splitting by the specified delimiter (default '|')
    if delimiter in s_list_string:
        return [item.strip() for item in s_list_string.split(delimiter) if item.strip()]
    
    # 3. Fallback to comma-separated if no pipe
    if ',' in s_list_string:
        return [item.strip() for item in s_list_string.split(',') if item.strip()]

    return [s_list_string] if s_list_string else []

def parse_timestamp(ts_val):
    """Parses a timestamp value from Excel/CSV (ISO format) or datetime object."""
    if pd.isna(ts_val):
        return None
    if isinstance(ts_val, datetime):
        return ts_val
    s_ts_val = str(ts_val).strip()
    try:
        return datetime.fromisoformat(s_ts_val.replace('Z', '+00:00'))
    except ValueError:
        return None
    
def parse_date(date_val):
    """Parses a date value from Excel/CSV (DD-MM-YYYY orYYYY-MM-DD) or datetime object."""
    if pd.isna(date_val):
        return None
    if isinstance(date_val, datetime):
        return date_val.date()
    s_date_val = str(date_val).strip()
    try:
        return datetime.strptime(s_date_val, '%d-%m-%Y').date()
    except ValueError:
        try:
            return datetime.strptime(s_date_val, '%Y-%m-%d').date()
        except ValueError:
            return None
        
NAMESPACE_UUID = uuid.UUID('d472c674-32d8-4f2a-b6b6-3a7b9c9f2b3b')         
def generate_hg_id(openalex_id_string):
    """Generates a deterministic UUID (hg_id) from an OpenAlex ID string."""
    if not openalex_id_string:
        return None
    s_openalex_id_string = str(openalex_id_string).strip()
    return str(uuid.uuid5(NAMESPACE_UUID, s_openalex_id_string))

def safe_parse_numeric(value):
    """Safely parses a value to a numeric (float), returning None on failure. Handles lists by taking first element."""
    if pd.isna(value) or str(value).strip() == '':
        return None
    
    if isinstance(value, list) and value:
        value = value[0]

    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return None

def safe_parse_int(value):
    """Safely parses a value to an integer, returning None on failure."""
    if pd.isna(value) or str(value).strip() == '':
        return None
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return None
    

def safe_parse_boolean(value):
    """Safely parses a value to a boolean, returning None for empty strings or unparseable values."""
    if pd.isna(value) or str(value).strip() == '':
        return None
    s_value = str(value).strip().lower()
    if s_value == 'true':
        return True
    elif s_value == 'false':
        return False
    return None # For anything else, return None (which maps to SQL NULL)

def parse_jsonb_string(json_string):
    """Parses a string that might contain a JSON object or array."""
    if pd.isna(json_string) or str(json_string).strip() == '':
        return None
    try:
        return json.loads(str(json_string))
    except (json.JSONDecodeError, TypeError):
        return None    
    
def make_hashable_for_set(obj):
    """
    Recursively converts mutable objects (dicts, lists) into immutable ones (frozensets, tuples)
    to allow the top-level object (usually a dict) to be hashed for set operations.
    """
    if isinstance(obj, dict):
        return frozenset((k, make_hashable_for_set(v)) for k, v in sorted(obj.items()))
    elif isinstance(obj, list):
        return tuple(make_hashable_for_set(elem) for elem in obj)
    else:
        return obj # Base case: immutable type (e.g., string, int, float, None)