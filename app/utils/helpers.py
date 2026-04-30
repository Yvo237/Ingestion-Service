import re
import math

def clean_column_name(name: str) -> str:
    """
    Standardizes column names: lowercase, strip whitespace, replace non-alphanumeric with underscores.
    """
    if not isinstance(name, str):
        return str(name)
    
    name = name.strip().lower().replace(' ', '_')
    return re.sub(r'[^\w]', '_', name)

def sanitize_json(obj):
    """
    Recursively converts non-serializable types (Pandas Timestamp, Numpy types, NaNs) 
    to standard JSON-compatible types.
    """
    if isinstance(obj, dict):
        return {k: sanitize_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_json(i) for i in obj]
    elif hasattr(obj, 'isoformat'): # For Timestamp, datetime, date
        return obj.isoformat()
    elif hasattr(obj, 'item'): # For numpy types (int64, float64, etc.)
        val = obj.item()
        return sanitize_json(val)
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj
