import pyarrow as pa

def pa_schema_from_list(columns_names, default_type, as_dict: bool = False):
    dict_schema = dict.fromkeys(columns_names, default_type)
    if as_dict:
        return dict_schema
    return pa.schema(dict_schema)
