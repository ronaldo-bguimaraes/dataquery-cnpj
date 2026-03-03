import psutil
import pyarrow as pa

from typing import Iterable


def pyarrow_schema_from_list(
    columns_names: Iterable[str],
    default_type: pa.DataType,
    as_dict: bool = False
):
    dict_schema = dict.fromkeys(columns_names, default_type)
    if as_dict:
        return dict_schema
    return pa.schema(dict_schema)


def get_available_memory():
    svmem = psutil.virtual_memory()
    return svmem.available
