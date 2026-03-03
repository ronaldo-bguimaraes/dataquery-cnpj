from abc import ABC, abstractmethod
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq

from utils import pa_schema_from_list


class CsvEngine(ABC):
    def __init__(
        self,
        input_file,
        columns_names: list[str],
        chunk_size: int = 1024 * 1000,
        encoding: str = "utf-8",
        schema: pa.Schema | dict = None
    ):
        self._input_file = input_file
        self._columns_names = columns_names
        self._chunk_size = chunk_size
        self._encoding = encoding

        if not schema:
            self._schema = pa_schema_from_list(self.columns_names, pa.large_string())
        else:
            self._schema = schema

    @property
    def input_file(self):
        return self._input_file
    
    @property
    def columns_names(self):
        return self._columns_names

    @property
    def chunk_size(self):
        return self._chunk_size

    @property
    def encoding(self):
        return self._encoding
    
    @property
    def schema(self):
        return self._schema
    
    @abstractmethod
    def get_iterator(self) -> Iterator[pa.Table]:
        pass


class PandasCsvEngine(CsvEngine):
    def _get_iterator(self):
        import pandas as pd
        return pd.read_csv(
            self.input_file,
            chunksize=self.chunk_size,
            encoding=self.encoding,
            low_memory=True,
            sep=";",
            names=self.columns_names,
            dtype="str"
        )

    def get_iterator(self):
        for df_pandas in self._get_iterator():
            yield pa.Table.from_pandas(df_pandas, schema=self.schema)
    
class PyArrowCsvEngine(CsvEngine):
    def get_iterator(self):
        import pyarrow.csv as pv
        reader_options = pv.ReadOptions(
            column_names=self.columns_names,
            block_size=self.chunk_size,
            encoding=self.encoding
        )
        convert_options = pv.ConvertOptions(
            strings_can_be_null=True,
            column_types=self.schema
        )
        parser_options = pv.ParseOptions(
            delimiter=";"
        )
        return pv.open_csv(
            self.input_file,
            read_options=reader_options,
            parse_options=parser_options,
            convert_options=convert_options
        )

def convert_csv_to_parquet(
    input_file,
    outputpath,
    columns_names: list[str],
    chunk_size: int = 1024 * 1000,
    encoding: str = "utf-8",
    use_pandas_csv_engine: bool = False
):
    schema = pa_schema_from_list(columns_names, pa.large_string())
    with pq.ParquetWriter(outputpath, schema) as writer:

        if use_pandas_csv_engine:
            engine = PandasCsvEngine(input_file, columns_names, chunk_size, encoding)
        else:
            engine = PyArrowCsvEngine(input_file, columns_names, chunk_size, encoding, schema)

        iterations = 0
        processed_lines = 0

        for table in engine.get_iterator():
            processed_lines += table.num_rows
            writer.write(table)
            iterations += 1

        return {
            "iterations": iterations,
            "processed_lines": processed_lines
        }
