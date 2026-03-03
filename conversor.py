import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

from utils import pyarrow_schema_from_list

def convert_csv_to_parquet(
    input_file,
    output_path,
    columns_names: list[str],
    chunk_size: int = 1024 * 1000,
    encoding: str = "utf-8",
    strings_can_be_null: bool = True,
    default_type: pa.DataType = pa.string(),
    delimiter: str = ";"
):
    schema = pyarrow_schema_from_list(columns_names, default_type)
    with pq.ParquetWriter(
        output_path,
        schema,
        write_statistics=False
    ) as writer:
        reader_options = pv.ReadOptions(
            column_names=columns_names,
            block_size=chunk_size,
            encoding=encoding
        )
        convert_options = pv.ConvertOptions(
            strings_can_be_null=strings_can_be_null,
            column_types=schema
        )
        parser_options = pv.ParseOptions(
            delimiter=delimiter
        )
        iterator = pv.open_csv(
            input_file,
            read_options=reader_options,
            parse_options=parser_options,
            convert_options=convert_options
        )
        iterations = 0
        processed_lines = 0
        for table in iterator:
            processed_lines += table.num_rows
            writer.write(table)
            iterations += 1
        return {
            "iterations": iterations,
            "processed_lines": processed_lines
        }
