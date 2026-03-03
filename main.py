import locale
from pathlib import Path
import time
import zipfile
from conversor import convert_csv_to_parquet

locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

def convert_compressed_csv_with_pyarrow(
    filepath: str,
    columns_names: list[str],
    use_pandas: bool = True
):
    newfilepath = Path(filepath).with_suffix(".parquet")
    with zipfile.ZipFile(filepath) as zip:
        namelist = zip.namelist()
        if len(namelist) > 1:
            raise RuntimeError(f"Expected exactly one file inside ZIP, found {len(namelist)}")
        filename = namelist[0]
        with zip.open(filename) as file:
            return convert_csv_to_parquet(file, newfilepath, columns_names, encoding="latin1", use_pandas_csv_engine=use_pandas)

filepath = "tmp/Empresas0.zip"

columns_names = [
    "cnpj_basico",
    "razao_social",
    "natureza_juridica",
    "qualificacao_responsavel",
    "capital_social",
    "porte",
    "ente_federativo"
]

def test_time(use_pandas: bool = False):
    print("iniciando conversao para parquet")
    start = time.perf_counter()
    metrics = convert_compressed_csv_with_pyarrow(filepath, columns_names, use_pandas)
    duration = time.perf_counter() - start
    iterations = metrics["iterations"]
    processed_lines = metrics["processed_lines"]
    formatted_string  = locale.format_string("%.2f", processed_lines, grouping=True)
    print(f"[with_pandas={use_pandas}] parquet com {formatted_string} linhas gerado com sucesso em {iterations} iteracoes em {duration:.2f}s")

test_time(use_pandas=True)
test_time(use_pandas=False)
