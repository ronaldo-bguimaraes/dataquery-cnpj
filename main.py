import locale
import time
import zipfile

from pathlib import Path
from conversor import convert_csv_to_parquet
from utils import get_available_memory

locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

def convert_compressed_csv_to_parquet(
    filepath: str,
    columns_names: list[str],
    chunk_size: int = 1024 * 1000
):
    newfilepath = Path(filepath).with_suffix(".parquet")
    with zipfile.ZipFile(filepath) as zip:
        namelist = zip.namelist()
        if len(namelist) > 1:
            raise RuntimeError(f"Expected exactly one file inside ZIP, found {len(namelist)}")
        filename = namelist[0]
        with zip.open(filename) as file:
            return convert_csv_to_parquet(file, newfilepath, columns_names, chunk_size, encoding="latin1")

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

def test_time():
    print("iniciando conversao para parquet...")
    start = time.perf_counter()
    ideal_chunk = get_available_memory() * 0.05
    metrics = convert_compressed_csv_to_parquet(filepath, columns_names, ideal_chunk)
    duration = time.perf_counter() - start
    iterations = metrics["iterations"]
    processed_lines = metrics["processed_lines"]
    formatted_string  = locale.format_string("%.2f", processed_lines, grouping=True)
    print(f"parquet com {formatted_string} linhas gerado com sucesso em {iterations} iteracoes em {duration:.2f}s")

test_time()
