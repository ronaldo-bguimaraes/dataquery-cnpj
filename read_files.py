from collections import defaultdict
import os
from pathlib import Path
import time
from conversor import convert_compressed_csv_to_parquet
from read_schema import test_regex
from utils import get_available_memory


class Process:
    def __init__(self):
        self._input_dir = Path("/home/ronaldo/Documents/tmp/2026-02")
        self._target_dir = Path("/home/ronaldo/Documents/tmp/target")

    def columns_empresas(self):
        return [
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte",
            "ente_federativo"
        ]

    def convert_all(self):
        os.makedirs(self._target_dir, exist_ok=True)

        counter = defaultdict(int)

        for file in self._input_dir.iterdir():
            dados = test_regex(file.name)

            etname = dados.get("name", "")
            columns = dados.get("columns", [])
            count = counter[etname]
            
            ideal_chunk = get_available_memory() * 0.05
            output_path = Path(self._target_dir, f"{etname}_{count}.parquet").resolve()
            print(f"convertendo arquivo {file.name} para parquet")
            start = time.perf_counter()
            convert_compressed_csv_to_parquet(file, columns, ideal_chunk, output_path)
            duration = time.perf_counter() - start
            print(f"arquivo {output_path.resolve()} gerado com sucesso em {duration:.2f}s")

            counter[etname] += 1


process = Process()
process.convert_all()
