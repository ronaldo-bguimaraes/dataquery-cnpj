import os
from pathlib import Path
import time
from conversor import convert_compressed_csv_to_parquet
from utils import get_available_memory


class Process:
    def __init__(self):
        self._input_dir = Path("C:\\tmp\\2026-02")
        self._target_dir = Path("C:\\tmp\\target")

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
        for file in self._input_dir.iterdir():
            filename = file.name
            if filename.startswith("Empresas"):
                print(f"convertendo arquivo {file.resolve()}")
                columns = self.columns_empresas()
                ideal_chunk = get_available_memory() * 0.05
                output_path = Path(self._target_dir, file.with_suffix(".parquet").name)
                start = time.perf_counter()
                convert_compressed_csv_to_parquet(file, columns, ideal_chunk, output_path)
                duration = time.perf_counter() - start
                print(f"arquivo {output_path.resolve()} gerado com sucesso em {duration:.2f}s")


process = Process()
process.convert_all()

# metrics = convert_compressed_csv_to_parquet(filepath, columns_names, ideal_chunk)
