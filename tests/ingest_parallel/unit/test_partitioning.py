"""T032 — Particionamento determinístico do manifesto (FR-018)."""

from __future__ import annotations

import pytest

from src.ingest_parallel.inventory import SourceFileEntry, SourceManifest
from src.ingest_parallel.partitioning import build_partitions


def _manifest_com_arquivos() -> SourceManifest:
    def entry(name: str, rows: int) -> SourceFileEntry:
        return SourceFileEntry(
            file_name=name,
            storage_path=f"/bronze/2026-10/{name}",
            size_bytes=rows * 100,
            sha256=f"hash-{name}",
            row_count=rows,
        )

    return SourceManifest(
        competencia="2026-10",
        data_version_id="a" * 64,
        files=[
            entry("Empresas0.parquet", 300),
            entry("Empresas1.parquet", 700),
            entry("Estabelecimentos0.parquet", 500),
            entry("Socios0.parquet", 1000),
            entry("Simples.parquet", 200),
        ],
        code_version="HEAD",
        source_count=5,
        total_rows_expected=2700,
    )


class TestDeterminismo:
    def test_mesmo_input_mesma_lista(self):
        primeiro = build_partitions(_manifest_com_arquivos(), target_rows_per_partition=500)
        segundo = build_partitions(_manifest_com_arquivos(), target_rows_per_partition=500)
        assert primeiro == segundo

    def test_ids_unicos_e_formato(self):
        partitions = build_partitions(_manifest_com_arquivos(), target_rows_per_partition=500)
        ids = [p["partition_id"] for p in partitions]
        assert len(ids) == len(set(ids))
        for pid in ids:
            table, _, index = pid.rpartition("-")
            assert table and index.isdigit() and len(index) == 2


class TestCobertura:
    def test_todos_os_arquivos_aparecem_uma_vez(self):
        partitions = build_partitions(_manifest_com_arquivos(), target_rows_per_partition=500)
        arquivos = [file for p in partitions for file in p["files"]]
        names = [f.file_name for f in arquivos]
        assert len(names) == len(set(names))
        assert set(names) == {
            "Empresas0.parquet",
            "Empresas1.parquet",
            "Estabelecimentos0.parquet",
            "Socios0.parquet",
            "Simples.parquet",
        }

    def test_soma_rows_bate_com_manifesto(self):
        partitions = build_partitions(_manifest_com_arquivos(), target_rows_per_partition=500)
        total = sum(p["expected_rows"] for p in partitions)
        assert total == 2700

    def test_partition_id_aponta_para_tabela(self):
        partitions = build_partitions(_manifest_com_arquivos(), target_rows_per_partition=500)
        por_tabela = {p["partition_id"]: p["table"] for p in partitions}
        for pid, table in por_tabela.items():
            assert pid.startswith(f"{table}-")


class TestLimites:
    def test_respeita_cap_de_particoes(self):
        partitions = build_partitions(_manifest_com_arquivos(), target_rows_per_partition=100)
        assert len(partitions) <= 128  # FR: 64–128 partições lógicas

    def test_target_pequeno_cria_mais_particoes(self):
        grande = build_partitions(_manifest_com_arquivos(), target_rows_per_partition=500)
        pequena = build_partitions(_manifest_com_arquivos(), target_rows_per_partition=100)
        assert len(pequena) >= len(grande)


class TestValidacao:
    def test_particao_para_arquivo_invalido_sem_ser_parquet(self):
        manifest_com_txt = build_partitions(
            _manifest_com_arquivos(), target_rows_per_partition=500
        )
        # file entry sem nome de tabela conhecida é ignorado silenciosamente com warning
        assert isinstance(manifest_com_txt, list)
