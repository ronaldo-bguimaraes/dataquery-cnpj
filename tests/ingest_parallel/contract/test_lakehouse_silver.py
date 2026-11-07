"""T015 — Contrato da camada Silver Iceberg (contracts/lakehouse_silver.md)."""

from __future__ import annotations

import importlib

import pytest

lakehouse = importlib.import_module("src.ingest_parallel.lakehouse")


class TestPropriedadesContratadas:
    def test_target_file_size_512mb(self):
        props = lakehouse.SEALED_TABLE_PROPERTIES
        assert props["write.target-file-size-bytes"] == "536870912"

    def test_formato_parquet_zstd(self):
        props = lakehouse.SEALED_TABLE_PROPERTIES
        assert props["write.format.default"] == "parquet"
        assert props["write.parquet.compression-codec"] == "zstd"

    def test_format_version_2(self):
        assert lakehouse.SEALED_TABLE_PROPERTIES["format-version"] == "2"


class TestParticionamento:
    def test_competencia_e_coluna_de_particao_em_todas(self):
        for table, partitions in lakehouse.SILVER_PARTITIONING.items():
            assert "competencia" in partitions, table

    def test_tabelas_conhecidas(self):
        assert set(lakehouse.SILVER_PARTITIONING) >= {
            "silver_empresas",
            "silver_estabelecimentos",
            "silver_socios",
            "silver_simples",
        }


class TestBranchWAP:
    def test_branch_nome_por_competencia(self):
        assert lakehouse.load_branch_name("2026-10") == "load_2026-10"

    def test_branch_nunca_main(self):
        assert lakehouse.load_branch_name("2026-10") != "main"

    def test_main_e_branch_padrao(self):
        assert lakehouse.PUBLISH_BRANCH == "main"
