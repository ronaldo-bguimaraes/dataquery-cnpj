"""T043/T044/T045 — Classificação incremental e evolução de schema (FR-027..032, FR-006)."""

from __future__ import annotations

import pytest

from src.ingest_parallel.changes import classify_files
from src.ingest_parallel.quality import SchemaEvolutionError, validate_schema_evolution


class TestClassificacao:
    def test_novos_modificados_inalterados_removidos(self):
        anterior = {
            "Empresas0.parquet": "hash-a",
            "Estabelecimentos0.parquet": "hash-b",
            "Socios0.parquet": "hash-c",
            "Removido.parquet": "hash-d",
        }
        atual = {
            "Empresas0.parquet": "hash-a",
            "Estabelecimentos0.parquet": "hash-b2",
            "Socios0.parquet": "hash-c",
            "Simples.parquet": "hash-e",
        }
        report = classify_files(anterior, atual)
        assert report["inalterados"] == ["Empresas0.parquet", "Socios0.parquet"]
        assert report["modificados"] == ["Estabelecimentos0.parquet"]
        assert report["novos"] == ["Simples.parquet"]
        assert report["removidos"] == ["Removido.parquet"]

    def test_sem_mudancas_registra_tudo_inalterado(self):
        iguais = {"A.parquet": "h", "B.parquet": "h2"}
        report = classify_files(iguais, iguais)
        assert report == {
            "novos": [],
            "modificados": [],
            "inalterados": ["A.parquet", "B.parquet"],
            "removidos": [],
        }

    def test_particoes_so_incluem_mudancas(self):
        from src.ingest_parallel.changes import partitions_forte_mudancas

        partitions = [
            {
                "partition_id": "empresas-00",
                "table": "empresas",
                "files": [{"file_name": "Empresas0.parquet", "row_count": 100}],
                "expected_rows": 100,
            },
            {
                "partition_id": "estabelecimentos-00",
                "table": "estabelecimentos",
                "files": [{"file_name": "Estabelecimentos0.parquet", "row_count": 300}],
                "expected_rows": 300,
            },
        ]
        mudados = ["Estabelecimentos0.parquet"]
        reduzidas = partitions_forte_mudancas(partitions, [], mudados)
        assert all(p["files"] for p in reduzidas)
        assert all("estabelecimentos" in p["partition_id"] for p in reduzidas)


class TestEvolucaoSchema:
    def test_campo_novo_compativel(self):
        old = ["a", "b"]
        new = ["a", "b", "c"]
        validate_schema_evolution(old, new)  # não levanta

    def test_campo_removido_incompativel(self):
        old = ["a", "b"]
        new = ["a"]
        with pytest.raises(SchemaEvolutionError) as exc:
            validate_schema_evolution(old, new)
        assert exc.value.code == "SCHEMA_INCOMPATIBLE"

    def test_tipo_alterado_incompativel(self):
        old = ["a: text", "b: int"]
        new = ["a: text", "b: text"]
        with pytest.raises(SchemaEvolutionError) as exc:
            validate_schema_evolution(old, new)
        assert exc.value.code == "SCHEMA_INCOMPATIBLE"
