"""T007 — Contrato do manifesto da fonte (contracts/source_manifest.md)."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.ingest_parallel.inventory import (
    ManifestError,
    build_source_manifest,
    validate_bronze_inventory,
)


def criar_arquivo(path: Path, name: str, content: bytes) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    target = path / name
    target.write_bytes(content)
    return target


def criar_parquet(path: Path, name: str, rows: int = 1) -> Path:
    """Cria um Parquet válido com `rows` linhas dummy."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.mkdir(parents=True, exist_ok=True)
    target = path / name
    table = pa.table({"chave": pa.array([str(i) for i in range(rows)], pa.string())})
    pq.write_table(table, target)
    return target


@pytest.fixture(name="bronze_minimo")
def bronze_minimo_fixture(tmp_path):
    """Um Bronze sintético com apenas 1 arquivo (fora do contrato RFB de 37)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    bronze = tmp_path / "bronze" / "2026-10"
    bronze.mkdir(parents=True, exist_ok=True)
    criar_parquet(bronze, "Empresas0.parquet")
    return bronze


class TestManifestoConstruido:
    def test_campos_obrigatorios_presentes(self, bronze_minimo):
        manifest = build_source_manifest(bronze_minimo, "2026-10")
        assert manifest.competencia == "2026-10"
        assert manifest.source_count == 1
        assert manifest.total_rows_expected >= 0
        assert len(manifest.data_version_id) == 64
        assert manifest.code_version
        assert manifest.files[0].file_name == "Empresas0.parquet"
        assert manifest.files[0].size_bytes == (bronze_minimo / "Empresas0.parquet").stat().st_size
        assert len(manifest.files[0].sha256) == 64
        assert manifest.total_rows_expected == 1  # 1 linha no parquet sintético

    def test_hash_do_arquivo_sha256(self, bronze_minimo):
        import hashlib

        manifest = build_source_manifest(bronze_minimo, "2026-10")
        deve = hashlib.sha256((bronze_minimo / "Empresas0.parquet").read_bytes()).hexdigest()
        assert manifest.files[0].sha256 == deve

    def test_data_version_id_deterministico(self, bronze_minimo):
        primeiro = build_source_manifest(bronze_minimo, "2026-10")
        segundo = build_source_manifest(bronze_minimo, "2026-10")
        assert primeiro.data_version_id == segundo.data_version_id

    def test_data_version_id_muda_com_conteudo(self, bronze_minimo):
        original = build_source_manifest(bronze_minimo, "2026-10")
        criar_parquet(bronze_minimo, "Empresas0.parquet", rows=7)
        novo = build_source_manifest(bronze_minimo, "2026-10")
        assert novo.data_version_id != original.data_version_id

    def test_load_id_derivado_da_versao_da_fonte(self, bronze_minimo):
        manifest = build_source_manifest(bronze_minimo, "2026-10")
        assert manifest.load_id == manifest.competencia + "-" + manifest.data_version_id[:8]

    def test_arquivo_renomeado_nao_sobrescreve_por_nome_unico(self, bronze_minimo):
        criar_parquet(bronze_minimo, "Empresas1.parquet")
        criar_parquet(bronze_minimo, "Zeta.parquet")
        criar_parquet(bronze_minimo, "Alpha.parquet")
        manifest = build_source_manifest(bronze_minimo, "2026-10")
        names = [entry.file_name for entry in manifest.files]
        assert len(names) == len(set(names))
        assert names == sorted(names)


class TestValidacaoContrato:
    def test_competencia_invalida(self, bronze_minimo):
        with pytest.raises(ManifestError) as exc:
            build_source_manifest(bronze_minimo, "10-2026")
        assert exc.value.code == "COMPETENCIA_INVALID"

    def test_fonte_vazia(self, tmp_path):
        bronze = tmp_path / "bronze" / "2026-10"
        bronze.mkdir(parents=True)
        with pytest.raises(ManifestError) as exc:
            build_source_manifest(bronze, "2026-10")
        assert exc.value.code == "SOURCE_COUNT_EMPTY"

    def test_arquivo_ausente_no_validate(self, bronze_minimo):
        manifest = build_source_manifest(bronze_minimo, "2026-10")
        storage_ausente = bronze_minimo.parent / "vazio"
        storage_ausente.mkdir()
        with pytest.raises(ManifestError) as exc:
            validate_bronze_inventory(manifest, storage_ausente)
        assert exc.value.code == "SOURCE_FILE_MISSING"

    def test_hash_divergente(self, bronze_minimo):
        manifest = build_source_manifest(bronze_minimo, "2026-10")
        # arquivo mutado após o manifesto: mesmo storage, hash diferente
        (bronze_minimo / "Empresas0.parquet").write_bytes(b"hash-diferente")
        with pytest.raises(ManifestError) as exc:
            validate_bronze_inventory(manifest, bronze_minimo)
        assert exc.value.code == "SOURCE_HASH_MISMATCH"


class TestContratoRfb37:
    def test_source_count_divergente_rejeita_para_rfb(self, bronze_minimo):
        """Validar contratos/source_manifest.md §3.2: source_count != 37 impede carga."""

        with pytest.raises(ManifestError) as exc:
            validate_bronze_inventory(build_source_manifest(bronze_minimo, "2026-10"), bronze_minimo, valido=37)
        assert exc.value.code == "SOURCE_COUNT_DIVERGENT"
