"""T053 — Edge cases do manifesto (arquivo vazio, hash duplicado, competência sem arquivos)."""

from __future__ import annotations

import pytest

from src.ingest_parallel.inventory import (
    ManifestError,
    SourceManifest,
    build_source_manifest,
    validate_bronze_inventory,
)


class TestEdgeCasesManifesto:
    def test_competencia_sem_arquivos_e_falha_de_qualidade_nao_publica(self, tmp_path):
        bronze = tmp_path / "bronze" / "2026-10"
        bronze.mkdir(parents=True)
        with pytest.raises(ManifestError) as exc:
            build_source_manifest(bronze, "2026-10")
        assert exc.value.code == "SOURCE_COUNT_EMPTY"

    def test_arquivo_vazio_ainda_vai_no_manifesto_com_rows_zero(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        bronze = tmp_path / "bronze" / "2026-10"
        bronze.mkdir(parents=True)
        table = pa.table({"chave": pa.array([], pa.string())})
        pq.write_table(table, bronze / "Empresas0.parquet")

        manifest = build_source_manifest(bronze, "2026-10")
        assert manifest.source_count == 1
        assert manifest.total_rows_expected == 0

    def test_hash_duplicado_entre_arquivos_nao_era_silencioso(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        bronze = tmp_path / "bronze" / "2026-10"
        bronze.mkdir(parents=True)
        table = pa.table({"chave": pa.array(["1"], pa.string())})
        pq.write_table(table, bronze / "Empresas0.parquet")
        pq.write_table(table, bronze / "Empresas1.parquet")  # mesmo conteúdo → mesmo hash

        manifest = build_source_manifest(bronze, "2026-10")
        hashes = [entry.sha256 for entry in manifest.files]
        assert len(hashes) == len(manifest.files)  # entradas únicas
        assert len(set(hashes)) < len(hashes)  # mas os hashes duplicam -- contrato assume detectável

    def test_remocao_de_arquivo_gera_outra_versao(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        bronze = tmp_path / "bronze" / "2026-10"
        bronze.mkdir(parents=True)
        table = pa.table({"chave": pa.array(["1"], pa.string())})
        for name in ("Empresas0.parquet", "Empresas1.parquet"):
            pq.write_table(table, bronze / name)
        original = build_source_manifest(bronze, "2026-10").data_version_id
        (bronze / "Empresas1.parquet").unlink()
        depois = build_source_manifest(bronze, "2026-10").data_version_id
        assert depois != original

    def test_diretorio_inexistente_falha_explicita(self, tmp_path):
        with pytest.raises(ManifestError) as exc:
            build_source_manifest(tmp_path / "sem-atual", "2026-10")
        assert exc.value.code == "SOURCE_FILE_MISSING"


_ = (SourceManifest, validate_bronze_inventory, pytest)  # uso declarativo de imports
