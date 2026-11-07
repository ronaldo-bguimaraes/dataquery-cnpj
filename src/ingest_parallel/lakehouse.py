"""Contrato da camada Silver Iceberg (contracts/lakehouse_silver.md)."""

from __future__ import annotations

#: Propriedades sealed de toda tabela Silver (contrato §3).
SEALED_TABLE_PROPERTIES = {
    "write.target-file-size-bytes": "536870912",  # 512 MB
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
    "format-version": "2",
}

#: Colunas de partição por tabela (contrato §2) — `competencia` é a chave do incremental.
SILVER_PARTITIONING = {
    "silver_empresas": ("competencia",),
    "silver_estabelecimentos": ("competencia", "uf"),
    "silver_socios": ("competencia",),
    "silver_simples": ("competencia",),
}

#: Branches de publicação (padrão WAP: branch de carga → fast-forward para main ).
PUBLISH_BRANCH = "main"
COLUMNS_OF_CARGA = ("competencia", "load_id", "_source_file_hash", "_ingested_at")


def load_branch_name(competencia: str) -> str:
    """Nome da branch de carga da competência — nunca a `main`."""

    return f"load_{competencia}"
