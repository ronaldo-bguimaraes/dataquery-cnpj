"""Particionamento determinístico do manifesto em 64–128 partições lógicas (FR-018).

Cada partição é `{table}-{index:02d}` com subconjunto disjunto de arquivos;
a soma de expected_rows do manifesto é preservada exatamente.
"""

from __future__ import annotations

import re

from src.ingest_parallel.inventory import SourceManifest

MAX_PARTITIONS = 128
MIN_PARTITIONS_PER_TABLE = 1

_TABLE_PATTERNS = {
    "empresas": re.compile(r"^Empresas(\d+)\.parquet$"),
    "estabelecimentos": re.compile(r"^Estabelecimentos(\d+)\.parquet$"),
    "socios": re.compile(r"^Socios(\d+)\.parquet$"),
    "simples": re.compile(r"^Simples\.parquet$"),
}


def _table_of(file_name: str) -> str | None:
    for table, pattern in _TABLE_PATTERNS.items():
        if pattern.match(file_name):
            return table
    return None


def build_partitions(
    manifest: SourceManifest,
    *,
    target_rows_per_partition: int = 500_000,
) -> list[dict]:
    """Divide o manifesto em partições independentes de propriedade determinística.

    Determinismo: mesmo manifesto + mesmo target → mesma lista de partições,
    em qualquer execução (FR-014/018); a união cobre 100% dos arquivos.
    """

    if target_rows_per_partition < 1:
        raise ValueError("target_rows_per_partition deve ser >= 1")

    grouped: dict[str, list] = {}
    for entry in manifest.files:
        table = _table_of(entry.file_name)
        if table is not None:
            grouped.setdefault(table, []).append(entry)

    partitions: list[dict] = []
    for table in sorted(grouped):
        files = grouped[table]
        total = sum(entry.row_count for entry in files)
        count = max(MIN_PARTITIONS_PER_TABLE, (total + target_rows_per_partition - 1) // target_rows_per_partition)
        count = min(count, MAX_PARTITIONS)
        # Distribuição determinística por faixas contíguas de arquivos ordenados
        buckets: list[list] = [[] for _ in range(count)]
        expected = [0] * count
        idx = 0
        for entry in files:
            if total and expected[idx] / max(target_rows_per_partition, 1) >= 1 and idx < count - 1:
                idx += 1
            buckets[idx].append(entry)
            expected[idx] += entry.row_count
        for index, bucket in enumerate(buckets):
            if not bucket:
                continue
            partitions.append(
                {
                    "partition_id": f"{table}-{index:02d}",
                    "table": table,
                    "files": bucket,
                    "expected_rows": expected[index],
                }
            )
    return partitions
