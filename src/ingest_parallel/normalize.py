"""Transformação Bronze → lotes Arrow tipados em streaming (FR-005, FR-033/034).

Reusa os generators já validados do loader atual (src/query/ingest/transform.py)
e adiciona streaming por lote com *BatchBudget* (nenhum dataset inteiro em memória).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import pyarrow as pa

from src.ingest_parallel.contracts import DATASET_REGISTRY
from src.query.ingest.transform import (
    TransformStats,
    empresas_rows,
    estabelecimentos_rows,
    simples_rows,
    socios_rows,
)

DATASET_MODEL = {
    "empresas": {"pattern": "Empresas*.parquet"},
    "estabelecimentos": {"pattern": "Estabelecimentos*.parquet"},
    "socios": {"pattern": "Socios*.parquet"},
    "simples": {"pattern": "Simples.parquet"},
}

COMPETENCIA_FIELD = "competencia"
LOAD_ID_FIELD = "load_id"


class UnknownDataset(ValueError):
    """Dataset fora do Dataset Identity Registry."""


@dataclass(frozen=True)
class BatchBudget:
    """Limites por lote: batch cap acumulado e linhas máximas por batch (FR-034)."""

    max_rows: int = 10_000
    max_total_rows: int = 0  # 0 = sem limite total (uso fechado pelo partitioning)

    def __post_init__(self) -> None:
        if self.max_rows < 1:
            raise ValueError("max_rows deve ser >= 1")


def _rows(dataset: str, bronze_dir: Path, competencia: str, stats: TransformStats) -> Iterator[dict[str, Any]]:
    """Itera linhas normalizadas do generator existente, adicionando metadados de carga."""

    if dataset not in DATASET_MODEL or dataset not in DATASET_REGISTRY:
        raise UnknownDataset(f"dataset desconhecido ou sem identidade: {dataset}")
    pattern = DATASET_MODEL[dataset]["pattern"]
    paths = sorted(bronze_dir.glob(pattern))
    if not paths:
        raise UnknownDataset(f"nenhum arquivo Bronze para {dataset} em {bronze_dir}")

    if dataset == "empresas":
        yield from empresas_rows(paths, stats)
    elif dataset == "estabelecimentos":
        for row, _cnaes in estabelecimentos_rows(paths, stats):
            yield row
    elif dataset == "socios":
        yield from socios_rows(paths, stats)
    elif dataset == "simples":
        yield from simples_rows(paths[0], competencia, stats)


def normalize_bronze(
    bronze_dir: str | Path,
    dataset: str,
    *,
    competencia: str = "2026-10",
    load_id: str = "",
    budget: BatchBudget | None = None,
) -> Iterator[pa.RecordBatch]:
    """Produz *RecordBatches* Arrow tipados e limitados por orçamento."""

    budget = budget or BatchBudget()
    partida = Path(bronze_dir)
    stats = TransformStats()
    rows = _rows(dataset, partida, competencia, stats)
    rows = (
        {**row, COMPETENCIA_FIELD: competencia, LOAD_ID_FIELD: load_id}
        for row in rows
    )
    yield from _batches(dataset, rows, budget)


def _batches(dataset: str, rows: Iterator[dict[str, Any]], budget: BatchBudget) -> Iterator[pa.RecordBatch]:
    produced = 0
    while True:
        chunk: list[dict[str, Any]] = []
        for row in rows:
            chunk.append(row)
            if len(chunk) >= budget.max_rows:
                break
        if not chunk:
            break
        produced += len(chunk)
        if budget.max_total_rows and produced > budget.max_total_rows:
            raise RuntimeError(
                f"orçamento estourado para {dataset}: {produced} > {budget.max_total_rows} "  # noqa: TRY003
            )
        yield pa.RecordBatch.from_pylist(chunk)
        if len(chunk) < budget.max_rows:
            break
