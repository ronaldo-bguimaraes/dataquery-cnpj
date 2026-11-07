"""Validadores de qualidade pré-publicação (FR-037, FR-012, contracts/serving_publication.md §3.2)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.ingest_parallel.contracts import DATASET_REGISTRY, identity_key

DATASETS = ("empresas", "estabelecimentos", "socios", "simples")


class QualityError(ValueError):
    """Qualidade insuficiente para publicar; `code` segue contracts/serving_publication.md §7."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code


@dataclass
class DatasetCounts:
    """Dados em staging por dataset (abastração sobre linhas para validação)."""

    competencia: str
    load_id: str
    empresas: list[dict[str, Any]] = field(default_factory=list)
    estabelecimentos: list[dict[str, Any]] = field(default_factory=list)
    socios: list[dict[str, Any]] = field(default_factory=list)
    simples: list[dict[str, Any]] = field(default_factory=list)

    def dataset(self, name: str) -> list[dict[str, Any]]:
        return getattr(self, name)


def _check_unique(dataset: DatasetCounts, name: str) -> list[tuple]:
    """Chaves de negócio repetidas (STAGING_UNIQUE_DUP) por business key."""

    seen: dict[tuple, int] = {}
    duplicates: list[tuple] = []
    for row in dataset.dataset(name):
        key = identity_key(name, row)
        if key in seen:
            if key not in duplicates:
                duplicates.append(key)
        else:
            seen[key] = 1
    return duplicates


def _check_fk(dataset: DatasetCounts) -> list[tuple]:
    """Referências órfãs estabelecimentos/socios/simples → empresas."""

    empresas = {row["cnpj_basico"] for row in dataset.empresas}
    orfas: list[tuple] = []
    for name, fk_column in (
        ("estabelecimentos", "cnpj_basico"),
        ("socios", "cnpj_basico"),
        ("simples", "cnpj_basico"),
    ):
        for row in dataset.dataset(name):
            if row.get(fk_column) not in empresas:
                orfas.append((name, row.get(fk_column)))
    return orfas


def _check_schema(dataset: DatasetCounts, obrigatorio: dict[str, list[str]]) -> list[tuple[str, str]]:
    missing: list[tuple[str, str]] = []
    for name, columns in obrigatorio.items():
        for row in dataset.dataset(name):
            for column in columns:
                if column not in row:
                    missing.append((name, column))
    return missing


class SchemaEvolutionError(ValueError):
    """Evolução de schema incompatível deve falhar antes da publicação (FR-006, SC-010)."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code


def validate_schema_evolution(
    colunas_anteriores: list[str],
    colunas_novas: list[str],
    *,
    tipos_anteriores: dict[str, str] | None = None,
    tipos_novas: dict[str, str] | None = None,
) -> None:
    """Campo novo é compatível; campo removido ou tipo alterado não é (FR-006)."""

    removidos = [coluna for coluna in colunas_anteriores if coluna not in colunas_novas and not coluna.endswith(":")]
    if removidos:
        raise SchemaEvolutionError(
            "SCHEMA_INCOMPATIBLE", f"colunas removidas: {sorted(removidos)}"
        )
    if tipos_anteriores is not None and tipos_novas is not None:
        changed = {
            column
            for column in set(tipos_anteriores) & set(tipos_novas)
            if tipos_anteriores[column] != tipos_novas[column]
        }
        if changed:
            raise SchemaEvolutionError(
                "SCHEMA_INCOMPATIBLE", f"tipos alterados: {sorted(changed)}"
            )


def run_quality_checks(
    dataset: DatasetCounts,
    *,
    total_esperado: dict[str, int],
    schema_obrigatorio: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Executa unicidade → FK → contagens → schema, nessa ordem auditável (FR-037)."""

    report = {"unicidade": {name: [] for name in DATASETS}, "fk_orfas": [], "contagens_ok": False}

    for name in DATASETS:
        duplicates = _check_unique(dataset, name)
        if duplicates:
            raise QualityError(
                "STAGING_UNIQUE_DUP",
                f"chave de negócio duplicada em {name}: {len(duplicates)} exemplo(s); "
                "não expose valores (contados no ledger de rejeições)",
            )
        report["unicidade"][name] = duplicates

    orfas = _check_fk(dataset)
    if orfas:
        raise QualityError("STAGING_FK_ORPHAN", f"{len(orfas)} referência(s) órfã(s)")
    report["fk_orfas"] = orfas

    divergentes: list[str] = []
    for name in DATASETS:
        esperado = total_esperado.get(name)
        atual = len(dataset.dataset(name))
        if esperado is not None and atual != esperado:
            divergentes.append(f"{name}: {atual} != {esperado}")
    if divergentes:
        raise QualityError(
            "STAGING_COUNT_MISMATCH", "contagens divergentes: " + ", ".join(divergentes)
        )
    report["contagens_ok"] = True

    if schema_obrigatorio:
        missing = _check_schema(dataset, schema_obrigatorio)
        if missing:
            raise QualityError(
                "STAGING_SCHEMA_MISMATCH",
                f"colunas obrigatórias ausentes: {sorted(set(missing))}",
            )

    return report
