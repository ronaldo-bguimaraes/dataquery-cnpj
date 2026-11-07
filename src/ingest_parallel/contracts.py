"""Dataset Identity Registry — identidade e unicidade de cada dataset (FR-003/004).

Fontes: data-model.md §5.6 e contracts/lakehouse_silver.md §5.
"""

from __future__ import annotations

from dataclasses import dataclass

DatasetName = str


@dataclass(frozen=True)
class DatasetIdentity:
    """Identidade declarada de um dataset."""

    dataset_name: str
    business_keys: tuple[str, ...]
    uniqueness_rule: str
    dedup_policy: str  # keep_first | keep_last | merge | reject
    pii_policy: str  # mask | omit | preserve
    active: bool = True


#: Registro único de identidade por dataset (Dataset Identity Registry).
DATASET_REGISTRY: dict[DatasetName, DatasetIdentity] = {
    "empresas": DatasetIdentity(
        dataset_name="empresas",
        business_keys=("cnpj_basico",),
        uniqueness_rule=(
            "cnpj_basico única por competência; uma linha por CNPJ básico (RAIZ/CNPJ)"
        ),
        dedup_policy="keep_last",
        pii_policy="preserve",
    ),
    "estabelecimentos": DatasetIdentity(
        dataset_name="estabelecimentos",
        business_keys=("cnpj_completo",),
        uniqueness_rule=(
            "cnpj_completo única por competência; uma linha por estabelecimento (CNPJ completo)"
        ),
        dedup_policy="keep_last",
        pii_policy="preserve",
    ),
    "socios": DatasetIdentity(
        dataset_name="socios",
        business_keys=(
            "cnpj_basico",
            "identificador_socio",
            "documento_socio_mascarado",
            "nome_socio",
            "qualificacao_socio_codigo",
            "data_entrada_sociedade",
        ),
        uniqueness_rule=(
            "chave composta do sócio; CPF/CNPJ sempre mascarado "
            "(documento_socio_mascarado entra na identidade já mascarado)"
        ),
        dedup_policy="keep_last",
        pii_policy="mask",
    ),
    "simples": DatasetIdentity(
        dataset_name="simples",
        business_keys=("cnpj_basico",),
        uniqueness_rule="cnpj_basico única por competência; opção atual Simples/MEI",
        dedup_policy="keep_last",
        pii_policy="preserve",
    ),
}


def identity_key(dataset: DatasetName, row: dict) -> tuple:  # noqa: ANN001
    """Chave de negócio determinística de um registro de um dataset."""

    identity = DATASET_REGISTRY[dataset]
    return tuple(row.get(column) for column in identity.business_keys)


def dedupe_rows(dataset: DatasetName, rows) -> tuple[list[dict], int]:  # noqa: ANN001
    """Aplica a `dedup_policy` do dataset (`keep_last`) e devolve (rows, duplicatas)."""

    identity = DATASET_REGISTRY[dataset]
    if identity.dedup_policy != "keep_last":  # pragma: no cover - política única no MVP
        raise NotImplementedError(identity.dedup_policy)
    seen: dict[tuple, int] = {}
    duplicates = 0
    ordered: list[tuple[tuple, dict]] = []
    for row in rows:
        key = identity_key(dataset, row)
        if key in seen:
            duplicates += 1
            ordered[seen[key]] = (key, row)
        else:
            seen[key] = len(ordered)
            ordered.append((key, row))
    return [row for _key, row in ordered], duplicates
