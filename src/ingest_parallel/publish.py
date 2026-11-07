"""Planejamento da publicação atômica do serving (contracts/serving_publication.md)."""

from __future__ import annotations

from dataclasses import dataclass


def staging_table_name(table: str, load_id: str) -> str:
    """Nome determinístico da tabela de staging por carga (contrato §3.1)."""

    return f"staging_{table}_{load_id}"


@dataclass(frozen=True)
class SwapStep:
    """Uma troca atômica: staging → base, mesma transação."""

    staging: str
    target: str


@dataclass(frozen=True)
class PublicationState:
    """Estado do plano de publicação; o swap é executado por universo único."""

    load_id: str
    status: str
    previous_active: str
    rollback_to: str
    lock_timeout: str
    swaps: tuple[SwapStep, ...]


def swap_plan(
    *,
    load_id: str,
    tables: tuple[str, ...] | list[str],
    previous_active: str,
    lock_timeout: str = "2s",
) -> PublicationState:
    """Monta plano de swap: staging → target para cada tabela e rotação de metadata.

    O swap real é coordenado no `publish()`; no plano os nomes são determinísticos
    e nunca colidem com as tabelas ativas, garantindo leitura da versão anterior (FR-010).
    """

    steps: list[SwapStep] = []
    for table in tables:
        if table == "query_metadata":
            steps.append(SwapStep(staging=staging_table_name(load_id, load_id), target=table))
            continue
        steps.append(SwapStep(staging=staging_table_name(table, load_id), target=table))
    return PublicationState(
        load_id=load_id,
        status="planned",
        previous_active=previous_active,
        rollback_to=previous_active,
        lock_timeout=lock_timeout,
        swaps=tuple(steps),
    )
