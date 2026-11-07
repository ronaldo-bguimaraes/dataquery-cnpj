"""Plano de manutenção Silver: compaction/expiração/limpeza observáveis (FR-036/048, T046).

Decisões: research.md D10 — PyIceberg `table.maintenance.compact()` quando
disponível; Spark 4 one-off como fallback documentado no runbook.
"""

from __future__ import annotations

DEFAULT_RETENTION_VERSIONS = 2


def build_maintenance_plan(
    tables: list[str],
    *,
    retention_versions: int = DEFAULT_RETENTION_VERSIONS,
) -> dict:
    """Plano declarativo de manutenção (compact → expire → orphan)."""

    if retention_versions < 1:
        raise ValueError("retention_versions precisa manter ao menos 1 versão (FR-047)")
    return {
        "tabelas": list(tables),
        "passos": [
            {"acao": "compact", "tabelas": list(tables), "parametros": {"target_file_size_bytes": 2**29}},
            {
                "acao": "expire_snapshots",
                "tabelas": list(tables),
                "parametros": {"manter_ultimas": retention_versions},
            },
            {"acao": "delete_orphan_files", "tabelas": list(tables), "parametros": {"older_than_hours": 24}},
        ],
        "passo_ativo_preservado": True,
    }


def validate_maintenance_safety(plan: dict, *, active_snapshot: str) -> bool:
    """Versão ativa não pode ser compactada/expirada (FR-048, Edge 'retention falha')."""

    expire = next((p for p in plan["passos"] if p["acao"] == "expire_snapshots"), None)
    if expire is None:
        return True
    if expire["parametros"].get("manter_ultimas", 0) < 1:
        return False
    if expire["parametros"].get("preservar_snapshot_ativo") is False and active_snapshot:
        return False
    return plan.get("passo_ativo_preservado", False) is True
