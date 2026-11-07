"""Classificação incremental de mudanças da fonte (FR-027..032, T043/044)."""

from __future__ import annotations

CHANGE_REPORT_KEYS = ("novos", "modificados", "inalterados", "removidos")


def classify_files(
    anterior: dict[str, str],
    atual: dict[str, str],
) -> dict[str, list[str]]:
    """Classifica arquivos entre duas versões do Source Manifest por hash.

    A semântica da fonte não tem CDC: a classificação é no nível de arquivo
    (spec.md §Assumptions). Registros por arquivo são tratados conforme contrato.
    """

    report: dict[str, list[str]] = {key: [] for key in CHANGE_REPORT_KEYS}
    for name in sorted(set(anterior) | set(atual)):
        if name not in anterior:
            report["novos"].append(name)
        elif name not in atual:
            report["removidos"].append(name)
        elif anterior[name] != atual[name]:
            report["modificados"].append(name)
        else:
            report["inalterados"].append(name)
    return report


def partitions_forte_mudancas(
    partitions: list[dict],
    manifest_files: list[dict],
    mudados: list[str],
) -> list[dict]:
    """Reduz as partições para arquivo(s) com mudança (FR-028/032: evitar reescrita)."""

    set_mudados = set(mudados)
    reduzidas: list[dict] = []
    for partition in partitions:
        files = [f["file_name"] for f in partition.get("files", [])]
        if any(name in set_mudados for name in files):
            reduzidas.append(partition)
    return reduzidas


def registre_decisao_carga_completa(
    report: dict[str, list[str]],
    *,
    justificativa: str,
) -> dict[str, str]:
    """Carga completa é decisão explícita e registrada (FR-029)."""

    # Fonte sem hash comparável (ex.: raw substituído por inteiro) força full load
    return {"decisao": "full_load", "justificativa": justificativa}
