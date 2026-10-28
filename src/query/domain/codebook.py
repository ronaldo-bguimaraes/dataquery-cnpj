"""Official codebooks that are not present as lookup files in the RFB Parquets."""

from __future__ import annotations

SITUACOES = {
    "01": "Nula",
    "02": "Ativa",
    "03": "Suspensa",
    "04": "Inapta",
    "05": "Ativa Não Regular",
    "08": "Baixada",
}

PORTES = {
    "00": "Não informado",
    "01": "Microempresa",
    "03": "Empresa de Pequeno Porte",
    "05": "Demais empresas",
}

MATRIZ_FILIAL = {
    "1": "Matriz",
    "2": "Filial",
}


def _normalize_code(code: object) -> str:
    text = str(code or "").strip()
    if text.isdigit():
        return text.zfill(2)
    return text


def _label(catalog: dict[str, str], code: object) -> str:
    normalized = _normalize_code(code)
    return catalog.get(normalized, normalized)


def descricao_situacao(code: object) -> str:
    return _label(SITUACOES, code)


def descricao_porte(code: object) -> str:
    return _label(PORTES, code)


def descricao_matriz_filial(code: object) -> str:
    normalized = str(code or "").strip()
    return MATRIZ_FILIAL.get(normalized, normalized)
