"""Commercial segmentation based on the CNAE 2.0 division.

The catalogue is a product decision versioned in code, not a Receita Federal
dataset. Each establishment is classified by the first two digits of its
principal CNAE (the division). Unknown or missing CNAEs fall back to
``NAO_CLASSIFICADO``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

NAO_CLASSIFICADO = "NAO_CLASSIFICADO"


@dataclass(frozen=True)
class Segmento:
    codigo: str
    nome: str


SEGMENTOS: tuple[Segmento, ...] = (
    Segmento("AGRO", "Agronegócio e extrativismo"),
    Segmento("INDUSTRIA", "Indústria e transformação"),
    Segmento("UTILIDADES", "Energia, água e saneamento"),
    Segmento("CONSTRUCAO", "Construção"),
    Segmento("VEICULOS", "Veículos e peças"),
    Segmento("ATACADO", "Distribuição e atacado"),
    Segmento("VAREJO", "Comércio varejista"),
    Segmento("TRANSPORTE", "Transporte e logística"),
    Segmento("ALOJAMENTO", "Alojamento e alimentação"),
    Segmento("TECNOLOGIA", "Informação e comunicação"),
    Segmento("FINANCEIRO", "Financeiro e seguros"),
    Segmento("IMOBILIARIO", "Imobiliário"),
    Segmento("SERVICOS_PROFISSIONAIS", "Serviços profissionais e técnicos"),
    Segmento("SERVICOS_APOIO", "Serviços administrativos e apoio"),
    Segmento("ADMIN_PUBLICA", "Administração pública"),
    Segmento("EDUCACAO", "Educação"),
    Segmento("SAUDE", "Saúde e assistência social"),
    Segmento("CULTURA", "Artes, cultura e recreação"),
    Segmento("OUTROS_SERVICOS", "Outros serviços"),
    Segmento(NAO_CLASSIFICADO, "Não classificado"),
)

SEGMENTOS_POR_CODIGO: dict[str, Segmento] = {item.codigo: item for item in SEGMENTOS}

#: CNAE division (two digits) -> segment code.
DIVISOES: dict[str, str] = {}


def _range(start: int, end: int, codigo: str) -> None:
    for division in range(start, end + 1):
        DIVISOES[f"{division:02d}"] = codigo


_range(1, 3, "AGRO")
_range(5, 9, "AGRO")
_range(10, 33, "INDUSTRIA")
_range(35, 39, "UTILIDADES")
_range(41, 43, "CONSTRUCAO")
_range(45, 45, "VEICULOS")
_range(46, 46, "ATACADO")
_range(47, 47, "VAREJO")
_range(49, 53, "TRANSPORTE")
_range(55, 56, "ALOJAMENTO")
_range(58, 63, "TECNOLOGIA")
_range(64, 66, "FINANCEIRO")
_range(68, 68, "IMOBILIARIO")
_range(69, 75, "SERVICOS_PROFISSIONAIS")
_range(77, 82, "SERVICOS_APOIO")
_range(84, 84, "ADMIN_PUBLICA")
_range(85, 85, "EDUCACAO")
_range(86, 88, "SAUDE")
_range(90, 93, "CULTURA")
_range(94, 96, "OUTROS_SERVICOS")
_range(97, 99, NAO_CLASSIFICADO)


def divisao_cnae(cnae: str | None) -> str | None:
    """Return the two-digit CNAE division, or ``None`` when unavailable."""

    digits = re.sub(r"\D", "", str(cnae or ""))
    if len(digits) < 2:
        return None
    return digits[:2]


def segmento_codigo(cnae: str | None) -> str:
    """Classify a CNAE into a commercial segment code."""

    divisao = divisao_cnae(cnae)
    if divisao is None:
        return NAO_CLASSIFICADO
    return DIVISOES.get(divisao, NAO_CLASSIFICADO)


def segmento_nome(codigo: str) -> str:
    """Return the display name for a segment code."""

    segmento = SEGMENTOS_POR_CODIGO.get(codigo)
    return segmento.nome if segmento else SEGMENTOS_POR_CODIGO[NAO_CLASSIFICADO].nome


def segmento_para_cnae(cnae: str | None) -> Segmento:
    return SEGMENTOS_POR_CODIGO[segmento_codigo(cnae)]


def is_segmento_valido(codigo: str | None) -> bool:
    return bool(codigo) and codigo in SEGMENTOS_POR_CODIGO


def segmento_rows() -> list[tuple[str, str]]:
    return [(item.codigo, item.nome) for item in SEGMENTOS]


def divisao_rows() -> list[tuple[str, str]]:
    return sorted(DIVISOES.items())
