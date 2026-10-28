"""View-model helpers for the server-rendered pages."""

from __future__ import annotations

import math
from datetime import date
from typing import Any
from urllib.parse import urlencode

from src.query.domain.masking import redact_cpf_patterns
from src.query.domain.normalization import format_cnpj, format_date
from src.query.web.filters import EstabelecimentoFiltros


def listing_rows(
    rows: list[dict[str, Any]],
    filtros: EstabelecimentoFiltros | None = None,
) -> list[dict[str, Any]]:
    result = []
    params = filtros.as_query_params() if filtros else {}
    for row in rows:
        detail_url = f"/estabelecimentos/{row.get('cnpj_completo')}"
        if params:
            detail_url += f"?{urlencode(params)}"
        result.append(
            {
                **row,
                "detail_url": detail_url,
                "cnpj_formatado": format_cnpj(row.get("cnpj_completo")),
                "razao_social": redact_cpf_patterns(row.get("razao_social")),
                "nome_fantasia": redact_cpf_patterns(row.get("nome_fantasia")),
                "data_inicio_atividade_formatada": format_date(
                    row.get("data_inicio_atividade")
                ),
                "matriz_filial": row.get("matriz_filial_descricao")
                or row.get("identificador_matriz_filial"),
                "simples_label": "Sim" if row.get("optante_simples") else "Não",
                "mei_label": "Sim" if row.get("optante_mei") else "Não",
            }
        )
    return result


def _url(filtros: EstabelecimentoFiltros, page: int) -> str:
    params = filtros.as_query_params(include_page=False)
    if page > 1:
        params["pagina"] = str(page)
    return "/estabelecimentos" + (f"?{urlencode(params)}" if params else "")


def pagination(
    filtros: EstabelecimentoFiltros,
    total: int,
    page_size: int,
) -> dict[str, Any]:
    total_pages = math.ceil(total / page_size) if total else 0
    page = filtros.pagina
    return {
        "page": page,
        "total": total,
        "total_pages": total_pages,
        "page_size": page_size,
        "has_previous": page > 1,
        "has_next": total_pages > 0 and page < total_pages,
        "previous_url": _url(filtros, page - 1) if page > 1 else None,
        "next_url": _url(filtros, page + 1) if total_pages > page else None,
        "first_url": _url(filtros, 1),
    }


def clear_url() -> str:
    return "/estabelecimentos"


def back_url(filtros: EstabelecimentoFiltros) -> str:
    return _url(filtros, filtros.pagina)


def today() -> date:
    return date.today()


def _money(value: Any) -> str:
    if value is None:
        return "Não informado"
    try:
        return f"{float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return str(value)


def detail_view(
    estabelecimento: dict[str, Any],
    cnaes: list[dict[str, Any]],
    socios: list[dict[str, Any]],
    simples: dict[str, Any] | None,
) -> dict[str, Any]:
    data = {
        **estabelecimento,
        "cnpj_formatado": format_cnpj(estabelecimento.get("cnpj_completo")),
        "razao_social": redact_cpf_patterns(estabelecimento.get("razao_social")),
        "nome_fantasia": redact_cpf_patterns(estabelecimento.get("nome_fantasia")),
        "data_situacao_cadastral_formatada": format_date(
            estabelecimento.get("data_situacao_cadastral")
        ),
        "data_inicio_atividade_formatada": format_date(
            estabelecimento.get("data_inicio_atividade")
        ),
        "data_situacao_especial_formatada": format_date(
            estabelecimento.get("data_situacao_especial")
        ),
        "capital_social_formatado": _money(estabelecimento.get("capital_social")),
        "cnaes": cnaes,
        "socios": [
            {
                **socio,
                "nome_socio": redact_cpf_patterns(socio.get("nome_socio")),
                "nome_representante": redact_cpf_patterns(
                    socio.get("nome_representante")
                ),
                "data_entrada_formatada": format_date(
                    socio.get("data_entrada_sociedade")
                ),
            }
            for socio in socios
        ],
        "simples": None,
    }
    if simples:
        data["simples"] = {
            **simples,
            "data_opcao_simples_formatada": format_date(simples.get("data_opcao_simples")),
            "data_exclusao_simples_formatada": format_date(
                simples.get("data_exclusao_simples")
            ),
            "data_opcao_mei_formatada": format_date(simples.get("data_opcao_mei")),
            "data_exclusao_mei_formatada": format_date(
                simples.get("data_exclusao_mei")
            ),
            "simples_label": "Sim" if simples.get("optante_simples") else "Não",
            "mei_label": "Sim" if simples.get("optante_mei") else "Não",
        }
    return data
