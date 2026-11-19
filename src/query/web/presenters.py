"""View-model helpers for the server-rendered pages."""

from __future__ import annotations

import math
from datetime import date
from typing import Any
from urllib.parse import urlencode

from src.query.domain.indicadores import idade_em_anos, tem_contato
from src.query.domain.masking import redact_cpf_patterns
from src.query.domain.normalization import format_cnpj, format_date
from src.query.web.filters import EstabelecimentoFiltros


def _telefone(ddd: Any, numero: Any) -> str:
    ddd_text = "".join(char for char in str(ddd or "") if char.isdigit())
    numero_text = "".join(char for char in str(numero or "") if char.isdigit())
    if not numero_text:
        return ""
    if not ddd_text:
        return numero_text
    if len(numero_text) == 9:
        return f"({ddd_text}) {numero_text[:5]}-{numero_text[5:]}"
    if len(numero_text) == 8:
        return f"({ddd_text}) {numero_text[:4]}-{numero_text[4:]}"
    return f"({ddd_text}) {numero_text}"


def listing_rows(
    rows: list[dict[str, Any]],
    filtros: EstabelecimentoFiltros | None = None,
    *,
    referencia: date | None = None,
) -> list[dict[str, Any]]:
    result = []
    params = filtros.as_query_params() if filtros else {}
    reference = referencia or date.today()
    for row in rows:
        detail_url = f"/estabelecimentos/{row.get('cnpj_completo')}"
        if params:
            detail_url += f"?{urlencode(params)}"
        result.append(
            {
                **row,
                "detail_url": detail_url,
                "cnpj_formatado": format_cnpj(row.get("cnpj_completo")),
                "cnpj_raiz": row.get("cnpj_basico"),
                "razao_social": redact_cpf_patterns(row.get("razao_social")),
                "nome_fantasia": redact_cpf_patterns(row.get("nome_fantasia")),
                "data_inicio_atividade_formatada": format_date(
                    row.get("data_inicio_atividade")
                ),
                "idade_anos": idade_em_anos(row.get("data_inicio_atividade"), reference),
                "matriz_filial": row.get("matriz_filial_descricao")
                or row.get("identificador_matriz_filial"),
                "simples_label": "Sim" if row.get("optante_simples") else "Não",
                "mei_label": "Sim" if row.get("optante_mei") else "Não",
                "telefone": _telefone(row.get("ddd_1"), row.get("telefone_1")),
                "email": row.get("correio_eletronico") or "",
                "tem_contato": tem_contato(
                    row.get("ddd_1"),
                    row.get("telefone_1"),
                    row.get("correio_eletronico"),
                ),
                "segmento_nome": row.get("segmento_nome") or "Não classificado",
                "qtd_cnaes": row.get("qtd_cnaes") or 0,
                "qtd_estabelecimentos": row.get("qtd_estabelecimentos") or 0,
                "qtd_filiais": row.get("qtd_filiais") or 0,
                "capital_social_formatado": _money(row.get("capital_social")),
            }
        )
    return result


def _url(
    filtros: EstabelecimentoFiltros,
    page: int,
    path: str = "/estabelecimentos",
) -> str:
    params = filtros.as_query_params(include_page=False)
    if page > 1:
        params["pagina"] = str(page)
    return path + (f"?{urlencode(params)}" if params else "")


def pagination(
    filtros: EstabelecimentoFiltros,
    total: int,
    page_size: int,
    path: str = "/estabelecimentos",
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
        "previous_url": _url(filtros, page - 1, path) if page > 1 else None,
        "next_url": _url(filtros, page + 1, path) if total_pages > page else None,
        "first_url": _url(filtros, 1, path),
    }


def clear_url(path: str = "/estabelecimentos") -> str:
    return path


def back_url(
    filtros: EstabelecimentoFiltros,
    path: str = "/estabelecimentos",
) -> str:
    return _url(filtros, filtros.pagina, path)


def export_url(filtros: EstabelecimentoFiltros) -> str:
    params = filtros.as_query_params(include_page=False)
    return "/prospeccao.csv" + (f"?{urlencode(params)}" if params else "")


def segmento_url(filtros: EstabelecimentoFiltros, codigo: str) -> str:
    params = filtros.as_query_params(include_page=False)
    params["segmento"] = codigo
    return "/prospeccao?" + urlencode(params)


CSV_HEADER = [
    "cnpj",
    "cnpj_raiz",
    "razao_social",
    "nome_fantasia",
    "segmento",
    "situacao",
    "uf",
    "municipio",
    "cep",
    "cnae_principal",
    "qtd_cnaes",
    "porte",
    "natureza_juridica",
    "capital_social",
    "data_abertura",
    "idade_anos",
    "matriz_filial",
    "qtd_estabelecimentos",
    "qtd_filiais",
    "telefone",
    "email",
    "simples",
    "mei",
    "cnaes_secundarios",
]

CSV_FORMULA_PREFIXES = ("=", "+", "-", "@")


def csv_value(value: Any) -> str:
    """Stringify a CSV value and neutralize spreadsheet formulas."""

    if value is None:
        return ""
    if isinstance(value, date):
        text = value.isoformat()
    elif isinstance(value, bool):
        text = "Sim" if value else "Não"
    else:
        text = str(value)
    if text.startswith(CSV_FORMULA_PREFIXES):
        text = "'" + text
    return text


def csv_row(
    row: dict[str, Any],
    referencia: date,
) -> list[str]:
    idade = idade_em_anos(row.get("data_inicio_atividade"), referencia)
    capital = row.get("capital_social")
    if capital is not None:
        try:
            capital = f"{float(capital):.2f}"
        except (TypeError, ValueError):
            capital = str(capital)
    return [
        csv_value(row.get("cnpj_completo")),
        csv_value(row.get("cnpj_basico")),
        csv_value(redact_cpf_patterns(row.get("razao_social"))),
        csv_value(redact_cpf_patterns(row.get("nome_fantasia"))),
        csv_value(row.get("segmento_nome") or "Não classificado"),
        csv_value(row.get("situacao_descricao") or row.get("situacao_codigo")),
        csv_value(row.get("uf")),
        csv_value(row.get("municipio_descricao") or row.get("municipio_codigo")),
        csv_value(row.get("cep")),
        csv_value(row.get("cnae_principal_codigo")),
        csv_value(row.get("qtd_cnaes")),
        csv_value(row.get("porte_descricao") or row.get("porte_codigo")),
        csv_value(
            row.get("natureza_juridica_descricao")
            or row.get("natureza_juridica_codigo")
        ),
        csv_value(capital),
        csv_value(row.get("data_inicio_atividade")),
        csv_value("" if idade is None else idade),
        csv_value(
            row.get("matriz_filial_descricao")
            or row.get("identificador_matriz_filial")
        ),
        csv_value(row.get("qtd_estabelecimentos")),
        csv_value(row.get("qtd_filiais")),
        csv_value(_telefone(row.get("ddd_1"), row.get("telefone_1"))),
        csv_value(row.get("correio_eletronico")),
        csv_value(row.get("optante_simples")),
        csv_value(row.get("optante_mei")),
        csv_value(row.get("cnae_secundaria_texto")),
    ]


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
