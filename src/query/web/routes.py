"""HTTP routes for the server-rendered explorer."""

from __future__ import annotations

import math
from dataclasses import replace

import psycopg
from psycopg_pool import PoolTimeout
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from src.query.db.repositories import (
    COUNT_CAP,
    active_metadata,
    listar_cnaes,
    listar_estabelecimentos,
    listar_socios,
    obter_estabelecimento,
    obter_simples,
    opcoes_filtro,
)
from src.query.domain.normalization import is_valid_cnpj, normalize_cnpj
from src.query.web.filters import FilterError, parse_filters
from src.query.web.presenters import (
    back_url,
    clear_url,
    detail_view,
    listing_rows,
    pagination,
)

router = APIRouter()


@router.get("/")
async def root():
    return RedirectResponse(url="/estabelecimentos", status_code=303)


@router.get("/estabelecimentos", response_class=HTMLResponse)
async def estabelecimentos(request: Request):
    config = request.app.state.config
    filtros = parse_filters(request.query_params)
    page_size = config.page_size

    notice = None
    with request.app.state.pool.connection() as connection:
        options = opcoes_filtro(connection, uf=filtros.uf)
        if filtros.uf and filtros.municipio and not any(
            item["codigo"] == filtros.municipio for item in options["municipios"]
        ):
            filtros = replace(filtros, municipio=None)
            options = opcoes_filtro(connection, uf=filtros.uf)
            notice = "O município informado não pertence à UF selecionada e foi removido."
        rows, total = listar_estabelecimentos(connection, filtros, page_size=page_size)
        total_capped = total > COUNT_CAP
        if total_capped:
            total = COUNT_CAP
        total_pages = math.ceil(total / page_size) if total else 0
        if total_pages and filtros.pagina > total_pages:
            filtros = replace(filtros, pagina=total_pages)
            rows, total = listar_estabelecimentos(
                connection, filtros, page_size=page_size
            )

    context = {
        "request": request,
        "competencia": config.competencia,
        "fonte": config.fonte,
        "filtros": filtros,
        "options": options,
        "rows": listing_rows(rows, filtros),
        "total": total,
        "total_capped": total_capped,
        "pagination": pagination(filtros, total, page_size),
        "clear_url": clear_url(),
        "back_url": back_url(filtros),
        "notice": notice,
    }
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="estabelecimentos.html",
        context=context,
    )


@router.get("/estabelecimentos/{cnpj}", response_class=HTMLResponse)
async def estabelecimento_detalhe(request: Request, cnpj: str):
    config = request.app.state.config
    filtros = parse_filters(request.query_params)
    digits = normalize_cnpj(cnpj)
    if len(digits) != 14 or not is_valid_cnpj(digits):
        raise FilterError("cnpj", "CNPJ inválido.")

    with request.app.state.pool.connection() as connection:
        row = obter_estabelecimento(connection, digits)
        if row is None:
            return request.app.state.templates.TemplateResponse(
                request=request,
                name="nao_encontrado.html",
                context={
                    "request": request,
                    "competencia": config.competencia,
                    "fonte": config.fonte,
                    "cnpj": digits,
                    "back_url": back_url(filtros),
                },
                status_code=404,
            )
        cnaes = listar_cnaes(connection, digits)
        socios = listar_socios(connection, row["empresa_cnpj_basico"])
        simples = obter_simples(connection, row["empresa_cnpj_basico"])

    context = {
        "request": request,
        "competencia": config.competencia,
        "fonte": config.fonte,
        "estabelecimento": detail_view(row, cnaes, socios, simples),
        "back_url": back_url(filtros),
    }
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="detalhe.html",
        context=context,
    )


@router.get("/health")
async def health(request: Request):
    config = request.app.state.config
    try:
        with request.app.state.pool.connection() as connection:
            metadata = active_metadata(connection)
    except (psycopg.Error, PoolTimeout):
        return JSONResponse(
            {"status": "error", "banco": "indisponivel", "competencia": config.competencia},
            status_code=503,
        )
    if not metadata:
        return JSONResponse(
            {"status": "error", "banco": "ok", "competencia": config.competencia},
            status_code=503,
        )
    return JSONResponse(
        {
            "status": "ok",
            "competencia": metadata["competencia"],
            "fonte": metadata["fonte"],
            "banco": "ok",
        }
    )
