"""HTTP routes for the server-rendered explorer."""

from __future__ import annotations

import csv
import io
import math
from dataclasses import replace

import psycopg
from psycopg_pool import PoolTimeout
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse

from src.query.db.repositories import (
    COUNT_CAP,
    active_metadata,
    agrupar_prospeccao,
    iterar_prospeccao,
    listar_cnaes,
    listar_estabelecimentos,
    listar_prospeccao,
    listar_socios,
    obter_estabelecimento,
    obter_simples,
    opcoes_filtro,
)
from src.query.domain.normalization import is_valid_cnpj, normalize_cnpj
from src.query.web.filters import FilterError, parse_filters
from src.query.web.presenters import (
    CSV_HEADER,
    back_url,
    clear_url,
    csv_row,
    detail_view,
    export_url,
    listing_rows,
    pagination,
    segmento_url,
)

router = APIRouter()

PROSPECCAO_PATH = "/prospeccao"


def _prospeccao_filters(request: Request):
    """Parse filters, defaulting the cadastral situation to active (02).

    The default only applies when the parameter is absent: an explicit empty
    value (``situacao=``) means "todas", so the user can still broaden the slice.
    """

    config = request.app.state.config
    items = list(request.query_params.multi_items())
    if "situacao" not in request.query_params:
        items.append(("situacao", "02"))
    return parse_filters(items, referencia=config.end_date)


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


@router.get("/prospeccao", response_class=HTMLResponse)
async def prospeccao(request: Request):
    config = request.app.state.config
    filtros = _prospeccao_filters(request)
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

        rows, total = listar_prospeccao(connection, filtros, page_size=page_size)
        grupos = agrupar_prospeccao(connection, filtros)
        total_capped = total > COUNT_CAP
        if total_capped:
            total = COUNT_CAP
        total_pages = math.ceil(total / page_size) if total else 0
        if total_pages and filtros.pagina > total_pages:
            filtros = replace(filtros, pagina=total_pages)
            rows, total = listar_prospeccao(connection, filtros, page_size=page_size)

    for item in grupos["segmentos"]:
        item["url"] = segmento_url(filtros, item["codigo"])

    context = {
        "request": request,
        "competencia": config.competencia,
        "fonte": config.fonte,
        "filtros": filtros,
        "options": options,
        "rows": listing_rows(rows, filtros, referencia=config.end_date),
        "total": total,
        "total_capped": total_capped,
        "grupos": grupos,
        "pagination": pagination(filtros, total, page_size, PROSPECCAO_PATH),
        "clear_url": clear_url(PROSPECCAO_PATH),
        "back_url": back_url(filtros, PROSPECCAO_PATH),
        "export_url": export_url(filtros),
        "export_limit": config.export_limit,
        "export_limited": total > config.export_limit,
        "notice": notice,
    }
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="prospeccao.html",
        context=context,
    )


@router.get("/prospeccao.csv")
async def prospeccao_csv(request: Request):
    config = request.app.state.config
    filtros = _prospeccao_filters(request)
    pool = request.app.state.pool
    referencia = config.end_date

    def generate():
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter=";", lineterminator="\r\n")
        writer.writerow(CSV_HEADER)
        yield "\ufeff" + buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)
        with pool.connection() as connection:
            with connection.transaction():
                for row in iterar_prospeccao(
                    connection, filtros, limit=config.export_limit
                ):
                    writer.writerow(csv_row(row, referencia))
                    if buffer.tell() >= 8192:
                        yield buffer.getvalue()
                        buffer.seek(0)
                        buffer.truncate(0)
        yield buffer.getvalue()

    filename = f"prospeccao-{config.competencia}.csv"
    return StreamingResponse(
        generate(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
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
