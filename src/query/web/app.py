"""FastAPI application for the CNPJ visual explorer."""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import psycopg
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException

from src.query.config import QueryConfig
from src.query.db.connection import create_pool
from src.query.web.filters import FilterError
from src.query.web.routes import router

log = logging.getLogger("src.query.web")

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"


def create_app(config: QueryConfig | None = None) -> FastAPI:
    query_config = config or QueryConfig.from_env()
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = create_pool(query_config.database_url, readonly=True)
        pool.wait(timeout=10)
        app.state.pool = pool
        try:
            yield
        finally:
            pool.close()

    app = FastAPI(
        title="Explorador Visual de Dados CNPJ",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.config = query_config
    app.state.templates = templates
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    app.include_router(router)

    @app.middleware("http")
    async def request_logging(request: Request, call_next):
        started = time.perf_counter()
        response = await call_next(request)
        duration_ms = (time.perf_counter() - started) * 1000
        log.info(
            "request method=%s path=%s status=%s duration_ms=%.1f",
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
        )
        return response

    def page_context(request: Request, **extra: Any) -> dict[str, Any]:
        return {
            "request": request,
            "competencia": query_config.competencia,
            "fonte": query_config.fonte,
            **extra,
        }

    @app.exception_handler(FilterError)
    async def filter_error_handler(request: Request, exc: FilterError):
        return templates.TemplateResponse(
            request=request,
            name="erro.html",
            context=page_context(
                request,
                status_code=400,
                title="Filtro inválido",
                message=exc.message,
                field=exc.field,
            ),
            status_code=400,
        )

    @app.exception_handler(psycopg.Error)
    async def database_error_handler(request: Request, exc: psycopg.Error):
        log.warning("database error path=%s type=%s", request.url.path, type(exc).__name__)
        return templates.TemplateResponse(
            request=request,
            name="erro.html",
            context=page_context(
                request,
                status_code=503,
                title="Dados temporariamente indisponíveis",
                message="Não foi possível consultar os dados agora. Tente novamente em instantes.",
                field=None,
            ),
            status_code=503,
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(request: Request, exc: StarletteHTTPException):
        status = exc.status_code
        if status == 404:
            title = "Página não encontrada"
            message = "O endereço informado não existe."
        elif status == 400:
            title = "Requisição inválida"
            message = "Revise os filtros informados."
        else:
            title = "Não foi possível concluir a consulta"
            message = "Tente novamente em instantes."
        return templates.TemplateResponse(
            request=request,
            name="erro.html",
            context=page_context(
                request,
                status_code=status,
                title=title,
                message=message,
                field=None,
            ),
            status_code=status,
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(request: Request, exc: Exception):
        log.exception("unexpected error path=%s", request.url.path)
        return templates.TemplateResponse(
            request=request,
            name="erro.html",
            context=page_context(
                request,
                status_code=500,
                title="Erro inesperado",
                message="Não foi possível concluir a consulta. Tente novamente.",
                field=None,
            ),
            status_code=500,
        )

    return app


app = create_app()
