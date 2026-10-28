from __future__ import annotations

import psycopg
import pytest


def test_root_redirects_to_listing(app_client):
    response = app_client.get("/", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/estabelecimentos"


def test_listing_shows_competence_source_and_rows(app_client):
    response = app_client.get("/estabelecimentos")
    assert response.status_code == 200
    text = response.text
    assert 'data-competencia="2026-10"' in text
    assert 'data-fonte="Receita Federal"' in text
    assert 'data-total="4"' in text
    assert "retrato mensal" in text.lower()
    assert "Empresa Alfa" in text
    assert "export" not in text.lower()
    assert "download" not in text.lower()
    assert "csv" not in text.lower()


def test_listing_filters_by_uf_and_cnae(app_client):
    response = app_client.get("/estabelecimentos?uf=sp&cnae=1111301")
    assert response.status_code == 200
    assert 'data-total="1"' in response.text
    assert "Empresa Alfa" in response.text
    assert "Empresa Beta" not in response.text


def test_listing_matches_secondary_cnae(app_client):
    response = app_client.get("/estabelecimentos?cnae=2222201")
    assert response.status_code == 200
    assert 'data-total="2"' in response.text


def test_listing_empty_state(app_client):
    response = app_client.get("/estabelecimentos?uf=SP&cnae=9999999")
    assert response.status_code == 200
    assert 'data-total="0"' in response.text
    assert "limpar" in response.text.lower()


def test_listing_rejects_invalid_cnpj(app_client):
    response = app_client.get("/estabelecimentos?q=11.222.333/0001-82")
    assert response.status_code == 400
    assert "CNPJ" in response.text


def test_listing_rejects_invalid_date_range(app_client):
    response = app_client.get(
        "/estabelecimentos?abertura_de=2021-01-01&abertura_ate=2020-01-01"
    )
    assert response.status_code == 400
    assert "data" in response.text.lower()


def test_sequential_requests_do_not_share_filter_state(app_client):
    first = app_client.get("/estabelecimentos?uf=SP")
    second = app_client.get("/estabelecimentos?q=Beta")
    assert 'data-total="2"' in first.text
    assert 'data-total="1"' in second.text
    assert "Empresa Beta" in second.text
    assert "Empresa Alfa" not in second.text


def test_health_returns_json(app_client):
    response = app_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "competencia": "2026-10",
        "fonte": "Receita Federal",
        "banco": "ok",
    }


def test_health_returns_503_on_database_failure(app_client, monkeypatch):
    import src.query.web.routes as routes

    def boom(_connection):
        raise psycopg.OperationalError("indisponível")

    monkeypatch.setattr(routes, "active_metadata", boom)
    response = app_client.get("/health")
    assert response.status_code == 503


def test_incompatible_municipality_is_removed(app_client):
    response = app_client.get("/estabelecimentos?uf=SP&municipio=6001")
    assert response.status_code == 200
    assert 'data-total="2"' in response.text
    assert "não pertence à UF" in response.text
