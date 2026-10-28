from __future__ import annotations

import logging

import psycopg
import pytest


def test_web_role_is_read_only(query_db, app_dsn: str):
    with psycopg.connect(app_dsn) as connection:
        with pytest.raises(psycopg.Error):
            connection.execute(
                "INSERT INTO empresas (cnpj_basico, razao_social, razao_social_normalizada) "
                "VALUES ('99999999', 'Teste', 'teste')"
            )


def test_no_export_or_download_routes(app_client):
    assert app_client.get("/export.csv").status_code == 404
    assert app_client.get("/download").status_code == 404
    assert "export" not in app_client.get("/estabelecimentos").text.lower()


def test_html_escapes_search_term(app_client):
    response = app_client.get("/estabelecimentos?q=%3Cscript%3Ealert(1)%3C/script%3E")
    assert response.status_code == 200
    assert "<script>" not in response.text


def test_sql_injection_attempt_is_rejected(app_client):
    response = app_client.get("/estabelecimentos?uf=SP%27%20OR%201%3D1--")
    assert response.status_code == 400


def test_no_forbidden_sources_are_presented(app_client):
    text = app_client.get("/estabelecimentos").text.lower()
    for forbidden in ("lucro real", "lucro presumido", "pgfn", "cno", "superset"):
        assert forbidden not in text


def test_logs_do_not_contain_cpf_or_search_terms(app_client, caplog):
    with caplog.at_level(logging.INFO, logger="src.query.web"):
        app_client.get("/estabelecimentos?q=Alfa")
        app_client.get("/estabelecimentos/00000001000136")
    assert "Alfa" not in caplog.text
    assert "12345678901" not in caplog.text


def test_database_has_no_full_cpf(loaded_db):
    query_db, _summary = loaded_db
    count = query_db.execute(
        "SELECT count(*) FROM socios WHERE documento_socio_mascarado ~ '^[0-9]{11}$'"
    ).fetchone()[0]
    assert count == 0
