from __future__ import annotations

import csv
import io
from dataclasses import replace


def _csv_rows(response) -> list[list[str]]:
    text = response.content.decode("utf-8-sig")
    return list(csv.reader(io.StringIO(text), delimiter=";"))


def test_prospeccao_defaults_to_active_companies(app_client):
    response = app_client.get("/prospeccao")
    assert response.status_code == 200
    text = response.text
    assert 'data-competencia="2026-10"' in text
    assert 'data-total="3"' in text
    assert "Empresa Alfa" in text
    assert "Comércio Açaí" in text
    # Estabelecimento baixado (situação 08) não aparece no recorte padrão.
    assert "00.000.001/0002-17" not in text
    assert "Estabelecimentos por segmento" in text
    assert "Estabelecimentos por UF" in text
    assert "Gerar lista CSV" in text
    assert "Receita Federal" in text


def test_prospeccao_can_include_all_situations(app_client):
    response = app_client.get("/prospeccao?situacao=")
    assert response.status_code == 200
    assert 'data-total="4"' in response.text
    assert "00.000.001/0002-17" in response.text


def test_prospeccao_filters_by_segmento(app_client):
    response = app_client.get("/prospeccao?segmento=VAREJO")
    assert response.status_code == 200
    assert 'data-total="1"' in response.text
    assert "Comércio Açaí" in response.text
    assert "Empresa Alfa" not in response.text


def test_prospeccao_filters_by_contact_and_branches(app_client):
    sem_contato = app_client.get("/prospeccao?situacao=&com_contato=nao")
    assert 'data-total="1"' in sem_contato.text

    com_filial = app_client.get("/prospeccao?filiais_min=1")
    assert 'data-total="1"' in com_filial.text
    assert "00.000.001/0001-36" in com_filial.text


def test_prospeccao_filters_by_age(app_client):
    response = app_client.get("/prospeccao?idade_min=5")
    assert response.status_code == 200
    assert 'data-total="1"' in response.text
    # A filial antiga está baixada e não entra no recorte padrão (ativa).
    assert "00.000.001/0002-17" not in response.text

    todas = app_client.get("/prospeccao?idade_min=5&situacao=")
    assert 'data-total="2"' in todas.text


def test_prospeccao_rejects_invalid_segmento(app_client):
    response = app_client.get("/prospeccao?segmento=INEXISTENTE")
    assert response.status_code == 400
    assert "Segmento inválido" in response.text


def test_prospeccao_rejects_inverted_age_range(app_client):
    response = app_client.get("/prospeccao?idade_min=10&idade_max=5")
    assert response.status_code == 400
    assert "idade" in response.text.lower()


def test_prospeccao_rejects_inverted_branch_range(app_client):
    response = app_client.get("/prospeccao?filiais_min=5&filiais_max=2")
    assert response.status_code == 400
    assert "filiais" in response.text.lower()


def test_prospeccao_empty_state(app_client):
    response = app_client.get("/prospeccao?segmento=SAUDE")
    assert response.status_code == 200
    assert 'data-total="0"' in response.text
    assert "limpar" in response.text.lower()


def test_segment_group_link_preserves_active_filters(app_client):
    response = app_client.get("/prospeccao?uf=SP")
    assert response.status_code == 200
    assert "/prospeccao?situacao=02&amp;uf=SP&amp;segmento=INDUSTRIA" in response.text


def test_all_situations_survives_pagination_and_export(app_client):
    app_client.app.state.config = replace(app_client.app.state.config, page_size=1)
    response = app_client.get("/prospeccao?situacao=")
    assert response.status_code == 200
    assert "situacao=&amp;pagina=2" in response.text
    assert "/prospeccao.csv?situacao=" in response.text


def test_export_returns_csv_with_header_and_rows(app_client):
    response = app_client.get("/prospeccao.csv?segmento=INDUSTRIA")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "attachment" in response.headers["content-disposition"]
    assert "prospeccao-2026-10.csv" in response.headers["content-disposition"]

    rows = _csv_rows(response)
    assert rows[0][0] == "cnpj"
    assert rows[0][1] == "cnpj_raiz"
    assert rows[0][4] == "segmento"
    assert len(rows) == 3  # cabeçalho + 2 indústrias ativas
    assert {row[1] for row in rows[1:]} <= {"00000001", "00000002"}


def test_export_defaults_to_active_and_respects_filters(app_client):
    response = app_client.get("/prospeccao.csv")
    rows = _csv_rows(response)
    assert len(rows) == 4  # cabeçalho + 3 ativos

    sem_contato = _csv_rows(app_client.get("/prospeccao.csv?situacao=&com_contato=nao"))
    assert len(sem_contato) == 2
    assert sem_contato[1][0] == "00000001000217"


def test_export_never_contains_cpf(app_client):
    response = app_client.get("/prospeccao.csv?situacao=")
    text = response.content.decode("utf-8-sig")
    assert "12345678901" not in text
    assert "***.***.***-**" not in text


def test_export_limit_is_applied_and_reported(app_client):
    app_client.app.state.config = replace(
        app_client.app.state.config, export_limit=2
    )
    page = app_client.get("/prospeccao?situacao=")
    assert "O recorte excede o limite" in page.text

    rows = _csv_rows(app_client.get("/prospeccao.csv?situacao="))
    assert len(rows) == 3  # cabeçalho + 2 linhas exportadas


def test_export_escapes_formula_injection(app_client, loaded_db):
    connection, _summary = loaded_db
    connection.execute(
        "UPDATE estabelecimentos_listagem SET razao_social = '=1+1' "
        "WHERE cnpj_completo = '00000001000136'"
    )
    connection.commit()

    rows = _csv_rows(app_client.get("/prospeccao.csv?segmento=INDUSTRIA"))
    nomes = [row[2] for row in rows[1:]]
    assert "'=1+1" in nomes
