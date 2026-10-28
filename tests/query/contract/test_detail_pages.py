from __future__ import annotations


def test_detail_shows_all_sections(app_client):
    response = app_client.get("/estabelecimentos/00000001000136")
    assert response.status_code == 200
    text = response.text
    assert "Empresa Alfa Ltda" in text
    assert "Alfa Matriz" in text
    assert "Ativa" in text
    assert "SAO PAULO" in text
    assert "1111301" in text
    assert "Cultivo de arroz" in text
    assert "Sócio-Administrador" in text
    assert "Maria Exemplo" in text
    assert "Simples" in text
    assert "MEI" in text
    assert "***.***.***-**" in text
    assert "Receita Federal" in text
    assert "desatualiz" in text.lower()
    assert "export" not in text.lower()
    assert "download" not in text.lower()


def test_detail_shows_inactive_establishment(app_client):
    response = app_client.get("/estabelecimentos/00000001000217")
    assert response.status_code == 200
    assert "Baixada" in response.text
    assert "RIO DE JANEIRO" in response.text


def test_detail_shows_mei_and_absent_sections(app_client):
    response = app_client.get("/estabelecimentos/00000002000180")
    assert response.status_code == 200
    assert "MEI" in response.text
    assert "Sim" in response.text

    none_response = app_client.get("/estabelecimentos/00000003000125")
    assert none_response.status_code == 200
    assert "Nenhum sócio registrado" in none_response.text
    assert "Sem registro" in none_response.text


def test_detail_invalid_cnpj_returns_400(app_client):
    response = app_client.get("/estabelecimentos/11222333000182")
    assert response.status_code == 400
    assert "CNPJ" in response.text


def test_detail_not_found_returns_404(app_client):
    response = app_client.get("/estabelecimentos/11222333000181")
    assert response.status_code == 404
    assert "não encontrado" in response.text.lower()


def test_detail_back_link_preserves_filters_and_page(app_client):
    response = app_client.get("/estabelecimentos/00000001000136?uf=SP&pagina=2")
    assert response.status_code == 200
    assert "uf=SP" in response.text
    assert "pagina=2" in response.text
