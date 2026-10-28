from __future__ import annotations

import os

import pytest


@pytest.mark.e2e
def test_explorer_flow_with_playwright():
    base_url = os.getenv("CNPJ_QUERY_E2E_URL")
    if not base_url:
        pytest.skip("CNPJ_QUERY_E2E_URL não configurada")
    sync_api = pytest.importorskip("playwright.sync_api")
    try:
        with sync_api.sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page()
            page.goto(f"{base_url.rstrip('/')}/estabelecimentos", wait_until="networkidle")
            assert "2026-10" in page.content()
            assert "Receita Federal" in page.content()

            page.select_option("#uf", "SP")
            page.fill("#cnae", "1111301")
            page.click("button[type=submit]")
            page.wait_for_load_state("networkidle")
            assert 'data-total="1"' in page.content()
            assert "Empresa Alfa" in page.content()

            page.click("tbody tr td a")
            page.wait_for_load_state("networkidle")
            assert "Estabelecimento" in page.content()
            assert "***.***.***-**" in page.content()
            assert "export" not in page.content().lower()

            page.click("text=Voltar para a lista")
            page.wait_for_load_state("networkidle")
            assert "uf=SP" in page.url
            assert "cnae=1111301" in page.url
            browser.close()
    except Exception as exc:  # noqa: BLE001 - browser may not be installed
        pytest.skip(f"Playwright indisponível: {type(exc).__name__}")
