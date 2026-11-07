"""DAG `cnpj_ingestion` (T036/T037): import sem rede, DTM, pool e limites."""

from __future__ import annotations

import ast
from pathlib import Path

DAG_PATH = Path("airflow/dags/cnpj_ingestion.py")


class TestDagImportamento:
    def test_sem_trabalho_no_import(self):
        """A DAG não deve chamar módulos de ingest no escopo do módulo (padrão do repo)."""

        tree = ast.parse(DAG_PATH.read_text())
        chamadas_top_level = {
            node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")
            for node in tree.body
            if isinstance(node, ast.Call)
        }
        assert "build_source_manifest" not in chamadas_top_level
        assert "connect" not in chamadas_top_level

    def test_pool_e_limite_dedicao(self):
        source = DAG_PATH.read_text()
        assert 'INGEST_POOL = os.environ.get("CNPJ_INGEST_POOL", "ingest_parallel")' in source
        assert 'MAX_ACTIVE_TIS = int(os.environ.get("CNPJ_INGEST_MAX_ACTIVE_TIS", "8"))' in source


class TestDagMapping:
    def test_expansion_do_dtm_presente(self):
        source = DAG_PATH.read_text()
        assert "carregar_particao.expand(" in source
        # `expand` depende da lista determinística de partições
        assert "particionar(manifesto)" in source

    def test_ordem_validade_antes_de_publicar(self):
        source = DAG_PATH.read_text()
        assert "validadas >> publicar_serving(manifesto)" in source
