from __future__ import annotations

import importlib.util
import json
import logging
from pathlib import Path

import pytest

from src.extracao_rfb.competencia import InvalidLimitError
from src.pipeline.raw_bronze import PipelineFailedError
from tests.fakes import FakeObjectStore, make_zip, read_parquet

DAG_PATH = Path(__file__).resolve().parents[2] / "airflow" / "dags" / "cnpj_download.py"


class FakeClient:
    base_path = "Dados/Cadastros/CNPJ"

    def __init__(self, files):
        self.files = list(files)
        self.list_calls = 0

    def list_files(self, competencia):
        self.list_calls += 1
        return list(self.files)

    def get_file_url(self, caminho):
        return f"https://example.test/{caminho}"


def load_dag_module():
    spec = importlib.util.spec_from_file_location("cnpj_download_dag", DAG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_dag_import_does_not_touch_network(block_network):
    module = load_dag_module()
    dag = module.dag

    assert dag.dag_id == "cnpj_download_manual"
    assert dag.schedule is None
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert set(dag.task_ids) == {
        "listar_arquivos",
        "preparar_arquivos",
        "pipeline_arquivo.baixar_raw",
        "pipeline_arquivo.converter_bronze",
        "consolidar_resumo",
    }
    assert {"cnpj", "download", "manual"} <= set(dag.tags)
    assert "competencia" in dag.params
    assert "limite" in dag.params


def test_limit_and_competencia_param_schema():
    module = load_dag_module()
    dag = module.dag

    assert dag.params.get_param("limite").schema == {
        "type": ["null", "integer"],
        "minimum": 1,
    }
    competencia = dag.params.get_param("competencia").schema
    assert competencia["pattern"] == r"^\d{4}-(0[1-9]|1[0-2])$"


def test_mapped_group_has_download_before_conversion():
    module = load_dag_module()
    dag = module.dag

    baixar = dag.get_task("pipeline_arquivo.baixar_raw")
    converter = dag.get_task("pipeline_arquivo.converter_bronze")

    assert "pipeline_arquivo.converter_bronze" in baixar.downstream_task_ids
    assert "pipeline_arquivo.baixar_raw" in converter.upstream_task_ids
    assert dag.get_task("consolidar_resumo").trigger_rule.value == "all_done"


def test_listar_arquivos_task_forwards_params(monkeypatch):
    module = load_dag_module()
    calls = {}

    def fake_listar(competencia, limite, **kwargs):
        calls.update({"competencia": competencia, "limite": limite})
        return {
            "competencia": competencia,
            "encontrados": 1,
            "selecionados": 1,
            "limite": limite,
            "parcial": False,
            "arquivos": [{"nome": "Cnaes.zip"}],
        }

    monkeypatch.setattr(module, "_listar_arquivos", fake_listar)

    payload = module.listar_arquivos.function(
        params={"competencia": "2026-02", "limite": 1}
    )

    assert calls == {"competencia": "2026-02", "limite": 1}
    assert payload["arquivos"] == [{"nome": "Cnaes.zip"}]


def test_listar_arquivos_rejects_invalid_limit_before_io():
    module = load_dag_module()
    client = FakeClient(["Cnaes.zip"])
    store = FakeObjectStore()

    with pytest.raises(InvalidLimitError):
        module._listar_arquivos("2026-02", 0, store=store, client=client)

    assert client.list_calls == 0
    assert store.ensure_calls == 0


def test_preparar_arquivos_returns_one_small_item_per_selected_zip():
    module = load_dag_module()
    listing = {
        "competencia": "2026-02",
        "encontrados": 3,
        "selecionados": 2,
        "limite": 2,
        "parcial": True,
        "arquivos": [{"nome": "Cnaes.zip"}, {"nome": "Empresas0.zip"}],
    }

    arquivos = module._preparar_arquivos(listing)

    assert arquivos == [
        {"nome": "Cnaes.zip", "competencia": "2026-02"},
        {"nome": "Empresas0.zip", "competencia": "2026-02"},
    ]


def test_baixar_raw_reuses_existing_object_without_network():
    module = load_dag_module()
    client = FakeClient(["Cnaes.zip"])
    store = FakeObjectStore({"raw/2026-02/Cnaes.zip": b"existing-zip"})

    payload = module._baixar_raw(
        {"nome": "Cnaes.zip", "competencia": "2026-02"},
        store=store,
        client=client,
    )

    assert payload["status_download"] == "reutilizado"
    assert payload["raw_key"] == "raw/2026-02/Cnaes.zip"
    assert client.list_calls == 0
    encoded = json.dumps(payload)
    assert "http://" not in encoded
    assert "https://" not in encoded


def test_converter_bronze_skips_when_download_failed():
    module = load_dag_module()
    store = FakeObjectStore()
    download = {
        "nome": "Cnaes.zip",
        "status_download": "falhou",
        "raw_key": None,
        "bytes": 0,
        "duracao_segundos": 0.0,
        "erro": "HTTP 500",
    }

    payload = module._converter_bronze(
        {"nome": "Cnaes.zip", "competencia": "2026-02"},
        download,
        store=store,
    )

    assert payload["status_conversao"] == "não executado"
    assert payload["bronze_key"] is None
    assert store.get_calls == []
    assert store.put_calls == []


def test_converter_bronze_publishes_parquet_from_raw():
    module = load_dag_module()
    csv = "1111-1/00;Comércio\n".encode("latin1")
    store = FakeObjectStore(
        {"raw/2026-02/Cnaes.zip": make_zip({"Cnaes.csv": csv})}
    )
    download = {
        "nome": "Cnaes.zip",
        "status_download": "reutilizado",
        "raw_key": "raw/2026-02/Cnaes.zip",
        "bytes": 0,
        "duracao_segundos": 0.0,
        "erro": None,
    }

    payload = module._converter_bronze(
        {"nome": "Cnaes.zip", "competencia": "2026-02"},
        download,
        store=store,
    )

    assert payload["status_conversao"] == "publicado"
    assert payload["bronze_key"] == "bronze/2026-02/Cnaes.parquet"
    assert payload["linhas"] == 1
    table = read_parquet(store.objects[payload["bronze_key"]])
    assert table.num_rows == 1
    assert table.column_names == ["cnae_codigo", "cnae_descricao"]


def test_consolidar_resumo_raises_when_any_file_failed():
    module = load_dag_module()
    listing = {
        "competencia": "2026-02",
        "encontrados": 2,
        "selecionados": 2,
        "limite": None,
        "parcial": False,
        "arquivos": [{"nome": "Cnaes.zip"}, {"nome": "Empresas0.zip"}],
    }
    downloads = [
        {
            "nome": "Cnaes.zip",
            "status_download": "falhou",
            "raw_key": None,
            "bytes": 0,
            "duracao_segundos": 0.0,
            "erro": "HTTP 500",
        },
        {
            "nome": "Empresas0.zip",
            "status_download": "baixado",
            "raw_key": "raw/2026-02/Empresas0.zip",
            "bytes": 10,
            "duracao_segundos": 0.0,
            "erro": None,
        },
    ]
    conversoes = [
        {
            "nome": "Cnaes.zip",
            "status_conversao": "não executado",
            "raw_key": None,
            "bronze_key": None,
            "bytes": 0,
            "linhas": None,
            "duracao_segundos": 0.0,
            "erro": "download falhou",
        },
        {
            "nome": "Empresas0.zip",
            "status_conversao": "publicado",
            "raw_key": "raw/2026-02/Empresas0.zip",
            "bronze_key": "bronze/2026-02/Empresas0.parquet",
            "bytes": 5,
            "linhas": 1,
            "duracao_segundos": 0.0,
            "erro": None,
        },
    ]

    with pytest.raises(PipelineFailedError) as excinfo:
        module._consolidar_resumo(listing, downloads, conversoes)

    summary = excinfo.value.summary
    assert summary["parcial"] is False
    assert summary["totais"]["publicados"] == 1
    assert summary["totais"]["falhas_download"] == 1
    assert "Cnaes.zip" in str(excinfo.value)


def test_consolidar_resumo_marks_partial_runs():
    module = load_dag_module()
    listing = {
        "competencia": "2026-02",
        "encontrados": 10,
        "selecionados": 1,
        "limite": 1,
        "parcial": True,
        "arquivos": [{"nome": "Cnaes.zip"}],
    }
    downloads = [
        {
            "nome": "Cnaes.zip",
            "status_download": "baixado",
            "raw_key": "raw/2026-02/Cnaes.zip",
            "bytes": 10,
            "duracao_segundos": 0.1,
            "erro": None,
        }
    ]
    conversoes = [
        {
            "nome": "Cnaes.zip",
            "status_conversao": "publicado",
            "raw_key": "raw/2026-02/Cnaes.zip",
            "bronze_key": "bronze/2026-02/Cnaes.parquet",
            "bytes": 5,
            "linhas": 2,
            "duracao_segundos": 0.2,
            "erro": None,
        }
    ]

    summary = module._consolidar_resumo(listing, downloads, conversoes)

    assert summary["parcial"] is True
    assert summary["limite"] == 1
    assert summary["encontrados"] == 10
    assert summary["selecionados"] == 1
    assert summary["falhas"] == 0
    encoded = json.dumps(summary)
    assert "http://" not in encoded
    assert "https://" not in encoded
    assert "existing-zip" not in encoded


def test_task_logs_do_not_leak_source_urls_or_file_content(caplog):
    module = load_dag_module()
    client = FakeClient(["Cnaes.zip"])
    store = FakeObjectStore({"raw/2026-02/Cnaes.zip": b"existing-zip"})

    with caplog.at_level(logging.INFO):
        payload = module._baixar_raw(
            {"nome": "Cnaes.zip", "competencia": "2026-02"},
            store=store,
            client=client,
        )

    assert payload["status_download"] == "reutilizado"
    assert "http://" not in caplog.text
    assert "https://" not in caplog.text
    assert "existing-zip" not in caplog.text
    assert "secret" not in caplog.text.lower()
    assert "token" not in caplog.text.lower()


def test_dag_builds_store_lazily_from_environment(monkeypatch):
    module = load_dag_module()
    sentinel = object()
    import src.storage.factory as factory

    monkeypatch.setattr(factory, "build_store_from_env", lambda: sentinel)

    assert module._get_store() is sentinel


def test_dag_default_store_is_filesystem_from_env(monkeypatch, tmp_path):
    module = load_dag_module()
    monkeypatch.setenv("CNPJ_STORAGE_BACKEND", "filesystem")
    monkeypatch.setenv("CNPJ_DATA_ROOT", str(tmp_path / "data"))

    store = module._get_store()

    from src.storage.filesystem import FilesystemObjectStore

    assert isinstance(store, FilesystemObjectStore)
    assert store.root == (tmp_path / "data").resolve()


def test_dag_module_does_not_instantiate_s3_store_at_import():
    source = DAG_PATH.read_text()

    assert "S3ObjectStore" not in source
    assert "build_store_from_env" in source


def test_dag_docs_describe_filesystem_and_r2_without_minio_requirement():
    module = load_dag_module()

    assert "filesystem" in module.DOC_MD.lower()
    assert "R2" in module.DOC_MD
    assert "MinIO" not in module.DOC_MD
    assert "MinIO" not in module.dag.description


def test_listar_arquivos_partial_limits_mapping_groups():
    module = load_dag_module()
    client = FakeClient(["d.zip", "b.zip", "c.zip", "a.zip"])
    store = FakeObjectStore()

    listing = module._listar_arquivos("2026-02", 2, store=store, client=client)
    arquivos = module._preparar_arquivos(listing)

    assert listing["encontrados"] == 4
    assert listing["selecionados"] == 2
    assert listing["limite"] == 2
    assert listing["parcial"] is True
    assert [item["nome"] for item in arquivos] == ["a.zip", "b.zip"]
    assert len(arquivos) == 2
    assert store.head_calls == []
