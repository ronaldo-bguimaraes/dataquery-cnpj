from __future__ import annotations

import json

import pyarrow as pa
import pytest
from aiohttp import web

from src.pipeline.raw_bronze import PipelineFailedError, executar_competencia_async
from src.storage.filesystem import FilesystemObjectStore
from tests.fakes import FakeObjectStore, make_zip, read_parquet

CSV = "1111-1/00;Comércio\n2222-2/00;Serviços\n".encode("latin1")
RAW_KEY = "raw/2026-02/Cnaes.zip"
BRONZE_KEY = "bronze/2026-02/Cnaes.parquet"


class FakeClient:
    base_path = "Dados/Cadastros/CNPJ"

    def __init__(self, files, url_for):
        self.files = list(files)
        self.url_for = url_for
        self.list_calls = 0

    def list_files(self, competencia):
        self.list_calls += 1
        return list(self.files)

    def get_file_url(self, caminho):
        return self.url_for(caminho.rsplit("/", 1)[-1])


async def test_end_to_end_raw_bronze_local_server(http_server, tmp_path):
    payload = make_zip({"Cnaes.csv": CSV})
    requests: list[str] = []

    async def handler(request):
        requests.append(request.path)
        return web.Response(body=payload)

    server = await http_server(handler)
    client = FakeClient(["Cnaes.zip", "Empresas0.zip"], server.url)
    store = FakeObjectStore()

    summary = await executar_competencia_async(
        "2026-02",
        limite=1,
        store=store,
        client=client,
        scratch_dir=tmp_path,
    )

    assert store.objects[RAW_KEY] == payload
    assert BRONZE_KEY in store.objects
    table = read_parquet(store.objects[BRONZE_KEY])
    assert table.num_rows == 2
    assert table.column_names == ["cnae_codigo", "cnae_descricao"]
    assert table.to_pydict()["cnae_descricao"] == ["Comércio", "Serviços"]

    assert summary["competencia"] == "2026-02"
    assert summary["encontrados"] == 2
    assert summary["selecionados"] == 1
    assert summary["parcial"] is True
    assert summary["falhas"] == 0
    item = summary["arquivos"][0]
    assert item["status_download"] == "baixado"
    assert item["status_conversao"] == "publicado"
    assert item["raw_key"] == RAW_KEY
    assert item["bronze_key"] == BRONZE_KEY
    assert item["bytes"] == len(payload)
    assert item["linhas"] == 2
    assert payload not in json.dumps(summary).encode()
    assert "http://" not in json.dumps(summary)
    assert "https://" not in json.dumps(summary)
    assert requests == ["/Cnaes.zip"]
    assert list(tmp_path.iterdir()) == []


async def test_second_run_reuses_raw_and_rebuilds_bronze(http_server, tmp_path):
    payload = make_zip({"Cnaes.csv": CSV})
    requests: list[str] = []

    async def handler(request):
        requests.append(request.path)
        return web.Response(body=payload)

    server = await http_server(handler)
    client = FakeClient(["Cnaes.zip"], server.url)
    store = FakeObjectStore()

    first = await executar_competencia_async(
        "2026-02", store=store, client=client, scratch_dir=tmp_path
    )
    first_table = read_parquet(store.objects[BRONZE_KEY])

    second = await executar_competencia_async(
        "2026-02", store=store, client=client, scratch_dir=tmp_path
    )
    second_table = read_parquet(store.objects[BRONZE_KEY])

    assert len(requests) == 1
    assert store.put_calls.count(RAW_KEY) == 1
    assert store.put_calls.count(BRONZE_KEY) == 2
    assert first["arquivos"][0]["status_download"] == "baixado"
    assert second["arquivos"][0]["status_download"] == "reutilizado"
    assert second["arquivos"][0]["bytes"] == 0
    assert second["arquivos"][0]["status_conversao"] == "publicado"
    assert first_table.to_pydict() == second_table.to_pydict()
    assert all(".part" not in key for key in store.objects)
    assert list(tmp_path.iterdir()) == []


async def test_failed_download_is_isolated_and_run_fails(http_server, tmp_path):
    payload = make_zip({"Cnaes.csv": CSV})

    async def handler(request):
        if request.path.endswith("Empresas0.zip"):
            return web.Response(status=500, text="falha")
        return web.Response(body=payload)

    server = await http_server(handler)
    client = FakeClient(["Cnaes.zip", "Empresas0.zip"], server.url)
    store = FakeObjectStore()

    with pytest.raises(PipelineFailedError) as excinfo:
        await executar_competencia_async(
            "2026-02", store=store, client=client, scratch_dir=tmp_path
        )

    summary = excinfo.value.summary
    by_name = {item["nome"]: item for item in summary["arquivos"]}
    assert by_name["Cnaes.zip"]["status_conversao"] == "publicado"
    assert by_name["Empresas0.zip"]["status_download"] == "falhou"
    assert by_name["Empresas0.zip"]["status_conversao"] == "não executado"
    assert RAW_KEY in store.objects
    assert "raw/2026-02/Empresas0.zip" not in store.objects
    assert "bronze/2026-02/Empresas0.parquet" not in store.objects
    assert summary["totais"]["falhas_download"] == 1
    assert summary["totais"]["publicados"] == 1
    assert "http://" not in str(excinfo.value)
    assert "https://" not in str(excinfo.value)
    assert all(".part" not in key for key in store.objects)
    assert list(tmp_path.iterdir()) == []

async def test_conversion_error_does_not_leak_cpf_in_summary_or_xcom(
    http_server, tmp_path, monkeypatch
):
    payload = make_zip({"Cnaes.csv": CSV})

    async def handler(request):
        return web.Response(body=payload)

    server = await http_server(handler)
    client = FakeClient(["Cnaes.zip"], server.url)
    store = FakeObjectStore()

    def broken_conversion(*args, **kwargs):
        raise pa.ArrowInvalid(
            "CSV parse error near '123.456.789-00'; expected string"
        )

    monkeypatch.setattr(
        "src.conversao.bronze.convert_csv_to_parquet", broken_conversion
    )

    with pytest.raises(PipelineFailedError) as excinfo:
        await executar_competencia_async(
            "2026-02", store=store, client=client, scratch_dir=tmp_path
        )

    summary = excinfo.value.summary
    encoded = json.dumps(summary)
    assert summary["totais"]["falhas_conversao"] == 1
    assert "123.456.789-00" not in encoded
    assert "123.456.789-00" not in str(excinfo.value)
    assert "ArrowInvalid" in encoded
    assert len(encoded.encode()) < 4096
    assert list(tmp_path.iterdir()) == []


def filesystem_store(tmp_path):
    root = tmp_path / "data"
    root.mkdir()
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    return FilesystemObjectStore(root=root), root, scratch


async def test_end_to_end_with_filesystem_store(http_server, tmp_path):
    payload = make_zip({"Cnaes.csv": CSV})
    requests: list[str] = []

    async def handler(request):
        requests.append(request.path)
        return web.Response(body=payload)

    server = await http_server(handler)
    client = FakeClient(["Cnaes.zip", "Empresas0.zip"], server.url)
    store, root, scratch = filesystem_store(tmp_path)

    summary = await executar_competencia_async(
        "2026-02",
        limite=1,
        store=store,
        client=client,
        scratch_dir=scratch,
    )

    assert (root / RAW_KEY).read_bytes() == payload
    bronze_path = root / BRONZE_KEY
    assert bronze_path.is_file()
    table = read_parquet(bronze_path.read_bytes())
    assert table.num_rows == 2
    assert table.column_names == ["cnae_codigo", "cnae_descricao"]
    assert summary["selecionados"] == 1
    assert summary["parcial"] is True
    assert summary["arquivos"][0]["status_download"] == "baixado"
    assert summary["arquivos"][0]["status_conversao"] == "publicado"
    encoded = json.dumps(summary)
    assert "http://" not in encoded
    assert "https://" not in encoded
    assert "s3://" not in encoded
    assert str(tmp_path) not in encoded
    assert requests == ["/Cnaes.zip"]
    assert list(scratch.iterdir()) == []
    assert not list((root / "raw").rglob("*.part"))
    assert not list((root / "bronze").rglob("*.part"))


async def test_filesystem_store_second_run_reuses_raw_and_rebuilds_bronze(
    http_server, tmp_path
):
    payload = make_zip({"Cnaes.csv": CSV})
    requests: list[str] = []

    async def handler(request):
        requests.append(request.path)
        return web.Response(body=payload)

    server = await http_server(handler)
    client = FakeClient(["Cnaes.zip"], server.url)
    store, root, scratch = filesystem_store(tmp_path)

    first = await executar_competencia_async(
        "2026-02", store=store, client=client, scratch_dir=scratch
    )
    first_table = read_parquet((root / BRONZE_KEY).read_bytes())
    bronze_mtime = (root / BRONZE_KEY).stat().st_mtime_ns

    second = await executar_competencia_async(
        "2026-02", store=store, client=client, scratch_dir=scratch
    )
    second_table = read_parquet((root / BRONZE_KEY).read_bytes())

    assert requests == ["/Cnaes.zip"]
    assert first["arquivos"][0]["status_download"] == "baixado"
    assert second["arquivos"][0]["status_download"] == "reutilizado"
    assert second["arquivos"][0]["bytes"] == 0
    assert second["arquivos"][0]["status_conversao"] == "publicado"
    assert (root / BRONZE_KEY).stat().st_mtime_ns >= bronze_mtime
    assert first_table.to_pydict() == second_table.to_pydict()
    assert list(scratch.iterdir()) == []
    assert not list(root.rglob("*.part"))
