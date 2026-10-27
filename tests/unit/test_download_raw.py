from __future__ import annotations

import json

import pytest
from aiohttp import web

from src.extracao_rfb.competencia import InvalidCompetenciaError, InvalidLimitError
from src.extracao_rfb.download_competencia import EmptyCompetenciaError
from src.extracao_rfb.download_raw import (
    download_raw_file,
    download_raw_file_async,
    listar_arquivos,
)
from src.storage.filesystem import FilesystemObjectStore
from src.storage.minio import S3OperationError
from tests.fakes import FakeObjectStore

RAW_KEY = "raw/2026-02/Cnaes.zip"


class FakeClient:
    base_path = "Dados/Cadastros/CNPJ"

    def __init__(self, files, url_for=None, error=None):
        self.files = list(files)
        self.url_for = url_for or (lambda name: f"https://example.test/{name}")
        self.error = error
        self.list_calls = 0
        self.requested_paths: list[str] = []

    def list_files(self, competencia):
        self.list_calls += 1
        if self.error is not None:
            raise self.error
        return list(self.files)

    def get_file_url(self, caminho):
        self.requested_paths.append(caminho)
        return self.url_for(caminho.rsplit("/", 1)[-1])


def test_listar_arquivos_filters_directories_and_sorts():
    client = FakeClient(["b.zip", "2026-02/", "a.zip", "", ".", "..", "sub/x.zip"])
    store = FakeObjectStore()

    listing = listar_arquivos("2026-02", store=store, client=client)

    assert listing.nomes == ("a.zip", "b.zip")
    assert listing.encontrados == 2
    assert listing.selecionados == 2
    assert listing.parcial is False
    assert listing.limite is None
    assert store.ensure_calls == 1
    assert client.list_calls == 1


def test_listar_arquivos_empty_competencia_fails_explicitly():
    client = FakeClient([])
    store = FakeObjectStore()

    with pytest.raises(EmptyCompetenciaError):
        listar_arquivos("2026-02", store=store, client=client)


def test_listar_arquivos_validates_competencia_before_io():
    client = FakeClient(["a.zip"])
    store = FakeObjectStore()

    with pytest.raises(InvalidCompetenciaError):
        listar_arquivos("2026-13", store=store, client=client)

    assert client.list_calls == 0
    assert store.ensure_calls == 0


def test_listar_arquivos_validates_limit_before_io():
    client = FakeClient(["a.zip"])
    store = FakeObjectStore()

    with pytest.raises(InvalidLimitError):
        listar_arquivos("2026-02", limite=0, store=store, client=client)

    assert client.list_calls == 0
    assert store.ensure_calls == 0


@pytest.mark.parametrize("limite", [0, -1, True, False, "1", 1.5])
def test_listar_arquivos_rejects_invalid_limit_before_store_and_rfb(limite):
    client = FakeClient(["a.zip"])
    store = FakeObjectStore()

    with pytest.raises(InvalidLimitError):
        listar_arquivos("2026-02", limite=limite, store=store, client=client)

    assert client.list_calls == 0
    assert client.requested_paths == []
    assert store.ensure_calls == 0
    assert store.head_calls == []


def test_listar_arquivos_applies_limit_before_raw_check():
    client = FakeClient(["c.zip", "a.zip", "b.zip"])
    store = FakeObjectStore({"raw/2026-02/a.zip": b"already-raw"})

    listing = listar_arquivos("2026-02", limite=2, store=store, client=client)

    assert listing.nomes == ("a.zip", "b.zip")
    assert listing.encontrados == 3
    assert listing.selecionados == 2
    assert listing.parcial is True
    assert listing.limite == 2
    assert store.head_calls == []


def test_listar_arquivos_limit_above_total_returns_all():
    client = FakeClient(["b.zip", "a.zip"])
    store = FakeObjectStore()

    listing = listar_arquivos("2026-02", limite=10, store=store, client=client)

    assert listing.nomes == ("a.zip", "b.zip")
    assert listing.selecionados == 2
    assert listing.parcial is False


def test_listar_arquivos_payload_contains_only_small_metadata():
    client = FakeClient(["b.zip", "a.zip"])
    store = FakeObjectStore()

    payload = listar_arquivos(
        "2026-02", limite=1, store=store, client=client
    ).to_dict()
    encoded = json.dumps(payload)

    assert payload["competencia"] == "2026-02"
    assert payload["encontrados"] == 2
    assert payload["selecionados"] == 1
    assert payload["parcial"] is True
    assert payload["arquivos"] == [{"nome": "a.zip"}]
    assert "http://" not in encoded
    assert "https://" not in encoded


async def test_download_raw_file_stores_zip_under_raw_key(http_server, tmp_path):
    payload = b"zip-bytes-for-cnaes"
    requests: list[str] = []

    async def handler(request):
        requests.append(request.path)
        return web.Response(body=payload)

    server = await http_server(handler)
    client = FakeClient(["Cnaes.zip"], url_for=server.url)
    store = FakeObjectStore()

    result = await download_raw_file_async(
        "2026-02",
        "Cnaes.zip",
        store=store,
        client=client,
        scratch_dir=tmp_path,
    )

    assert result.status == "baixado"
    assert result.nome == "Cnaes.zip"
    assert result.raw_key == "raw/2026-02/Cnaes.zip"
    assert result.bytes_baixados == len(payload)
    assert result.erro is None
    assert store.objects["raw/2026-02/Cnaes.zip"] == payload
    assert len(requests) == 1
    assert list(tmp_path.iterdir()) == []


async def test_existing_non_empty_raw_is_reused_without_rfb_request(http_server, tmp_path):
    requests: list[str] = []

    async def handler(request):
        requests.append(request.path)
        return web.Response(body=b"should-not-be-requested")

    server = await http_server(handler)
    client = FakeClient(["Cnaes.zip"], url_for=server.url)
    store = FakeObjectStore({"raw/2026-02/Cnaes.zip": b"existing-zip"})

    result = await download_raw_file_async(
        "2026-02",
        "Cnaes.zip",
        store=store,
        client=client,
        scratch_dir=tmp_path,
    )

    assert result.status == "reutilizado"
    assert result.raw_key == "raw/2026-02/Cnaes.zip"
    assert result.bytes_baixados == 0
    assert requests == []
    assert client.requested_paths == []
    assert store.put_calls == []
    assert store.objects["raw/2026-02/Cnaes.zip"] == b"existing-zip"


async def test_zero_byte_raw_is_downloaded_again(http_server, tmp_path):
    payload = b"new-zip"

    async def handler(request):
        return web.Response(body=payload)

    server = await http_server(handler)
    client = FakeClient(["Cnaes.zip"], url_for=server.url)
    store = FakeObjectStore({"raw/2026-02/Cnaes.zip": b""})

    result = await download_raw_file_async(
        "2026-02",
        "Cnaes.zip",
        store=store,
        client=client,
        scratch_dir=tmp_path,
    )

    assert result.status == "baixado"
    assert result.bytes_baixados == len(payload)
    assert store.objects["raw/2026-02/Cnaes.zip"] == payload


async def test_download_failure_returns_sanitized_failure(http_server, tmp_path):
    async def handler(request):
        return web.Response(status=500, text="falha no servidor")

    server = await http_server(handler)
    client = FakeClient(["Cnaes.zip"], url_for=server.url)
    store = FakeObjectStore()

    result = await download_raw_file_async(
        "2026-02",
        "Cnaes.zip",
        store=store,
        client=client,
        scratch_dir=tmp_path,
    )

    assert result.status == "falhou"
    assert result.raw_key is None
    assert result.bytes_baixados == 0
    assert "HTTP 500" in (result.erro or "")
    assert "http://" not in (result.erro or "")
    assert "127.0.0.1" not in (result.erro or "")
    assert "raw/2026-02/Cnaes.zip" not in store.objects
    assert list(tmp_path.iterdir()) == []


async def test_upload_failure_does_not_publish_partial_raw(http_server, tmp_path):
    async def handler(request):
        return web.Response(body=b"zip-bytes")

    server = await http_server(handler)
    client = FakeClient(["Cnaes.zip"], url_for=server.url)
    store = FakeObjectStore()
    store.fail_put = S3OperationError("falha simulada no upload")

    result = await download_raw_file_async(
        "2026-02",
        "Cnaes.zip",
        store=store,
        client=client,
        scratch_dir=tmp_path,
    )

    assert result.status == "falhou"
    assert result.raw_key is None
    assert "raw/2026-02/Cnaes.zip" not in store.objects
    assert list(tmp_path.iterdir()) == []


async def test_bucket_preflight_failure_prevents_download(http_server, tmp_path):
    requests: list[str] = []

    async def handler(request):
        requests.append(request.path)
        return web.Response(body=b"zip-bytes")

    server = await http_server(handler)
    client = FakeClient(["Cnaes.zip"], url_for=server.url)
    store = FakeObjectStore()
    store.fail_ensure = S3OperationError("bucket indisponível")

    result = await download_raw_file_async(
        "2026-02",
        "Cnaes.zip",
        store=store,
        client=client,
        scratch_dir=tmp_path,
    )

    assert result.status == "falhou"
    assert result.raw_key is None
    assert requests == []
    assert store.head_calls == []


async def test_invalid_name_is_rejected_before_storage_access():
    store = FakeObjectStore()
    client = FakeClient(["Cnaes.zip"])

    with pytest.raises(ValueError):
        await download_raw_file_async(
            "2026-02", "../Cnaes.zip", store=store, client=client
        )

    assert store.ensure_calls == 0
    assert client.requested_paths == []


async def test_invalid_competencia_is_rejected_before_storage_access():
    store = FakeObjectStore()
    client = FakeClient(["Cnaes.zip"])

    with pytest.raises(InvalidCompetenciaError):
        await download_raw_file_async(
            "2026-13", "Cnaes.zip", store=store, client=client
        )

    assert store.ensure_calls == 0
    assert client.requested_paths == []


def test_sync_wrapper_reuses_existing_raw_without_network(tmp_path):
    client = FakeClient(["Cnaes.zip"])
    store = FakeObjectStore({"raw/2026-02/Cnaes.zip": b"existing-zip"})

    result = download_raw_file(
        "2026-02",
        "Cnaes.zip",
        store=store,
        client=client,
        scratch_dir=tmp_path,
    )

    assert result.status == "reutilizado"
    assert client.requested_paths == []
    assert list(tmp_path.iterdir()) == []


def test_result_payload_is_small_and_has_no_urls(tmp_path):
    client = FakeClient(["Cnaes.zip"])
    store = FakeObjectStore({"raw/2026-02/Cnaes.zip": b"existing-zip"})

    result = download_raw_file(
        "2026-02", "Cnaes.zip", store=store, client=client, scratch_dir=tmp_path
    )
    payload = json.dumps(result.to_dict())

    assert "http://" not in payload
    assert "https://" not in payload
    assert "existing-zip" not in payload
    assert result.to_dict()["status_download"] == "reutilizado"


def test_filesystem_store_reuses_non_empty_raw_without_network(tmp_path):
    root = tmp_path / "data"
    raw_path = root / RAW_KEY
    raw_path.parent.mkdir(parents=True)
    raw_path.write_bytes(b"existing-zip")
    store = FilesystemObjectStore(root=root)
    client = FakeClient(["Cnaes.zip"])

    result = download_raw_file(
        "2026-02",
        "Cnaes.zip",
        store=store,
        client=client,
        scratch_dir=tmp_path / "scratch",
    )

    assert result.status == "reutilizado"
    assert result.bytes_baixados == 0
    assert client.requested_paths == []
    assert raw_path.read_bytes() == b"existing-zip"


async def test_filesystem_store_downloads_again_when_raw_is_zero_byte(
    http_server, tmp_path
):
    payload = b"new-zip"
    requests: list[str] = []

    async def handler(request):
        requests.append(request.path)
        return web.Response(body=payload)

    server = await http_server(handler)
    root = tmp_path / "data"
    raw_path = root / RAW_KEY
    raw_path.parent.mkdir(parents=True)
    raw_path.write_bytes(b"")
    store = FilesystemObjectStore(root=root)
    client = FakeClient(["Cnaes.zip"], url_for=server.url)
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    result = await download_raw_file_async(
        "2026-02",
        "Cnaes.zip",
        store=store,
        client=client,
        scratch_dir=scratch,
    )

    assert result.status == "baixado"
    assert result.bytes_baixados == len(payload)
    assert raw_path.read_bytes() == payload
    assert requests == ["/Cnaes.zip"]


async def test_filesystem_store_download_failure_leaves_no_raw_object(
    http_server, tmp_path
):
    async def handler(request):
        return web.Response(status=500, text="falha no servidor")

    server = await http_server(handler)
    root = tmp_path / "data"
    root.mkdir()
    store = FilesystemObjectStore(root=root)
    client = FakeClient(["Cnaes.zip"], url_for=server.url)
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    result = await download_raw_file_async(
        "2026-02",
        "Cnaes.zip",
        store=store,
        client=client,
        scratch_dir=scratch,
    )

    assert result.status == "falhou"
    assert result.raw_key is None
    assert not (root / RAW_KEY).exists()
    assert list(scratch.iterdir()) == []
    assert not list(root.rglob("*.part"))
