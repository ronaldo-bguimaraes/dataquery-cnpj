from __future__ import annotations

from pathlib import Path

from aiohttp import web

from src.extracao_rfb.download_competencia import download_competencia_async


class FakeClient:
    base_path = "Dados/Cadastros/CNPJ"

    def __init__(self, files, url_for):
        self.files = list(files)
        self.url_for = url_for

    def list_files(self, competencia):
        return list(self.files)

    def get_file_url(self, caminho):
        return self.url_for(caminho.rsplit("/", 1)[-1])


async def test_end_to_end_download_local_server(http_server, tmp_download_dir):
    payloads = {
        "Cnaes.zip": b"cnaes",
        "Empresas0.zip": b"empresas-0",
        "Socios0.zip": b"socios-0",
    }

    async def handler(request):
        name = request.match_info["tail"].rsplit("/", 1)[-1]
        return web.Response(body=payloads[name])

    server = await http_server(handler)
    client = FakeClient(payloads, server.url)

    summary = await download_competencia_async("2026-02", tmp_download_dir, client=client)

    directory = tmp_download_dir / "2026-02"
    assert summary.encontrados == 3
    assert summary.baixados == 3
    assert summary.falhas == 0
    assert summary.parcial is False
    assert summary.bytes_baixados == sum(len(body) for body in payloads.values())
    for name, body in payloads.items():
        assert (directory / name).read_bytes() == body
    assert [path for path in directory.iterdir() if path.suffix == ".part"] == []


async def test_second_run_is_idempotent_and_preserves_files(http_server, tmp_download_dir):
    payloads = {
        "Cnaes.zip": b"cnaes",
        "Empresas0.zip": b"empresas-0",
        "Socios0.zip": b"socios-0",
    }

    async def handler(request):
        name = request.match_info["tail"].rsplit("/", 1)[-1]
        return web.Response(body=payloads[name])

    server = await http_server(handler)
    client = FakeClient(payloads, server.url)

    first = await download_competencia_async("2026-02", tmp_download_dir, client=client)
    assert first.baixados == 3

    directory = tmp_download_dir / "2026-02"
    before = {
        name: ((directory / name).read_bytes(), (directory / name).stat().st_mtime_ns)
        for name in payloads
    }

    second = await download_competencia_async(
        "2026-02", tmp_download_dir, client=client
    )

    assert second.baixados == 0
    assert second.ignorados == 3
    assert second.bytes_baixados == 0
    assert second.parcial is False
    for name, (body, mtime) in before.items():
        path = directory / name
        assert path.read_bytes() == body
        assert path.stat().st_mtime_ns == mtime
