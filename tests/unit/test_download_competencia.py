from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from aiohttp import web

from src.extracao_rfb.download_competencia import (
    DownloadError,
    EmptyCompetenciaError,
    ExecutionInProgressError,
    OutputDirectoryError,
    download_competencia_async,
)
from src.extracao_rfb.competencia import InvalidLimitError
from src.extracao_rfb.rfb_client import ListingError


class FakeClient:
    base_path = "Dados/Cadastros/CNPJ"

    def __init__(self, files, url_for, error=None):
        self.files = list(files)
        self.url_for = url_for
        self.error = error
        self.list_calls = 0

    def list_files(self, competencia):
        self.list_calls += 1
        if self.error is not None:
            raise self.error
        return list(self.files)

    def get_file_url(self, caminho):
        return self.url_for(caminho.rstrip("/").rsplit("/", 1)[-1])


def make_handler(payloads, failures=()):
    async def handler(request):
        name = request.match_info["tail"].rsplit("/", 1)[-1]
        if name in failures:
            return web.Response(status=500, text="falha")
        return web.Response(body=payloads.get(name, b"ok"))

    return handler


async def test_downloads_competencia_and_returns_summary(http_server, tmp_download_dir):
    payloads = {"Cnaes.zip": b"a" * 10, "Empresas0.zip": b"b" * 20}
    server = await http_server(make_handler(payloads))
    client = FakeClient(payloads, server.url)

    summary = await download_competencia_async("2026-02", tmp_download_dir, client=client)

    assert summary.competencia == "2026-02"
    assert Path(summary.output_dir) == (tmp_download_dir / "2026-02").resolve()
    assert summary.encontrados == 2
    assert summary.selecionados == 2
    assert summary.baixados == 2
    assert summary.ignorados == 0
    assert summary.falhas == 0
    assert summary.bytes_baixados == 30
    assert summary.parcial is False
    assert [result.nome for result in summary.arquivos] == ["Cnaes.zip", "Empresas0.zip"]
    assert (tmp_download_dir / "2026-02" / "Cnaes.zip").read_bytes() == b"a" * 10
    assert (tmp_download_dir / "2026-02" / "Empresas0.zip").read_bytes() == b"b" * 20
    json.dumps(summary.to_dict())


async def test_empty_listing_raises_empty_competencia(http_server, tmp_download_dir):
    server = await http_server(make_handler({}))
    client = FakeClient([], server.url)

    with pytest.raises(EmptyCompetenciaError):
        await download_competencia_async("2026-02", tmp_download_dir, client=client)


async def test_listing_failure_raises_without_downloading(http_server, tmp_download_dir):
    server = await http_server(make_handler({}))
    client = FakeClient([], server.url, error=ListingError("fonte indisponível"))

    with pytest.raises(ListingError):
        await download_competencia_async("2026-02", tmp_download_dir, client=client)

    assert {path.name for path in (tmp_download_dir / "2026-02").iterdir()} <= {
        ".download.lock"
    }


async def test_one_failed_file_raises_download_error_and_preserves_success(http_server, tmp_download_dir):
    payloads = {"bad.zip": b"bad", "good.zip": b"good"}
    server = await http_server(make_handler(payloads, failures={"bad.zip"}))
    client = FakeClient(payloads, server.url)

    with pytest.raises(DownloadError) as excinfo:
        await download_competencia_async("2026-02", tmp_download_dir, client=client)

    summary = excinfo.value.summary
    assert summary.falhas == 1
    assert summary.baixados == 1
    assert "bad.zip" in str(excinfo.value)
    assert (tmp_download_dir / "2026-02" / "good.zip").read_bytes() == b"good"
    assert not (tmp_download_dir / "2026-02" / "bad.zip").exists()


async def test_invalid_output_directory_fails_before_listing(http_server, tmp_path):
    server = await http_server(make_handler({}))
    client = FakeClient(["Cnaes.zip"], server.url)
    invalid = tmp_path / "arquivo"
    invalid.write_bytes(b"x")

    with pytest.raises(OutputDirectoryError):
        await download_competencia_async("2026-02", invalid, client=client)

    assert client.list_calls == 0


async def test_summary_and_error_do_not_leak_token(http_server, tmp_download_dir):
    payloads = {"bad.zip": b"bad"}
    server = await http_server(make_handler(payloads, failures={"bad.zip"}))
    client = FakeClient(
        payloads,
        lambda name: server.url(f"public.php/dav/files/SECRET/{name}"),
    )

    with pytest.raises(DownloadError) as excinfo:
        await download_competencia_async("2026-02", tmp_download_dir, client=client)

    serialized = json.dumps(excinfo.value.summary.to_dict())
    assert "SECRET" not in serialized
    assert "public.php" not in serialized
    assert "SECRET" not in str(excinfo.value)
    assert "bad.zip" in serialized


async def test_limit_selects_first_files_and_marks_partial(http_server, tmp_download_dir):
    payloads = {
        "Cnaes.zip": b"cnaes",
        "Empresas0.zip": b"empresas",
        "Socios0.zip": b"socios",
    }
    requests: list[str] = []

    async def handler(request):
        name = request.match_info["tail"].rsplit("/", 1)[-1]
        requests.append(name)
        return web.Response(body=payloads[name])

    server = await http_server(handler)
    client = FakeClient(payloads, server.url)

    summary = await download_competencia_async(
        "2026-02", tmp_download_dir, limite=1, client=client
    )

    assert summary.encontrados == 3
    assert summary.selecionados == 1
    assert summary.parcial is True
    assert summary.baixados == 1
    assert [result.nome for result in summary.arquivos] == ["Cnaes.zip"]
    assert requests == ["Cnaes.zip"]
    assert (tmp_download_dir / "2026-02" / "Cnaes.zip").read_bytes() == b"cnaes"
    assert not (tmp_download_dir / "2026-02" / "Empresas0.zip").exists()
    assert not (tmp_download_dir / "2026-02" / "Socios0.zip").exists()


async def test_limit_with_existing_first_file_does_not_download_second(
    http_server, tmp_download_dir
):
    payloads = {
        "Cnaes.zip": b"new-cnaes",
        "Empresas0.zip": b"new-empresas",
    }
    requests: list[str] = []

    async def handler(request):
        name = request.match_info["tail"].rsplit("/", 1)[-1]
        requests.append(name)
        return web.Response(body=payloads[name])

    server = await http_server(handler)
    client = FakeClient(payloads, server.url)
    directory = tmp_download_dir / "2026-02"
    directory.mkdir(parents=True)
    (directory / "Cnaes.zip").write_bytes(b"existing")

    summary = await download_competencia_async(
        "2026-02", tmp_download_dir, limite=1, client=client
    )

    assert summary.encontrados == 2
    assert summary.selecionados == 1
    assert summary.parcial is True
    assert summary.baixados == 0
    assert summary.ignorados == 1
    assert summary.bytes_baixados == 0
    assert requests == []
    assert (directory / "Cnaes.zip").read_bytes() == b"existing"
    assert not (directory / "Empresas0.zip").exists()


async def test_limit_above_total_is_not_partial(http_server, tmp_download_dir):
    payloads = {"Cnaes.zip": b"a", "Empresas0.zip": b"b", "Socios0.zip": b"c"}
    server = await http_server(make_handler(payloads))
    client = FakeClient(payloads, server.url)

    summary = await download_competencia_async(
        "2026-02", tmp_download_dir, limite=100, client=client
    )

    assert summary.encontrados == 3
    assert summary.selecionados == 3
    assert summary.parcial is False
    assert summary.baixados == 3


async def test_invalid_limit_fails_before_listing(http_server, tmp_download_dir):
    server = await http_server(make_handler({}))
    client = FakeClient(["Cnaes.zip"], server.url)

    with pytest.raises(InvalidLimitError):
        await download_competencia_async(
            "2026-02", tmp_download_dir, limite=0, client=client
        )

    assert client.list_calls == 0


async def test_stale_part_is_removed_before_listing(http_server, tmp_download_dir):
    server = await http_server(make_handler({}))
    directory = tmp_download_dir / "2026-02"
    directory.mkdir(parents=True)
    stale = directory / ".Cnaes.zip.123.deadbeef.part"
    stale.write_bytes(b"incomplete")
    client = FakeClient([], server.url, error=ListingError("fonte indisponível"))

    with pytest.raises(ListingError):
        await download_competencia_async("2026-02", tmp_download_dir, client=client)

    assert not stale.exists()


async def test_concurrent_runs_for_same_competencia_fail_fast(
    http_server, tmp_download_dir
):
    async def handler(request):
        await asyncio.sleep(0.5)
        return web.Response(body=b"ok")

    server = await http_server(handler)
    client = FakeClient(["Cnaes.zip"], server.url)

    first = asyncio.create_task(
        download_competencia_async(
            "2026-02", tmp_download_dir, client=client, lock_timeout=2
        )
    )

    for _ in range(200):
        if client.list_calls:
            break
        await asyncio.sleep(0.01)
    assert client.list_calls == 1

    with pytest.raises(ExecutionInProgressError):
        await download_competencia_async(
            "2026-02", tmp_download_dir, client=client, lock_timeout=0.2
        )

    assert client.list_calls == 1
    summary = await first
    assert summary.baixados == 1


async def test_second_run_ignores_every_file(http_server, tmp_download_dir):
    payloads = {"Cnaes.zip": b"cnaes", "Empresas0.zip": b"empresas"}
    requests: list[str] = []

    async def handler(request):
        name = request.match_info["tail"].rsplit("/", 1)[-1]
        requests.append(name)
        return web.Response(body=payloads[name])

    server = await http_server(handler)
    client = FakeClient(payloads, server.url)

    first = await download_competencia_async("2026-02", tmp_download_dir, client=client)
    assert first.baixados == 2

    requests.clear()
    second = await download_competencia_async(
        "2026-02", tmp_download_dir, client=client
    )

    assert second.baixados == 0
    assert second.ignorados == 2
    assert second.bytes_baixados == 0
    assert requests == []
    assert (tmp_download_dir / "2026-02" / "Cnaes.zip").read_bytes() == b"cnaes"


async def test_resume_after_partial_failure_only_downloads_missing(
    http_server, tmp_download_dir
):
    payloads = {"bad.zip": b"bad", "good.zip": b"good"}
    failures = {"bad.zip"}
    requests: list[str] = []

    async def handler(request):
        name = request.match_info["tail"].rsplit("/", 1)[-1]
        requests.append(name)
        if name in failures:
            return web.Response(status=500, text="falha")
        return web.Response(body=payloads[name])

    server = await http_server(handler)
    client = FakeClient(payloads, server.url)

    with pytest.raises(DownloadError):
        await download_competencia_async("2026-02", tmp_download_dir, client=client)

    directory = tmp_download_dir / "2026-02"
    assert (directory / "good.zip").read_bytes() == b"good"
    good_mtime = (directory / "good.zip").stat().st_mtime_ns

    failures.clear()
    requests.clear()
    summary = await download_competencia_async(
        "2026-02", tmp_download_dir, client=client
    )

    assert summary.baixados == 1
    assert summary.ignorados == 1
    assert requests == ["bad.zip"]
    assert (directory / "bad.zip").read_bytes() == b"bad"
    assert (directory / "good.zip").stat().st_mtime_ns == good_mtime
