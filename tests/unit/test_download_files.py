import pytest
from aiohttp import web

from src.download_utils.download_files import DownloadTarget, download_files


def target(server, name):
    return DownloadTarget(nome=name, url=server.url(name))


def part_files(directory):
    return [path for path in directory.iterdir() if path.name.endswith(".part")]


async def test_download_writes_exact_bytes_and_reports_status(http_server, tmp_path):
    async def handler(request):
        return web.Response(body=b"a" * 3000)

    server = await http_server(handler)
    report = await download_files([target(server, "Cnaes.zip")], tmp_path, chunk_size=512)

    assert report.baixados == 1
    assert report.ignorados == 0
    assert report.falhas == 0
    assert report.bytes_baixados == 3000
    assert report.duracao_segundos >= 0
    assert (tmp_path / "Cnaes.zip").read_bytes() == b"a" * 3000
    assert part_files(tmp_path) == []
    result = report.resultados[0]
    assert (result.nome, result.status, result.bytes, result.erro) == (
        "Cnaes.zip",
        "baixado",
        3000,
        None,
    )


async def test_failure_is_isolated_and_error_is_sanitized(http_server, tmp_path):
    async def handler(request):
        if request.match_info["tail"] == "bad.zip":
            return web.Response(status=500, text="falha")
        return web.Response(body=b"ok")

    server = await http_server(handler)
    targets = [target(server, "bad.zip"), target(server, "good.zip")]
    report = await download_files(targets, tmp_path, concorrencia=2)

    assert report.baixados == 1
    assert report.falhas == 1
    assert (tmp_path / "good.zip").read_bytes() == b"ok"
    assert not (tmp_path / "bad.zip").exists()
    failed = next(result for result in report.resultados if result.status == "falhou")
    assert failed.erro is not None
    assert "HTTP 500" in failed.erro
    assert "http://" not in failed.erro
    assert part_files(tmp_path) == []


async def test_short_body_is_reported_as_failure(http_server, tmp_path):
    async def handler(request):
        response = web.StreamResponse()
        response.content_length = 100
        # Close the connection after the truncated body so the client sees EOF
        # instead of waiting for the missing bytes until sock_read expires.
        response.force_close()
        await response.prepare(request)
        await response.write(b"x" * 10)
        try:
            await response.write_eof()
        except ConnectionError:
            pass
        return response

    server = await http_server(handler)
    report = await download_files([target(server, "short.zip")], tmp_path)

    assert report.falhas == 1
    assert not (tmp_path / "short.zip").exists()
    assert part_files(tmp_path) == []


async def test_results_are_ordered_by_name(http_server, tmp_path):
    async def handler(request):
        return web.Response(body=b"ok")

    server = await http_server(handler)
    targets = [target(server, "b.zip"), target(server, "a.zip")]
    report = await download_files(targets, tmp_path, concorrencia=2)

    assert [result.nome for result in report.resultados] == ["a.zip", "b.zip"]


async def test_existing_non_empty_file_is_ignored_without_request(http_server, tmp_path):
    requests: list[str] = []

    async def handler(request):
        requests.append(request.path)
        return web.Response(body=b"new")

    server = await http_server(handler)
    (tmp_path / "Cnaes.zip").write_bytes(b"existing")

    report = await download_files([target(server, "Cnaes.zip")], tmp_path)

    assert report.ignorados == 1
    assert report.baixados == 0
    assert report.bytes_baixados == 0
    assert requests == []
    assert (tmp_path / "Cnaes.zip").read_bytes() == b"existing"


async def test_zero_byte_file_is_downloaded_again(http_server, tmp_path):
    async def handler(request):
        return web.Response(body=b"new")

    server = await http_server(handler)
    (tmp_path / "Cnaes.zip").write_bytes(b"")

    report = await download_files([target(server, "Cnaes.zip")], tmp_path)

    assert report.baixados == 1
    assert (tmp_path / "Cnaes.zip").read_bytes() == b"new"


@pytest.mark.parametrize("kwargs", [{"concorrencia": 0}, {"concorrencia": -1}, {"chunk_size": 0}])
async def test_invalid_parameters_fail_before_any_request(tmp_path, kwargs):
    with pytest.raises(ValueError):
        await download_files([], tmp_path, **kwargs)


async def test_leftover_part_file_is_never_used_as_complete(http_server, tmp_path):
    async def handler(request):
        return web.Response(body=b"fresh")

    server = await http_server(handler)
    stale = tmp_path / ".Cnaes.zip.999.deadbeef.part"
    stale.write_bytes(b"stale")

    report = await download_files([target(server, "Cnaes.zip")], tmp_path)

    assert report.baixados == 1
    assert (tmp_path / "Cnaes.zip").read_bytes() == b"fresh"
