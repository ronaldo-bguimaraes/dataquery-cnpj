from __future__ import annotations

import socket
from dataclasses import dataclass

import pytest
from aiohttp import web


@dataclass
class LocalHTTPServer:
    """Small aiohttp server used by downloader and orchestration tests."""

    port: int
    runner: web.AppRunner

    def url(self, path: str) -> str:
        return f"http://127.0.0.1:{self.port}/{path.lstrip('/')}"


@pytest.fixture
def tmp_download_dir(tmp_path):
    return tmp_path / "cnpj"


@pytest.fixture
async def http_server():
    servers: list[LocalHTTPServer] = []

    async def start(handler):
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        server = LocalHTTPServer(port=port, runner=runner)
        servers.append(server)
        return server

    try:
        yield start
    finally:
        for server in servers:
            await server.runner.cleanup()


@pytest.fixture
def block_network(monkeypatch):
    def guard(*args, **kwargs):
        raise AssertionError("network access is not allowed during DAG import")

    monkeypatch.setattr(socket.socket, "connect", guard)
    monkeypatch.setattr(socket, "create_connection", guard)
