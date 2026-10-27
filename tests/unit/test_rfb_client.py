import pytest
from webdav3.exceptions import RemoteResourceNotFound, WebDavException

from src.extracao_rfb.rfb_client import ClientRFB, ListingError


class StubWebDav:
    def __init__(self, entries=None, error=None):
        self.entries = entries or []
        self.error = error
        self.listed: list[str] = []

    def list(self, path):
        self.listed.append(path)
        if self.error is not None:
            raise self.error
        return list(self.entries)


def make_client(entries=None, error=None):
    stub = StubWebDav(entries=entries, error=error)
    client = ClientRFB(
        client=stub,
        hostname="https://example.test/dav",
        base_path="Dados/Cadastros/CNPJ",
    )
    return client, stub


def test_list_files_returns_sorted_basenames_without_directories():
    client, stub = make_client(["Empresas0.zip", "2026-02/", "Cnaes.zip", "Empresas1.zip"])

    assert client.list_files("2026-02") == ["Cnaes.zip", "Empresas0.zip", "Empresas1.zip"]
    assert stub.listed == ["Dados/Cadastros/CNPJ/2026-02"]


def test_list_files_translates_remote_not_found_to_empty_list():
    client, _ = make_client(error=RemoteResourceNotFound("Remote resource not found"))

    assert client.list_files("2099-01") == []


def test_list_files_raises_listing_error_without_url_or_token():
    client, _ = make_client(
        error=WebDavException("failed https://host/public.php/dav/files/SECRET/file")
    )

    with pytest.raises(ListingError) as excinfo:
        client.list_files("2026-02")

    message = str(excinfo.value)
    assert "2026-02" in message
    assert "SECRET" not in message
    assert "http" not in message


def test_list_files_skips_unsafe_names():
    client, _ = make_client(["ok.zip", "../evil.zip", "..", "/etc/passwd"])

    assert client.list_files("2026-02") == ["ok.zip"]


def test_get_file_url_builds_url_without_token_in_attributes():
    client, _ = make_client()

    url = client.get_file_url("Dados/Cadastros/CNPJ/2026-02/Cnaes.zip")

    assert url == "https://example.test/dav/Dados/Cadastros/CNPJ/2026-02/Cnaes.zip"
