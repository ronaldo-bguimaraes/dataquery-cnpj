import aiohttp
import pytest

from src.download_utils.download_files import DownloadTarget
from src.download_utils.redaction import describe_exception, redact_urls


def test_redact_urls_replaces_full_url():
    text = (
        "falha em https://arquivos.receitafederal.gov.br/public.php/dav/files/"
        "TOKEN/Cnaes.zip (timeout)"
    )
    assert redact_urls(text) == "falha em <url> (timeout)"


def test_redact_urls_leaves_plain_text_untouched():
    assert redact_urls("sem url aqui") == "sem url aqui"


def test_redact_urls_handles_multiple_urls():
    text = "a https://host/x b http://host/y"
    assert redact_urls(text) == "a <url> b <url>"


def test_describe_client_response_error_reports_only_status():
    error = aiohttp.ClientResponseError(
        request_info=None,
        history=(),
        status=500,
        message="Internal Server Error",
    )
    described = describe_exception(error)
    assert described == "HTTP 500 Internal Server Error"
    assert "http://" not in described


def test_describe_client_error_reports_class_name_only():
    assert describe_exception(aiohttp.ClientConnectionError("boom")) == "ClientConnectionError"


def test_describe_generic_exception_redacts_url():
    described = describe_exception(RuntimeError("falha em https://host/public.php/dav/files/TOKEN"))
    assert described == "RuntimeError: falha em <url>"
    assert "TOKEN" not in described


def test_download_target_repr_hides_url():
    target = DownloadTarget(
        nome="Cnaes.zip",
        url="https://host/public.php/dav/files/TOKEN/Cnaes.zip",
    )
    representation = repr(target)
    assert "Cnaes.zip" in representation
    assert "TOKEN" not in representation
    assert "public.php" not in representation
