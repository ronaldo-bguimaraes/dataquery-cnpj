import logging

from webdav3.client import Client

from src.download_utils.redaction import describe_exception
from src.webdav.auto_token import get_webdav_token

log = logging.getLogger(__name__)


def get_webdav_url(hostname: str):
    try:
        webdav_token = get_webdav_token(hostname)
    except Exception as exc:  # noqa: BLE001 - sanitized and re-raised
        log.error("Erro ao capturar o token automaticamente: %s", describe_exception(exc))
        raise RuntimeError("Erro ao capturar o token automaticamente") from None

    return f"https://{hostname}/public.php/dav/files/{webdav_token}"


def get_webdav_client(hostname: str):
    options = {
        "webdav_hostname": get_webdav_url(hostname),
        "webdav_login": "",
        "webdav_password": "",
    }
    return Client(options)
