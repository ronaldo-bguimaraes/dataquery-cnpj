from webdav3.client import Client

from src.webdav.auto_token import get_webdav_token


def get_webdav_url(hostname: str):
    try:
        webdav_token = get_webdav_token(hostname)
    except:
        raise RuntimeError("Erro ao capturar o token automaticamente")

    return f"https://{hostname}/public.php/dav/files/{webdav_token}"


def get_webdav_client(hostname: str):
    options = {
        "webdav_hostname": get_webdav_url(hostname),
        "webdav_login": "",
        "webdav_password": "",
    }
    return Client(options)
