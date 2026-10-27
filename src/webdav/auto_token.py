from functools import lru_cache
import re

import requests

from urllib.parse import urlparse


@lru_cache(maxsize=256)
def get_webdav_token(hostname: str) -> str:
    url = f"https://{hostname}"

    try:
        resp = requests.get(url, allow_redirects=True, timeout=30)
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Erro ao obter acesso público à fonte: {type(exc).__name__}"
        ) from None

    urlpath = urlparse(resp.url).path

    pattern = re.compile(r"s/(?P<token>\w+)", re.IGNORECASE)
    result = pattern.search(urlpath)

    if result:
        return result.group("token")

    raise RuntimeError("Erro ao extrair token da url") from None
