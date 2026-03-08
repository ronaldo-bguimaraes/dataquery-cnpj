from functools import lru_cache
import re
import requests

from urllib.parse import urlparse


@lru_cache(maxsize=256)
def get_webdav_token(hostname: str) -> str:

    url = f"https://{hostname}"

    resp = requests.get(url, allow_redirects=True)
    urlpath = urlparse(resp.url).path

    pattern = re.compile("s/(?P<token>\\w+)", re.IGNORECASE)
    result = pattern.search(urlpath)

    if result:
        return result.group("token")
    else:
        raise RuntimeError("Erro ao extrair token da url")
