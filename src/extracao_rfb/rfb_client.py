import re

from functools import cached_property
from src.webdav.auto_webdav_client import get_webdav_client


class ClientRFB:
    _CNPJ_PATH = "Dados/Cadastros/CNPJ"
    _RFB_HOSTNAME = "arquivos.receitafederal.gov.br"

    @cached_property
    def client(self):
        return get_webdav_client(self._RFB_HOSTNAME)

    @cached_property
    def webdav_hostname(self):
        return self.client.webdav.hostname

    _PATTERN_COMPETENCIA = re.compile(r"(?P<competencia>\d{4}-\d{2})/")

    @classmethod
    def capture_competencia(cls, filename: str):
        result = cls._PATTERN_COMPETENCIA.search(filename)
        if result:
            return result.group("competencia")
        return None

    def list_competencias(self):
        for fname in self.client.list(self._CNPJ_PATH):
            competencia = self.capture_competencia(fname)
            if competencia:
                yield competencia

    def list_files(self, competencia: str):
        baseurl = f"{self._CNPJ_PATH}/{competencia}"
        for arquivo in self.client.list(baseurl):
            if not arquivo.endswith("/"):
                yield f"{baseurl}/{arquivo}"

    def get_file_url(self, filepath: str):
        return f"{self.webdav_hostname}/{filepath}"
