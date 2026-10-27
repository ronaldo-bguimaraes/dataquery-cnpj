from __future__ import annotations

import re

from webdav3.exceptions import RemoteResourceNotFound

from src.download_utils.redaction import describe_exception
from src.webdav.auto_webdav_client import get_webdav_client


class ListingError(RuntimeError):
    """Raised when the RFB listing cannot be obtained."""


class ClientRFB:
    _CNPJ_PATH = "Dados/Cadastros/CNPJ"
    _RFB_HOSTNAME = "arquivos.receitafederal.gov.br"

    _PATTERN_COMPETENCIA = re.compile(r"(?P<competencia>\d{4}-\d{2})/")

    def __init__(
        self,
        hostname: str | None = None,
        base_path: str | None = None,
        client=None,
    ) -> None:
        self._hostname = hostname or self._RFB_HOSTNAME
        self._base_path = base_path or self._CNPJ_PATH
        self._injected_client = client
        self._client_cache = None

    @property
    def client(self):
        if self._injected_client is not None:
            return self._injected_client
        if self._client_cache is None:
            self._client_cache = get_webdav_client(self._hostname)
        return self._client_cache

    @property
    def webdav_hostname(self) -> str:
        if self._injected_client is not None:
            return self._hostname
        return self.client.webdav.hostname

    @property
    def base_path(self) -> str:
        """Base WebDAV path that contains the competência directories."""

        return self._base_path

    @classmethod
    def capture_competencia(cls, filename: str):
        result = cls._PATTERN_COMPETENCIA.search(filename)
        if result:
            return result.group("competencia")
        return None

    def list_competencias(self):
        for fname in self.client.list(self._base_path):
            competencia = self.capture_competencia(fname)
            if competencia:
                yield competencia

    @staticmethod
    def _valid_name(name: str) -> bool:
        return bool(name) and name not in {".", ".."} and "/" not in name and "\\" not in name

    def list_files(self, competencia: str) -> list[str]:
        """Return the sorted file basenames for ``competencia``.

        Directory entries are discarded. A missing competência directory (404) is treated
        as an empty listing so the orchestrator can raise ``EmptyCompetenciaError``.
        """

        path = f"{self._base_path}/{competencia}"
        try:
            entries = self.client.list(path)
        except RemoteResourceNotFound:
            return []
        except Exception as exc:  # noqa: BLE001 - translated to a domain error
            raise ListingError(
                f"não foi possível listar a competência {competencia}: {describe_exception(exc)}"
            ) from None

        names = {
            entry
            for entry in entries
            if entry
            and not entry.endswith("/")
            and self._valid_name(entry)
        }
        return sorted(names)

    def get_file_url(self, filepath: str) -> str:
        return f"{self.webdav_hostname.rstrip('/')}/{filepath.lstrip('/')}"
