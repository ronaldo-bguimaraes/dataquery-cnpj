"""Observabilidade: logs estruturados, sanitização de PII/segredos e métricas (FR-041/043/044)."""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any

from src.query.domain.masking import redact_cpf_patterns

# DSNs: postgresql://user:password@host/db → esconder a senha.
_DSN_PASSWORD = re.compile(r"(://[^:/@\s]+:)([^@\s]+)@")

#: Campos que não podem ser logados por valor (só tamanho/contagem).
_CONTENT_FIELDS = {"registro_integral", "row_payload", "linhas_completas"}


def sanitize_dsn(value: Any) -> str:
    """Remove credenciais de DSNs antes de logar."""

    return _DSN_PASSWORD.sub(r"\1***@", str(value or ""))


def sanitize_text(value: Any) -> str:
    """Sanitiza texto livre: CPFs mascarados, itens sensíveis removidos."""

    text = redact_cpf_patterns(value)
    for field in _CONTENT_FIELDS:
        text = re.sub(rf'"{field}":\s*"[^"]*"', f'"{field}": "<omitido>"', text)
    return text


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    """Emite log linha-JSON estruturado, sem PII e sem segredos (FR-043)."""

    payload: dict[str, Any] = {"event": sanitize_text(event)}
    for key, value in fields.items():
        if key == "database_url":
            payload[key] = sanitize_dsn(value)
        elif isinstance(value, str):
            payload[key] = sanitize_text(value)
        else:
            payload[key] = value
    logger.info(json.dumps(payload, ensure_ascii=False, sort_keys=True))


class MetricsRecorder:
    """Contadores e duração por etapa de execução (FR-044)."""

    def __init__(self, load_id: str, etapa: str) -> None:
        self.load_id = load_id
        self.etapa = etapa
        self._counters: dict[str, int] = {}
        self._started = time.monotonic()
        self._finished = False

    def add(self, metric: str, amount: int = 1) -> None:
        self._counters[metric] = self._counters.get(metric, 0) + amount

    def finish(self) -> None:
        self._duracao = time.monotonic() - self._started
        self._finished = True

    def snapshot(self) -> dict[str, Any]:
        """Snapshot sanitizado, pronto para log/metadata."""

        payload: dict[str, Any] = {
            "load_id": self.load_id,
            "etapa": self.etapa,
            **self._counters,
        }
        if self._finished:
            payload["duracao_segundos"] = round(max(self._duracao, 0.0), 6)
        return payload
