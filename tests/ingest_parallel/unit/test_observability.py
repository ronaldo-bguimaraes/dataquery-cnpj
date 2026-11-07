"""T006 — Observabilidade: sanitização de PII e logs estruturados (FR-043/044)."""

from __future__ import annotations

import json
import logging

from src.ingest_parallel.observability import (
    MetricsRecorder,
    log_event,
    sanitize_dsn,
    sanitize_text,
)


class TestSanitizacao:
    def test_cpf_completo_mascarado(self):
        saida = sanitize_text("erro no registro do CPF 123.456.789-09 do socio")
        assert "123.456.789-09" not in saida
        assert "***" in saida

    def test_cpf_com_digitos_separados_espaco_ponto(self):
        texto = sanitize_text("12345678909")
        assert "12345678909" not in texto

    def test_texto_sem_pii_inalterado(self):
        texto = "empresa normalizada com cnpj 00000000000191 mascarado"
        assert sanitize_text(texto) == texto

    def test_none_retorna_vazio(self):
        assert sanitize_text(None) == ""

    def test_dsn_senha_mascarada(self):
        saida = sanitize_dsn("postgresql://user:senha-boa@db:5432/base")
        assert "senha-boa" not in saida
        assert "postgresql://user:***@db:5432/base" == saida


class TestLogEvent:
    def test_evento_estruturado_json(self, caplog):
        logger = logging.getLogger("test_observability")
        with caplog.at_level(logging.INFO):
            log_event(
                logger,
                "partition.done",
                load_id="2026-10-aaaa",
                partition_id="empresas-03",
                rows=123,
                motivo="CPF 123.456.789-09 reprovado",
            )
        record = caplog.records[0]
        payload = json.loads(record.getMessage())
        assert payload["event"] == "partition.done"
        assert payload["load_id"] == "2026-10-aaaa"
        assert payload["partition_id"] == "empresas-03"
        assert payload["rows"] == 123
        assert "123.456.789-09" not in record.getMessage()

    def test_mensagens_estruturadas_sao_json_valido(self, caplog):
        logger = logging.getLogger("test_observability")
        with caplog.at_level(logging.INFO):
            log_event(logger, "run.start", competencia="2026-10")
        json.loads(caplog.records[0].getMessage())  # não levanta


class TestMetricsRecorder:
    def test_contadores_por_etapa(self):
        metrics = MetricsRecorder("2026-10-aaaa", "normalize")
        metrics.add("rows_in", 100)
        metrics.add("rows_in", 50)
        metrics.add("rejects", 3)
        snapshot = metrics.snapshot()
        assert snapshot["rows_in"] == 150
        assert snapshot["rejects"] == 3
        assert snapshot["load_id"] == "2026-10-aaaa"
        assert snapshot["etapa"] == "normalize"

    def test_duracao_registrada(self):
        metrics = MetricsRecorder("run", "serving")
        metrics.finish()
        snapshot = metrics.snapshot()
        assert snapshot["duracao_segundos"] >= 0.0

    def test_sem_conteudo_integral_de_registro_no_snapshot(self):
        metrics = MetricsRecorder("run", "normalize")
        metrics.add("rows_in", 1)
        payload = json.dumps(metrics.snapshot())
        assert "registro_integral" not in payload
