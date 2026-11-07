"""T004 — load_id determinístico e configuração de carga (FR-014)."""

from __future__ import annotations

import pytest

from src.ingest_parallel.config import IngestConfig, load_config
from src.ingest_parallel.config import derive_load_id


class TestLoadIdDeterministico:
    def test_load_id_compoe_competencia_e_versao(self):
        load_id = derive_load_id("2026-10", "a" * 64)
        assert load_id == "2026-10-aaaaaaaa"

    def test_load_id_estavel_entre_execucoes(self):
        first = derive_load_id("2026-10", "f" * 64)
        second = derive_load_id("2026-10", "f" * 64)
        assert first == second

    def test_load_id_muda_quando_fonte_muda(self):
        assert derive_load_id("2026-10", "a" * 64) != derive_load_id("2026-10", "b" * 64)

    def test_load_id_rejeita_competencia_invalida(self):
        with pytest.raises(ValueError):
            derive_load_id("10-2026", "a" * 64)


class TestLoadConfig:
    base_env = {
        "CNPJ_INGEST_COMPETENCIA": "2026-10",
        "CNPJ_INGEST_BRONZE_DIR": "/tmp/cnpj/bronze",
        "CNPJ_INGEST_SERVING_DSN": "postgresql://u:s@h:5432/db",
    }

    def test_config_minima_via_env(self, monkeypatch):
        for key, value in self.base_env.items():
            monkeypatch.setenv(key, value)
        for key in ("CNPJ_INGEST_MAX_WORKERS",):
            monkeypatch.delenv(key, raising=False)
        config = load_config()
        assert config.competencia == "2026-10"
        assert config.max_workers == 8  # default do ambiente alvo (FR-023)

    def test_config_respeita_max_workers(self, monkeypatch):
        for key, value in {**self.base_env, "CNPJ_INGEST_MAX_WORKERS": "4"}.items():
            monkeypatch.setenv(key, value)
        config = load_config()
        assert config.max_workers == 4

    def test_target_file_size_padrao_512mb(self, monkeypatch):
        for key, value in self.base_env.items():
            monkeypatch.setenv(key, value)
        config = load_config()
        assert config.target_file_size_bytes == 2**29

    def test_codigo_versao_presente(self, monkeypatch):
        for key, value in self.base_env.items():
            monkeypatch.setenv(key, value)
        config = load_config()
        assert config.code_version  # git SHA — não pode ficar vazio

    def test_competencia_invalida_rejeita(self, monkeypatch):
        monkeypatch.setenv("CNPJ_INGEST_COMPETENCIA", "10/2026")
        monkeypatch.setenv("CNPJ_INGEST_BRONZE_DIR", "/tmp/x")
        with pytest.raises(ValueError):
            load_config()
