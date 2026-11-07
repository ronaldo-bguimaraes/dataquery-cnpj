"""Ingestão paralela e confiável de dados CNPJ (feature 003)."""

from __future__ import annotations

from src.ingest_parallel.config import IngestConfig, load_config

__all__ = ["IngestConfig", "load_config"]
