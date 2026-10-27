from __future__ import annotations

import json

import pytest

import src.pipeline.raw_bronze as pipeline
from src.conversao.bronze import BronzeFileResult
from src.extracao_rfb.download_raw import RawFileResult, RawListing


def raw(
    nome="Cnaes.zip",
    *,
    status="baixado",
    raw_key="raw/2026-02/Cnaes.zip",
    bytes_baixados=10,
    erro=None,
):
    return RawFileResult(
        nome=nome,
        raw_key=raw_key,
        status=status,
        bytes_baixados=bytes_baixados,
        duracao_segundos=1.5,
        erro=erro,
    )


def bronze(
    nome="Cnaes.zip",
    *,
    status="publicado",
    raw_key="raw/2026-02/Cnaes.zip",
    bronze_key="bronze/2026-02/Cnaes.parquet",
    bytes_parquet=5,
    linhas=3,
    erro=None,
):
    return BronzeFileResult(
        nome=nome,
        raw_key=raw_key,
        bronze_key=bronze_key,
        status=status,
        bytes_parquet=bytes_parquet,
        linhas=linhas,
        duracao_segundos=2.5,
        erro=erro,
    )


def test_download_is_followed_by_conversion_for_the_same_file(monkeypatch):
    calls: list[tuple] = []

    def fake_download(competencia, nome, *, store, client=None, scratch_dir=None, chunk_size=None):
        calls.append(("download", competencia, nome))
        return raw(nome)

    def fake_convert(competencia, nome, raw_key, *, store, scratch_dir=None, chunk_size=None):
        calls.append(("convert", competencia, nome, raw_key))
        return bronze(nome, raw_key=raw_key)

    monkeypatch.setattr(pipeline, "download_raw_file", fake_download)
    monkeypatch.setattr(pipeline, "convert_raw_file", fake_convert)

    result = pipeline.sincronizar_arquivo(
        "2026-02", "Cnaes.zip", store=object()
    )

    assert calls == [
        ("download", "2026-02", "Cnaes.zip"),
        ("convert", "2026-02", "Cnaes.zip", "raw/2026-02/Cnaes.zip"),
    ]
    assert result["nome"] == "Cnaes.zip"
    assert result["status_download"] == "baixado"
    assert result["status_conversao"] == "publicado"
    assert result["raw_key"] == "raw/2026-02/Cnaes.zip"
    assert result["bronze_key"] == "bronze/2026-02/Cnaes.parquet"
    assert result["bytes"] == 10
    assert result["bytes_parquet"] == 5
    assert result["linhas"] == 3
    assert result["duracao_segundos"] == pytest.approx(4.0)


def test_download_failure_skips_paired_conversion(monkeypatch):
    converted: list[str] = []

    def fake_download(competencia, nome, *, store, client=None, scratch_dir=None, chunk_size=None):
        return raw(nome, status="falhou", raw_key=None, bytes_baixados=0, erro="HTTP 500")

    def fake_convert(*args, **kwargs):
        converted.append("called")
        return bronze()

    monkeypatch.setattr(pipeline, "download_raw_file", fake_download)
    monkeypatch.setattr(pipeline, "convert_raw_file", fake_convert)

    result = pipeline.sincronizar_arquivo(
        "2026-02", "Cnaes.zip", store=object()
    )

    assert converted == []
    assert result["status_download"] == "falhou"
    assert result["status_conversao"] == "não executado"
    assert result["raw_key"] is None
    assert result["bronze_key"] is None
    assert result["linhas"] is None
    assert result["erro"] == "HTTP 500"


def test_conversion_failure_preserves_raw_key(monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "download_raw_file",
        lambda *a, **k: raw("Cnaes.zip"),
    )
    monkeypatch.setattr(
        pipeline,
        "convert_raw_file",
        lambda *a, **k: bronze("Cnaes.zip", status="falhou", linhas=None, erro="CSV inválido"),
    )

    result = pipeline.sincronizar_arquivo(
        "2026-02", "Cnaes.zip", store=object()
    )

    assert result["status_download"] == "baixado"
    assert result["status_conversao"] == "falhou"
    assert result["raw_key"] == "raw/2026-02/Cnaes.zip"
    assert result["bronze_key"] == "bronze/2026-02/Cnaes.parquet"
    assert result["linhas"] is None
    assert result["erro"] == "CSV inválido"


def test_combinar_resultados_merges_by_name():
    downloads = [raw("Cnaes.zip").to_dict()]
    conversions = [bronze("Cnaes.zip").to_dict()]

    combined = pipeline.combinar_resultados(downloads, conversions)

    assert len(combined) == 1
    assert combined[0]["nome"] == "Cnaes.zip"
    assert combined[0]["bytes"] == 10
    assert combined[0]["bytes_parquet"] == 5
    assert combined[0]["linhas"] == 3
    assert combined[0]["duracao_segundos"] == pytest.approx(4.0)


def test_combinar_resultados_marks_missing_conversion_as_failure():
    combined = pipeline.combinar_resultados([raw("Cnaes.zip").to_dict()], [])

    assert combined[0]["status_download"] == "baixado"
    assert combined[0]["status_conversao"] == "falhou"
    assert "conversão" in combined[0]["erro"].lower()


def test_combinar_resultados_marks_missing_download_as_failure():
    combined = pipeline.combinar_resultados([], [bronze("Cnaes.zip").to_dict()])

    assert combined[0]["status_download"] == "falhou"
    assert combined[0]["status_conversao"] == "não executado"
    assert combined[0]["raw_key"] is None
    assert "download" in combined[0]["erro"].lower()


def test_consolidar_resumo_reports_states_totals_and_keys():
    listing = RawListing(
        competencia="2026-02",
        encontrados=3,
        selecionados=2,
        limite=2,
        parcial=True,
        nomes=("Cnaes.zip", "Empresas0.zip"),
    ).to_dict()
    results = [
        {
            "nome": "Empresas0.zip",
            "status_download": "baixado",
            "status_conversao": "publicado",
            "bytes": 20,
            "bytes_parquet": 8,
            "linhas": 4,
            "raw_key": "raw/2026-02/Empresas0.zip",
            "bronze_key": "bronze/2026-02/Empresas0.parquet",
            "duracao_segundos": 2.0,
            "erro": None,
        },
        {
            "nome": "Cnaes.zip",
            "status_download": "reutilizado",
            "status_conversao": "falhou",
            "bytes": 0,
            "bytes_parquet": 0,
            "linhas": None,
            "raw_key": "raw/2026-02/Cnaes.zip",
            "bronze_key": "bronze/2026-02/Cnaes.parquet",
            "duracao_segundos": 1.0,
            "erro": "CSV inválido",
        },
    ]

    summary = pipeline.consolidar_resumo(listing, results)

    assert summary["competencia"] == "2026-02"
    assert summary["encontrados"] == 3
    assert summary["selecionados"] == 2
    assert summary["limite"] == 2
    assert summary["parcial"] is True
    assert [item["nome"] for item in summary["arquivos"]] == [
        "Cnaes.zip",
        "Empresas0.zip",
    ]
    assert summary["totais"]["baixados"] == 1
    assert summary["totais"]["reutilizados"] == 1
    assert summary["totais"]["publicados"] == 1
    assert summary["totais"]["falhas_conversao"] == 1
    assert summary["totais"]["falhas_download"] == 0
    assert summary["totais"]["bytes"] == 20
    assert summary["totais"]["bytes_parquet"] == 8
    assert summary["totais"]["linhas"] == 4
    assert summary["falhas"] == 1
    assert summary["duracao_segundos"] == pytest.approx(3.0)

    encoded = json.dumps(summary)
    assert "http://" not in encoded
    assert "https://" not in encoded
    assert "token" not in encoded.lower()


def test_raise_if_failed_raises_with_file_names_only():
    summary = {
        "competencia": "2026-02",
        "falhas": 1,
        "arquivos": [
            {
                "nome": "Cnaes.zip",
                "status_download": "falhou",
                "status_conversao": "não executado",
            }
        ],
    }

    with pytest.raises(pipeline.PipelineFailedError) as excinfo:
        pipeline.raise_if_failed(summary)

    assert "Cnaes.zip" in str(excinfo.value)
    assert "http://" not in str(excinfo.value)
    assert excinfo.value.summary == summary


def test_raise_if_failed_accepts_successful_summary():
    pipeline.raise_if_failed({"competencia": "2026-02", "falhas": 0, "arquivos": []})


def test_executar_competencia_continues_after_independent_failure(monkeypatch):
    processed: list[str] = []

    monkeypatch.setattr(
        pipeline,
        "listar_arquivos",
        lambda *a, **k: RawListing(
            competencia="2026-02",
            encontrados=2,
            selecionados=2,
            limite=None,
            parcial=False,
            nomes=("Cnaes.zip", "Empresas0.zip"),
        ),
    )

    def fake_sync(competencia, nome, *, store, client=None, scratch_dir=None):
        processed.append(nome)
        if nome == "Cnaes.zip":
            return {
                "nome": nome,
                "status_download": "falhou",
                "status_conversao": "não executado",
                "bytes": 0,
                "bytes_parquet": 0,
                "linhas": None,
                "raw_key": None,
                "bronze_key": None,
                "duracao_segundos": 0.1,
                "erro": "HTTP 500",
            }
        return {
            "nome": nome,
            "status_download": "baixado",
            "status_conversao": "publicado",
            "bytes": 10,
            "bytes_parquet": 5,
            "linhas": 3,
            "raw_key": "raw/2026-02/Empresas0.zip",
            "bronze_key": "bronze/2026-02/Empresas0.parquet",
            "duracao_segundos": 0.2,
            "erro": None,
        }

    monkeypatch.setattr(pipeline, "sincronizar_arquivo", fake_sync)

    with pytest.raises(pipeline.PipelineFailedError) as excinfo:
        pipeline.executar_competencia(
            "2026-02", store=object(), client=object()
        )

    assert processed == ["Cnaes.zip", "Empresas0.zip"]
    summary = excinfo.value.summary
    assert len(summary["arquivos"]) == 2
    assert summary["totais"]["publicados"] == 1
    assert summary["totais"]["falhas_download"] == 1


def test_executar_competencia_returns_successful_summary(monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "listar_arquivos",
        lambda *a, **k: RawListing(
            competencia="2026-02",
            encontrados=1,
            selecionados=1,
            limite=1,
            parcial=False,
            nomes=("Cnaes.zip",),
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "sincronizar_arquivo",
        lambda *a, **k: {
            "nome": "Cnaes.zip",
            "status_download": "baixado",
            "status_conversao": "publicado",
            "bytes": 10,
            "bytes_parquet": 5,
            "linhas": 1,
            "raw_key": "raw/2026-02/Cnaes.zip",
            "bronze_key": "bronze/2026-02/Cnaes.parquet",
            "duracao_segundos": 0.1,
            "erro": None,
        },
    )

    summary = pipeline.executar_competencia(
        "2026-02", limite=1, store=object(), client=object()
    )

    assert summary["falhas"] == 0
    assert summary["selecionados"] == 1
