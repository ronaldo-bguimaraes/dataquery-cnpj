"""T013 — Normalização Bronze → batches tipados com streaming e PII mascarada (FR-033/034/005)."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from schema import schema  # noqa: F401  (garante disponibilidade do schema canônico)
from src.ingest_parallel.normalize import (
    BatchBudget,
    normalize_bronze,
)
from src.query.domain.masking import has_full_cpf


@pytest.fixture(name="bronze_fixture")
def bronze_fixture_fixture(tmp_path):
    """Bronze sintético válido com colunas exatas do schema canônico."""

    bronze = tmp_path / "bronze" / "2026-10"
    bronze.mkdir(parents=True, exist_ok=True)

    def write(name, table):
        pq.write_table(table, bronze / name)

    empresas = pa.table(
        {
            "empresa_cnpj_basico": ["00000001", "19131243"],
            "empresa_razao_social": ["EMPRESA ALPHA SA", "EMPRESA BETA LTDA"],
            "empresa_natureza_juridica": ["2062", "2062"],
            "empresa_qualificacao_responsavel": ["49", "52"],
            "empresa_capital_social": ["1500.00", "20000.01"],
            "empresa_porte": ["05", "05"],
            "empresa_ente_federativo": ["", "***"],
        }
    )
    estabelecimentos = pa.table(
        {
            "estabelecimento_cnpj_basico": ["00000001"],
            "estabelecimento_cnpj_ordem": ["0001"],
            "estabelecimento_cnpj_dv": ["91"],
            "estabelecimento_identificador_matriz_filial": ["1"],
            "estabelecimento_nome_fantasia": ["ALPHA"],
            "estabelecimento_situacao_cadastral": ["02"],
            "estabelecimento_data_situacao_cadastral": ["20240101"],
            "estabelecimento_motivo_situacao_cadastral": ["00"],
            "estabelecimento_nome_cidade_exterior": ["0000"],
            "estabelecimento_pais": ["1058"],
            "estabelecimento_data_inicio_atividade": ["20050101"],
            "estabelecimento_cnae_fiscal_principal": ["6204000"],
            "estabelecimento_cnae_fiscal_secundaria": ["6204000"],
            "estabelecimento_tipo_logradouro": ["R"],
            "estabelecimento_logradouro": ["RUAAAA"],
            "estabelecimento_numero": ["1"],
            "estabelecimento_complemento": ["SALA 1"],
            "estabelecimento_bairro": ["CENTRO"],
            "estabelecimento_cep": ["01001000"],
            "estabelecimento_uf": ["SP"],
            "estabelecimento_municipio": ["7107"],
            "estabelecimento_ddd_1": ["11"],
            "estabelecimento_telefone_1": ["99999999"],
            "estabelecimento_ddd_2": [""],
            "estabelecimento_telefone_2": [""],
            "estabelecimento_ddd_fax": [""],
            "estabelecimento_numero_fax": [""],
            "estabelecimento_correio_eletronico": ["a@b.c"],
            "estabelecimento_situacao_especial": [""],
            "estabelecimento_data_situacao_especial": ["00000000"],
        }
    )
    socios = pa.table(
        {
            "socio_cnpj_basico": ["00000001"],
            "socio_identificador_socio": ["1"],
            "socio_nome_socio": ["JOÃO DA SILVA"],
            "socio_cnpj_cpf_socio": ["12345678909"],
            "socio_qualificacao_socio": ["10"],
            "socio_data_entrada_sociedade": ["20200101"],
            "socio_pais": ["1058"],
            "socio_representante_legal": ["***000000***"],
            "socio_nome_representante": ["MARIA SOUZA"],
            "socio_qualificacao_representante_legal": ["10"],
            "socio_faixa_etaria": ["4"],
        }
    )
    simples = pa.table(
        {
            "simples_cnpj_basico": ["00000001"],
            "simples_opcao_simples": ["S"],
            "simples_data_opcao_simples": ["20200101"],
            "simples_data_exclusao_simples": [""],
            "simples_opcao_mei": ["S"],
            "simples_data_opcao_mei": ["20200101"],
            "simples_data_exclusao_mei": ["00000000"],
        }
    )
    paises = pa.table({"pais_codigo": ["1058"], "pais_descricao": ["BRASIL"]})
    for path, table in [
        ("Empresas0.parquet", empresas),
        ("Estabelecimentos0.parquet", estabelecimentos),
        ("Socios0.parquet", socios),
        ("Simples.parquet", simples),
        ("Paises.parquet", paises),
    ]:
        write(path, table)
    return bronze


class TestStreaming:
    def test_batches_limitados_em_linhas(self, bronze_fixture):
        """FR-033/34: nada do dataset completo em memória (BatchBudget caps)."""

        budget = BatchBudget(max_rows=1)
        batches = list(normalize_bronze(bronze_fixture, "empresas", budget=budget))
        assert len(batches) == 2  # 2 rows em 2 batches
        assert all(batch.num_rows <= 1 for batch in batches)

    def test_todas_as_linhas_preservadas(self, bronze_fixture):
        budget = BatchBudget(max_rows=10)
        batches = list(normalize_bronze(bronze_fixture, "empresas", budget=budget))
        assert sum(batch.num_rows for batch in batches) == 2


@pytest.fixture(name="bronze_inventario_fonte")
def bronze_inventario_fonte_fixture(bronze_fixture):
    import shutil

    copy = bronze_fixture.parent / "copy"
    shutil.rmtree(copy, ignore_errors=True)
    shutil.copytree(bronze_fixture, copy)
    return copy


@pytest.fixture(name="gridado")
def gridado_fixture(bronze_fixture, monkeypatch):
    """Injecta o manifesto sobre o bronze sintético (apenas 5 arquivos)."""

    monkeypatch.setenv("CNPJ_INGEST_BRONZE_DIR", str(bronze_fixture.parent))
    return bronze_fixture


class TestTipagemPII:
    def test_socios_cpf_mascarado_no_lote(self, bronze_fixture):
        batches = list(
            normalize_bronze(
                bronze_fixture, "socios", budget=BatchBudget(max_rows=10)
            )
        )
        row = batches[0].to_pylist()[0]
        assert "?" not in row["documento_socio_mascarado"]
        assert not has_full_cpf(row["documento_socio_mascarado"])
        assert row["documento_socio_mascarado"] == "***.***.***-**"

    def test_data_zero_vira_null(self, bronze_fixture):
        batches = list(
            normalize_bronze(
                bronze_fixture, "simples", budget=BatchBudget(max_rows=10)
            )
        )
        row = batches[0].to_pylist()[0]
        assert row["data_exclusao_mei"] is None

    def test_estabelecimento_cnpj_completo_determinado(self, bronze_fixture):
        batches = list(
            normalize_bronze(
                bronze_fixture, "estabelecimentos", budget=BatchBudget(max_rows=10)
            )
        )
        row = batches[0].to_pylist()[0]
        assert row["cnpj_completo"] == "00000001000191"

    def test_capital_social_decimal(self, bronze_fixture):
        batches = list(
            normalize_bronze(
                bronze_fixture, "empresas", budget=BatchBudget(max_rows=10)
            )
        )
        row = batches[0].to_pylist()[0]
        assert float(row["capital_social"]) == 1500.00


class TestContratoDataset:
    def test_dataset_desconhecido_rejeita(self, bronze_fixture):
        with pytest.raises(ValueError):
            list(normalize_bronze(bronze_fixture, "qualquer"))

    def test_batch_schema_tem_colunas_canonicas(self, bronze_fixture):
        from src.ingest_parallel.contracts import DATASET_REGISTRY

        batch = next(
            normalize_bronze(bronze_fixture, "empresas", budget=BatchBudget(max_rows=10))
        )
        for column in DATASET_REGISTRY["empresas"].business_keys:
            assert column in batch.column_names
        assert "competencia" in batch.column_names
        assert "load_id" in batch.column_names
