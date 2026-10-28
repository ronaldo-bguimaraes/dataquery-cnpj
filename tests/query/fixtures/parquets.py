"""Synthetic Parquet builders for the query loader tests."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from schema import schema

EMPRESAS = schema["empresas"]["columns"]
ESTABELECIMENTOS = schema["estabelecimentos"]["columns"]
SOCIOS = schema["socios"]["columns"]
SIMPLES = schema["simples"]["columns"]
LOOKUPS = {
    "Cnaes": schema["cnaes"]["columns"],
    "Motivos": schema["motivos"]["columns"],
    "Municipios": schema["municipios"]["columns"],
    "Naturezas": schema["naturezas_juridicas"]["columns"],
    "Paises": schema["paises"]["columns"],
    "Qualificacoes": schema["qualificacoes_socios"]["columns"],
}


def _write(path: Path, columns: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        column: [str(row.get(column, "")) for row in rows]
        for column in columns
    }
    table = pa.table({name: pa.array(values, type=pa.string()) for name, values in data.items()})
    pq.write_table(table, path, compression="snappy")


def _empty(path: Path, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({name: pa.array([], type=pa.string()) for name in columns})
    pq.write_table(table, path, compression="snappy")


def empresas_rows() -> list[dict[str, str]]:
    return [
        {
            "empresa_cnpj_basico": "00000001",
            "empresa_razao_social": "Empresa Alfa Ltda",
            "empresa_natureza_juridica": "2062",
            "empresa_qualificacao_responsavel": "49",
            "empresa_capital_social": "5000,00",
            "empresa_porte": "01",
            "empresa_ente_federativo": "",
        },
        {
            "empresa_cnpj_basico": "00000002",
            "empresa_razao_social": "Empresa Beta S.A.",
            "empresa_natureza_juridica": "2054",
            "empresa_qualificacao_responsavel": "49",
            "empresa_capital_social": "0,00",
            "empresa_porte": "03",
            "empresa_ente_federativo": "",
        },
        {
            "empresa_cnpj_basico": "00000003",
            "empresa_razao_social": "Comércio Açaí",
            "empresa_natureza_juridica": "2062",
            "empresa_qualificacao_responsavel": "22",
            "empresa_capital_social": "1500,50",
            "empresa_porte": "05",
            "empresa_ente_federativo": "",
        },
    ]


def estabelecimentos_rows() -> list[dict[str, str]]:
    base = {
        "estabelecimento_nome_cidade_exterior": "",
        "estabelecimento_pais": "",
        "estabelecimento_tipo_logradouro": "RUA",
        "estabelecimento_logradouro": "EXEMPLO",
        "estabelecimento_numero": "100",
        "estabelecimento_complemento": "",
        "estabelecimento_bairro": "CENTRO",
        "estabelecimento_cep": "01000000",
        "estabelecimento_ddd_1": "11",
        "estabelecimento_telefone_1": "99999999",
        "estabelecimento_ddd_2": "",
        "estabelecimento_telefone_2": "",
        "estabelecimento_ddd_fax": "",
        "estabelecimento_numero_fax": "",
        "estabelecimento_correio_eletronico": "contato@example.test",
        "estabelecimento_situacao_especial": "",
        "estabelecimento_data_situacao_especial": "",
    }
    return [
        {
            **base,
            "estabelecimento_cnpj_basico": "00000001",
            "estabelecimento_cnpj_ordem": "0001",
            "estabelecimento_cnpj_dv": "36",
            "estabelecimento_identificador_matriz_filial": "1",
            "estabelecimento_nome_fantasia": "Alfa Matriz",
            "estabelecimento_situacao_cadastral": "02",
            "estabelecimento_data_situacao_cadastral": "20200101",
            "estabelecimento_motivo_situacao_cadastral": "00",
            "estabelecimento_data_inicio_atividade": "20200101",
            "estabelecimento_cnae_fiscal_principal": "1111301",
            "estabelecimento_cnae_fiscal_secundaria": "2222201,3333301",
            "estabelecimento_uf": "SP",
            "estabelecimento_municipio": "7107",
        },
        {
            **base,
            "estabelecimento_cnpj_basico": "00000001",
            "estabelecimento_cnpj_ordem": "0002",
            "estabelecimento_cnpj_dv": "17",
            "estabelecimento_identificador_matriz_filial": "2",
            "estabelecimento_nome_fantasia": "",
            "estabelecimento_situacao_cadastral": "08",
            "estabelecimento_data_situacao_cadastral": "20230101",
            "estabelecimento_motivo_situacao_cadastral": "01",
            "estabelecimento_data_inicio_atividade": "20210101",
            "estabelecimento_cnae_fiscal_principal": "2222201",
            "estabelecimento_cnae_fiscal_secundaria": "",
            "estabelecimento_uf": "RJ",
            "estabelecimento_municipio": "6001",
        },
        {
            **base,
            "estabelecimento_cnpj_basico": "00000002",
            "estabelecimento_cnpj_ordem": "0001",
            "estabelecimento_cnpj_dv": "80",
            "estabelecimento_identificador_matriz_filial": "1",
            "estabelecimento_nome_fantasia": "Beta",
            "estabelecimento_situacao_cadastral": "02",
            "estabelecimento_data_situacao_cadastral": "20220101",
            "estabelecimento_motivo_situacao_cadastral": "00",
            "estabelecimento_data_inicio_atividade": "20220101",
            "estabelecimento_cnae_fiscal_principal": "3333301",
            "estabelecimento_cnae_fiscal_secundaria": "",
            "estabelecimento_uf": "SP",
            "estabelecimento_municipio": "7107",
        },
        {
            **base,
            "estabelecimento_cnpj_basico": "00000003",
            "estabelecimento_cnpj_ordem": "0001",
            "estabelecimento_cnpj_dv": "25",
            "estabelecimento_identificador_matriz_filial": "1",
            "estabelecimento_nome_fantasia": "",
            "estabelecimento_situacao_cadastral": "02",
            "estabelecimento_data_situacao_cadastral": "20240101",
            "estabelecimento_motivo_situacao_cadastral": "00",
            "estabelecimento_data_inicio_atividade": "20240101",
            "estabelecimento_cnae_fiscal_principal": "1111301",
            "estabelecimento_cnae_fiscal_secundaria": "",
            "estabelecimento_uf": "MG",
            "estabelecimento_municipio": "4123",
        },
    ]


def socios_rows() -> list[dict[str, str]]:
    return [
        {
            "socio_cnpj_basico": "00000001",
            "socio_identificador_socio": "2",
            "socio_nome_socio": "Maria Exemplo",
            "socio_cnpj_cpf_socio": "***240659**",
            "socio_qualificacao_socio": "49",
            "socio_data_entrada_sociedade": "20200101",
            "socio_pais": "1058",
            "socio_representante_legal": "0",
            "socio_nome_representante": "",
            "socio_qualificacao_representante_legal": "",
            "socio_faixa_etaria": "5",
        },
        {
            "socio_cnpj_basico": "00000002",
            "socio_identificador_socio": "2",
            "socio_nome_socio": "João Exemplo",
            "socio_cnpj_cpf_socio": "12345678901",
            "socio_qualificacao_socio": "22",
            "socio_data_entrada_sociedade": "20220101",
            "socio_pais": "1058",
            "socio_representante_legal": "0",
            "socio_nome_representante": "",
            "socio_qualificacao_representante_legal": "",
            "socio_faixa_etaria": "6",
        },
    ]


def simples_rows() -> list[dict[str, str]]:
    return [
        {
            "simples_cnpj_basico": "00000001",
            "simples_opcao_simples": "S",
            "simples_data_opcao_simples": "20200101",
            "simples_data_exclusao_simples": "00000000",
            "simples_opcao_mei": "N",
            "simples_data_opcao_mei": "00000000",
            "simples_data_exclusao_mei": "00000000",
        },
        {
            "simples_cnpj_basico": "00000002",
            "simples_opcao_simples": "S",
            "simples_data_opcao_simples": "20200101",
            "simples_data_exclusao_simples": "20230101",
            "simples_opcao_mei": "S",
            "simples_data_opcao_mei": "20240101",
            "simples_data_exclusao_mei": "00000000",
        },
    ]


def lookup_rows() -> dict[str, list[dict[str, str]]]:
    return {
        "Cnaes": [
            {"cnae_codigo": "1111301", "cnae_descricao": "Cultivo de arroz"},
            {"cnae_codigo": "2222201", "cnae_descricao": "Fabricação de exemplo"},
            {"cnae_codigo": "3333301", "cnae_descricao": "Comércio de exemplo"},
        ],
        "Motivos": [
            {"motivo_codigo": "00", "motivo_descricao": "SEM MOTIVO"},
            {"motivo_codigo": "01", "motivo_descricao": "EXTINCAO POR ENCERRAMENTO"},
        ],
        "Municipios": [
            {"municipio_codigo": "7107", "municipio_descricao": "SAO PAULO"},
            {"municipio_codigo": "6001", "municipio_descricao": "RIO DE JANEIRO"},
            {"municipio_codigo": "4123", "municipio_descricao": "BELO HORIZONTE"},
        ],
        "Naturezas": [
            {"natureza_juridica_codigo": "2062", "natureza_juridica_descricao": "Sociedade Empresária Limitada"},
            {"natureza_juridica_codigo": "2054", "natureza_juridica_descricao": "Sociedade Anônima Fechada"},
        ],
        "Paises": [{"pais_codigo": "1058", "pais_descricao": "BRASIL"}],
        "Qualificacoes": [
            {"qualificacao_socio_codigo": "49", "qualificacao_socio_descricao": "Sócio-Administrador"},
            {"qualificacao_socio_codigo": "22", "qualificacao_socio_descricao": "Sócio"},
        ],
    }


def escrever_inventario_completo(destination: Path) -> Path:
    """Write the 37-file fixture inventory and return its directory."""

    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)

    _write(destination / "Empresas0.parquet", EMPRESAS, empresas_rows())
    for index in range(1, 10):
        _empty(destination / f"Empresas{index}.parquet", EMPRESAS)

    _write(destination / "Estabelecimentos0.parquet", ESTABELECIMENTOS, estabelecimentos_rows())
    for index in range(1, 10):
        _empty(destination / f"Estabelecimentos{index}.parquet", ESTABELECIMENTOS)

    _write(destination / "Socios0.parquet", SOCIOS, socios_rows())
    for index in range(1, 10):
        _empty(destination / f"Socios{index}.parquet", SOCIOS)

    _write(destination / "Simples.parquet", SIMPLES, simples_rows())
    for name, rows in lookup_rows().items():
        _write(destination / f"{name}.parquet", LOOKUPS[name], rows)

    return destination


def rewrite_wrong_schema(path: Path) -> None:
    table = pa.table({"wrong_column": pa.array(["x"], type=pa.string())})
    pq.write_table(table, path, compression="snappy")
