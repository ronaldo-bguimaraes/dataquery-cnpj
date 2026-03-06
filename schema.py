schema = {
    "empresas": {
        "match": "(?i)^empresas\\d*\\.zip",
        "columns": [
            "empresa_cnpj_basico",
            "empresa_razao_social",
            "empresa_natureza_juridica",
            "empresa_qualificacao_responsavel",
            "empresa_capital_social",
            "empresa_porte",
            "empresa_ente_federativo"
        ]
    },
    "estabelecimentos": {
        "match": "(?i)^estabelecimentos\\d*\\.zip",
        "columns": [
            "estabelecimento_cnpj_basico",
            "estabelecimento_cnpj_ordem",
            "estabelecimento_cnpj_dv",
            "estabelecimento_identificador_matriz_filial",
            "estabelecimento_nome_fantasia",
            "estabelecimento_situacao_cadastral",
            "estabelecimento_data_situacao_cadastral",
            "estabelecimento_motivo_situacao_cadastral",
            "estabelecimento_nome_cidade_exterior",
            "estabelecimento_pais",
            "estabelecimento_data_inicio_atividade",
            "estabelecimento_cnae_fiscal_principal",
            "estabelecimento_cnae_fiscal_secundaria",
            "estabelecimento_tipo_logradouro",
            "estabelecimento_logradouro",
            "estabelecimento_numero",
            "estabelecimento_complemento",
            "estabelecimento_bairro",
            "estabelecimento_cep",
            "estabelecimento_uf",
            "estabelecimento_municipio",
            "estabelecimento_ddd_1",
            "estabelecimento_telefone_1",
            "estabelecimento_ddd_2",
            "estabelecimento_telefone_2",
            "estabelecimento_ddd_fax",
            "estabelecimento_correio_eletronico",
            "estabelecimento_situacao_especial",
            "estabelecimento_data_situacao_especial"
        ]
    },
    "simples": {
        "match": "(?i)^simples\\d*\\.zip",
        "columns": [
            "simples_cnpj_basico",
            "simples_opcao_simples",
            "simples_data_opcao_simples",
            "simples_data_exclusao_simples",
            "simples_opcao_mei",
            "simples_data_opcao_mei",
            "simples_data_exclusao_mei"
        ]
    },
    "socios": {
        "match": "(?i)^socios\\d*\\.zip",
        "columns": [
            "socio_cnpj_basico",
            "socio_identificador_socio",
            "socio_nome_socio",
            "socio_cnpj_cpf_socio",
            "socio_qualificacao_socio",
            "socio_data_entrada_sociedade",
            "socio_pais",
            "socio_representante_legal",
            "socio_nome_representante",
            "socio_qualificacao_representante_legal",
            "socio_faixa_etaria"
        ]
    },
    "paises": {
        "match": "(?i)^paises\\d*\\.zip",
        "columns": [
            "pais_codigo",
            "pais_descricao"
        ]
    },
    "municipios": {
        "match": "(?i)^municipios\\d*\\.zip",
        "columns": [
            "municipio_codigo",
            "municipio_descricao"
        ]
    },
    "qualificacoes_socios": {
        "match": "(?i)^qualificacoes\\d*\\.zip",
        "columns": [
            "qualificacao_socio_codigo",
            "qualificacao_socio_descricao"
        ]
    },
    "naturezas_juridicas": {
        "match": "(?i)^naturezas\\d*\\.zip",
        "columns": [
            "natureza_juridica_codigo",
            "natureza_juridica_descricao"
        ]
    },
    "cnaes": {
        "match": "(?i)^cnaes\\d*\\.zip",
        "columns": [
            "cnae_codigo",
            "cnae_descricao"
        ]
    }
}
