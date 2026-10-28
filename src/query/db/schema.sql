CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS query_metadata (
    competencia text PRIMARY KEY,
    fonte text NOT NULL,
    carregado_em timestamptz NOT NULL DEFAULT now(),
    total_empresas bigint NOT NULL,
    total_estabelecimentos bigint NOT NULL,
    total_socios bigint NOT NULL,
    ativo boolean NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS empresas (
    cnpj_basico char(8) PRIMARY KEY,
    razao_social text NOT NULL,
    razao_social_normalizada text NOT NULL,
    natureza_juridica_codigo text,
    qualificacao_responsavel_codigo text,
    capital_social numeric(18, 2),
    porte_codigo text,
    ente_federativo text
);

CREATE TABLE IF NOT EXISTS estabelecimentos (
    cnpj_completo char(14) PRIMARY KEY,
    cnpj_basico char(8) NOT NULL REFERENCES empresas(cnpj_basico),
    cnpj_ordem char(4) NOT NULL,
    cnpj_dv char(2) NOT NULL,
    identificador_matriz_filial char(1),
    nome_fantasia text,
    situacao_cadastral char(2),
    data_situacao_cadastral date,
    motivo_situacao_cadastral char(2),
    nome_cidade_exterior text,
    pais_codigo text,
    data_inicio_atividade date,
    cnae_principal char(7),
    cnae_secundaria_texto text,
    tipo_logradouro text,
    logradouro text,
    numero text,
    complemento text,
    bairro text,
    cep text,
    uf char(2),
    municipio_codigo text,
    ddd_1 text,
    telefone_1 text,
    ddd_2 text,
    telefone_2 text,
    ddd_fax text,
    numero_fax text,
    correio_eletronico text,
    situacao_especial text,
    data_situacao_especial date
);

CREATE TABLE IF NOT EXISTS estabelecimento_cnaes (
    cnpj_completo char(14) NOT NULL REFERENCES estabelecimentos(cnpj_completo) ON DELETE CASCADE,
    cnae_codigo char(7) NOT NULL,
    principal boolean NOT NULL DEFAULT false,
    PRIMARY KEY (cnpj_completo, cnae_codigo)
);

CREATE TABLE IF NOT EXISTS socios (
    id bigserial PRIMARY KEY,
    cnpj_basico char(8) NOT NULL REFERENCES empresas(cnpj_basico) ON DELETE CASCADE,
    identificador_socio char(1),
    nome_socio text,
    documento_socio_mascarado text,
    qualificacao_socio_codigo text,
    data_entrada_sociedade date,
    pais_codigo text,
    representante_legal text,
    nome_representante text,
    qualificacao_representante_legal text,
    faixa_etaria text
);

ALTER TABLE socios ALTER COLUMN representante_legal TYPE text;

CREATE TABLE IF NOT EXISTS simples (
    cnpj_basico char(8) PRIMARY KEY REFERENCES empresas(cnpj_basico) ON DELETE CASCADE,
    opcao_simples char(1),
    data_opcao_simples date,
    data_exclusao_simples date,
    opcao_mei char(1),
    data_opcao_mei date,
    data_exclusao_mei date,
    optante_simples boolean NOT NULL DEFAULT false,
    optante_mei boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS cnaes (
    codigo char(7) PRIMARY KEY,
    descricao text NOT NULL
);

CREATE TABLE IF NOT EXISTS municipios (
    codigo text PRIMARY KEY,
    descricao text NOT NULL
);

CREATE TABLE IF NOT EXISTS naturezas_juridicas (
    codigo text PRIMARY KEY,
    descricao text NOT NULL
);

CREATE TABLE IF NOT EXISTS qualificacoes_socios (
    codigo text PRIMARY KEY,
    descricao text NOT NULL
);

CREATE TABLE IF NOT EXISTS motivos (
    codigo text PRIMARY KEY,
    descricao text NOT NULL
);

CREATE TABLE IF NOT EXISTS paises (
    codigo text PRIMARY KEY,
    descricao text NOT NULL
);

CREATE TABLE IF NOT EXISTS situacoes_cadastrais (
    codigo char(2) PRIMARY KEY,
    descricao text NOT NULL
);

CREATE TABLE IF NOT EXISTS portes (
    codigo char(2) PRIMARY KEY,
    descricao text NOT NULL
);

CREATE TABLE IF NOT EXISTS matriz_filial (
    codigo char(1) PRIMARY KEY,
    descricao text NOT NULL
);

CREATE TABLE IF NOT EXISTS municipios_por_uf (
    uf char(2) NOT NULL,
    municipio_codigo text NOT NULL,
    municipio_descricao text NOT NULL,
    PRIMARY KEY (uf, municipio_codigo)
);

CREATE TABLE IF NOT EXISTS cnaes_presentes (
    cnae_codigo char(7) PRIMARY KEY,
    descricao text NOT NULL
);

CREATE TABLE IF NOT EXISTS estabelecimentos_listagem (
    cnpj_completo char(14) PRIMARY KEY,
    cnpj_basico char(8) NOT NULL,
    razao_social text NOT NULL,
    razao_social_normalizada text NOT NULL,
    nome_fantasia text,
    situacao_codigo char(2),
    situacao_descricao text,
    uf char(2),
    municipio_codigo text,
    municipio_descricao text,
    cnae_principal_codigo char(7),
    cnae_principal_descricao text,
    identificador_matriz_filial char(1),
    matriz_filial_descricao text,
    data_inicio_atividade date,
    porte_codigo char(2),
    porte_descricao text,
    optante_simples boolean NOT NULL DEFAULT false,
    optante_mei boolean NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_estab_list_uf_municipio
    ON estabelecimentos_listagem (uf, municipio_codigo);
CREATE INDEX IF NOT EXISTS idx_estab_list_situacao
    ON estabelecimentos_listagem (situacao_codigo);
CREATE INDEX IF NOT EXISTS idx_estab_list_porte
    ON estabelecimentos_listagem (porte_codigo);
CREATE INDEX IF NOT EXISTS idx_estab_list_abertura
    ON estabelecimentos_listagem (data_inicio_atividade);
CREATE INDEX IF NOT EXISTS idx_estab_list_cnpj_basico
    ON estabelecimentos_listagem (cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_estab_list_razao_trgm
    ON estabelecimentos_listagem USING gin (razao_social_normalizada gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_estab_cnaes_cnae
    ON estabelecimento_cnaes (cnae_codigo);
CREATE INDEX IF NOT EXISTS idx_socios_cnpj_basico
    ON socios (cnpj_basico);

INSERT INTO situacoes_cadastrais (codigo, descricao) VALUES
    ('01', 'Nula'),
    ('02', 'Ativa'),
    ('03', 'Suspensa'),
    ('04', 'Inapta'),
    ('05', 'Ativa Não Regular'),
    ('08', 'Baixada')
ON CONFLICT (codigo) DO NOTHING;

INSERT INTO portes (codigo, descricao) VALUES
    ('00', 'Não informado'),
    ('01', 'Microempresa'),
    ('03', 'Empresa de Pequeno Porte'),
    ('05', 'Demais empresas')
ON CONFLICT (codigo) DO NOTHING;

INSERT INTO matriz_filial (codigo, descricao) VALUES
    ('1', 'Matriz'),
    ('2', 'Filial')
ON CONFLICT (codigo) DO NOTHING;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cnpj_query_app') THEN
        EXECUTE 'GRANT USAGE ON SCHEMA public TO cnpj_query_app';
        EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA public TO cnpj_query_app';
    END IF;
END $$;
