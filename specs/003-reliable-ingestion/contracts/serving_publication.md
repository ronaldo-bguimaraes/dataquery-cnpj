# Contrato de Publicação de Serving

**Feature**: `003-reliable-ingestion` · **Data**: 2026-11-06
**Consumidores**: query web, API, Superset

## 1. Interface

O pipeline de ingestão expõe a ação de publicar uma competência validada, atualizando o
conjunto de tabelas de serving de forma atômica para leitura, com troca de ponteiro.

## 2. Tabelas de serving

As mesmas já usadas pelo portal/API atuais:

| Tabela | Descrição |
|---|---|
| `empresas` | dados da PJ ativos |
| `estabelecimentos` | endereços e status por estabelecimento |
| `estabelecimento_cnaes` | CNAEs normalizados por estabelecimento |
| `socios` | sócios com documento mascarado |
| `simples` | opções Simples/MEI com optantes derivados |
| `lookup_*` | tabelas de apoio (país, município, natureza, etc.) |
| `query_metadata` | metadados de versão ativa |

## 3. Estratégia de staging

### 3.1 Criação

- Cada loader escreve em `staging_<table>_<load_id>`.
- É UNLOGGED (não-réplicada). Em caso de crash, a reconstrução é feita a partir do Silver, não do serving.
- Sem indexes nem FKs na carga bulk — criar apenas após.
- PK / FK / CONSTRAINT validation acontece no stage de validação.

### 3.2 Validação em staging antes de swap

```sql
-- Unicidade
SELECT business_key, count(*) FROM staging_<tbl>_<load_id>
GROUP BY business_key HAVING count(*) > 1

-- Divergência de count
SELECT count(*) FROM staging_<tbl>_<load_id>
-- deve igualar expected count

-- FK referencial
SELECT count(*) FROM staging_estabelecimentos
LEFT JOIN staging_empresas ON ...
WHERE staging_empresas IS NULL
```

### 3.3 Preparação de índices/FK

```sql
-- INDEXES
CREATE INDEX ON staging_<tbl>_<load_id> (...)

-- FKs (se aplicável)
ADD CONSTRAINT staging_estabelecimento_fk_cnpj
FOREIGN KEY (cnpj_basico)
REFERENCES staging_empresas_<load_id> (cnpj_basico)
NOT VALID
ALTER TABLE VALIDATE CONSTRAINT staging_estabelecimento_fk_cnpj
```

## 4. Swap atômico

### 4.1 Comando

Em uma única transação:

```sql
BEGIN;
SET LOCAL lock_timeout = 2s;

-- Cada tabela em sequência:
DROP TABLE IF EXISTS sil_idx_cnpj?; -- de index
DROP TABLE IF EXISTS <base_table>;
ALTER TABLE staging_<table>_<load_id> RENAME TO <base_table>;
-- recriar índices se a tabela live já existia...
-- etc.
```

Recomenda-se **não** publicar as tabelas com índices criados são disse — mas são permitidos:
o swap é rápido e bloqueado apenas durante o renormalization dos índices.

## 5. Rollback

1. O registro de `ServingPublication` fornece o `previous_publication_id`.
2. Tabelas antigas arquivadas com nomes truncados (`archived_<table>_<prior_load_id>`).
3. Em caso de rollback, as staging archives forem renormalizadas de volta.

## 6. Metadados de publicação

O `query_metadata` é atualizado na mesma transação do swap:

| Campo | Tipo |
|---|---|
| `competencia` | text |
| `fonte` | text |
| `carregado_em` | timestamptz |
| `total_empresas` | bigint |
| `total_estabelecimentos` | bigint |
| `total_socios` | bigint |
| `activo` | boolean |

## 7. Erros

| Código | HTTP | Quando |
|---|---|---|
| `STAGING_COUNT_MISMATCH` | 500 | contagem divergente |
| `STAGING_FK_ORPHAN` | 500 | referência órfã |
| `STAGING_UNIQUE_DUP` | 500 | chave única violada |
| `SWAP_TIMEOUT` | 500 | lock timeout no swap |
| `SILVER_SNAPSHOT_MISSING` | 500 | snapshot Iceberg ausente ou inválido |
| `LOAD_ID_MISSING` | 400 | load_id não declarado |
