# Data Model — Ingestão Confiável e Escalável de Dados CNPJ

**Feature**: `003-reliable-ingestion` · **Data**: 2026-11-06
**Fontes**: [spec.md](./spec.md) · [research.md](./research.md) · `schema.py` (schemas canônicos)

---

## 1. Camada Raw (immutable)

### 1.1 Raw Source File

| Campo | Tipo | Regras |
|---|---|---|
| `competencia` | text (`AAAA-MM`) | FK implicita para competencia; primary key composta com `file_name` |
| `file_name` | text | nome original da RFB; primary key composta com `competencia` |
| `storage_path` | text | caminho do arquivo no object storage |
| `file_size_bytes` | bigint | tamanho do compressed ZIP |
| `file_hash_sha256` | text | SHA-256 do conteúdo descompactado |
| `source_url` | text | URL de download (não logada) |
| `downloaded_at` | timestamptz | data/hora do download |
| `source_type` | text | sempre `"rfb"` |
| `row_count_expected` | bigint | estimated row count |

## 2. Camada Bronze (validated)

### 2.1 Bronze Source File

| Campo | Tipo | Regras |
|---|---|---|
| `file_hash_sha256` | text | FK → Raw Source File |
| `schema_contract_hash` | text | hash das colunas canônicas usadas |
| `row_count` | bigint | número real de linhas |
| `parsed_at` | timestamptz | data/hora da validação/schema |
| `status` | enum | `valid`, `schema_mismatch`, `empty` |

## 3. Camada Silver (Iceberg)

### 3.1 S3 silver_empresa

| Coluna | Tipo | Nullable | Identidade |
|---|---|---|---|
| `cnpj_basico` | string(8) | false | PK |
| `razao_social` | string | false | — |
| `natureza_juridica_codigo` | string | true | FK → lookup |
| `qualificacao_responsavel_codigo` | string | true | FK → lookup |
| `capital_social` | decimal(18,2) | true | — |
| `porte_codigo` | string(2) | true | FK → lookup |
| `ente_federativo` | string | true | — |
| `competencia` | string(`AAAA-MM`) | false | PK composite |
| `load_id` | string | false | FK → ingestion_run |
| `_source_file_hash` | string | false | — |
| `_ingested_at` | timestamptz | false | — |

### 3.2 S3 silver_estabelecimento

| Coluna | Tipo | Nullable | Identidade |
|---|---|---|---|
| `cnpj_basico` | string(8) | false | FK → silver_empresa |
| `cnpj_ordem` | string(4) | false | — |
| `cnpj_dv` | string(2) | false | — |
| `cnpj_completo` | string(14) | false | PK composite + `competencia` |
| `identificador_matriz_filial` | string(1) | true | — |
| `nome_fantasia` | string | true | — |
| `situacao_cadastral` | string(2) | true | FK → lookup |
| `data_situacao_cadastral` | date | true | — |
| `motivo_situacao_cadastral` | string(2) | true | FK → lookup |
| `cliente_cidade_exterior` | string | true | — |
| `pais_codigo` | string | true | FK → lookup |
| `data_inicio_atividade` | date | true | — |
| `cnae_principal` | string(7) | true | FK → lookup |
| `cnae_secundaria` | list(string(7)) | true | — |
| `tipo_logradouro` | string | true | — |
| `logradouro` | string | true | — |
| `numero` | string | true | — |
| `complemento` | string | true | — |
| `bairro` | string | true | — |
| `cep` | string | true | — |
| `uf` | string(2) | true | FK → lookup |
| `municipio_codigo` | string | true | FK → lookup |
| `ddd_1` | string | true | — |
| `telefone_1` | string | true | — |
| `ddd_2` | string | true | — |
| `telefone_2` | string | true | — |
| `ddd_fax` | string | true | — |
| `numero_fax` | string | true | — |
| `correio_eletronico` | string | true | — |
| `situacao_especial` | string | true | — |
| `data_situacao_especial` | date | true | — |
| `competencia` | string(`AAAA-MM`) | false | PK composite |
| `load_id` | string | false | — |
| `_source_file_hash` | string | false | — |
| `_ingested_at` | timestamptz | false | — |

### 3.3 S3 silver_socio

| Coluna | Tipo | Nullable | Identidade |
|---|---|---|---|
| `cnpj_basico` | string(8) | false | FK → silver_empresa |
| `identificador_socio` | string(1) | false | — |
| `nome_socio` | string | false | — |
| `documento_socio_mascarado` | string | true | LGPD: CPF/CNPJ sempre mascarados |
| `qualificacao_socio_codigo` | string | true | FK → lookup |
| `data_entrada_sociedade` | date | true | — |
| `pais_codigo` | string | true | FK → lookup |
| `representante_legal` | text | true | — |
| `nome_representante` | string | true | — |
| `qualificacao_representante` | string | true | FK → lookup |
| `faixa_etaria` | string | true | — |
| `competencia` | string(`AAAA-MM`) | false | PK composite |
| `load_id` | string | false | — |
| `_source_file_hash` | string | false | — |
| `_ingested_at` | timestamptz | false | — |

### 3.4 S3 silver_simples

| Coluna | Tipo | Nullable | Identidade |
|---|---|---|---|
| `cnpj_basico` | string(8) | false | FK → silver_empresa |
| `opcao_simples` | string(1) | true | — |
| `data_opcao_simples` | date | true | — |
| `data_exclusao_simples` | date | true | — |
| `opcao_mei` | string(1) | true | — |
| `data_opcao_mei` | date | true | — |
| `data_exclusao_mei` | date | true | — |
| `optante_simples` | boolean | false | derivado |
| `optante_mei` | boolean | false | derivado |
| `competencia` | string(`AAAA-MM`) | false | PK composite |
| `load_id` | string | false | — |
| `_source_file_hash` | string | false | — |
| `_ingested_at` | timestamptz | false | — |

## 4. Tabelas de Lookup (immutable)

| Tabela | Código | Descrição | PK |
|---|---|---|---|
| `lookup_pais` | `pais_codigo` | `pais_descricao` | código |
| `lookup_municipio` | `municipio_codigo` | `municipio_descricao` | código |
| `lookup_natureza_juridica` | `natureza_juridica_codigo` | `natureza_juridica_descricao` | código |
| `lookup_qualificacao_socio` | `qualificacao_socio_codigo` | `qualificacao_socio_descricao` | código |
| `lookup_cnae` | `cnae_codigo` | `cnae_descricao` | código |
| `lookup_motivo` | `motivo_codigo` | `motivo_descricao` | código |
| `lookup_porte` | `porte_codigo` | `porte_descricao` | código |

## 5. Entidades de Carga

### 5.1 Source Manifest

Documento versionado com o inventário dos arquivos de origem para uma competência.

| Campo | Tipo | Regras |
|---|---|---|
| `competencia` | text | `AAAA-MM`; not null |
| `data_version_id` | text | hash SHA-256 concatenação de hashes de todos os arquivos; not null |
| `source_count` | integer | número de arquivos RFB; esperado 37 |
| `total_rows_expected` | bigint | soma de todas as linhas; not null |
| `files` | list[SourceFileEntry] | lista de arquivos com path, size, hash, row count |
| `code_version` | text | git → não pode ser omitido |
| `created_at` | timestamptz | not null |

### 5.2 Load Partition

Subdivisão do trabalho dentro de uma competência para processamento em paralelo.

| Campo | Tipo | Regras |
|---|---|---|
| `load_id` | text | `{competencia}-{data_version_id[:8]}` |
| `partition_id` | text | `{table}-{partition_index}`; deterministic |
| `table` | enum | `empresa`, `estabelecimento`, `socio`, `simples` |
| `source_files` | list[SourceFileEntry] | files included in this partition |
| `expected_rows` | bigint | sum |
| `status` | enum | `pending`, `running`, `succeeded`, `failed` |
| `owner_lease` | timestamptz | claim por worker; expira |
| `retry_count` | integer | número de tentativas |
| `started_at` | timestamptz | — |
| `completed_at` | timestamptz | — |

### 5.3 Ingestion Run

Top-level execution record.

| Campo | Tipo | Regras |
|---|---|---|
| `run_id` | text | `{load_id}-{attempt}`; random |
| `load_id` | text | FK → Source Manifest; deterministic |
| `competencia` | text | required |
| `status` | enum | `running`, `succeeded`, `failed`, `aborted` |
| `started_at` | timestamptz | not null |
| `completed_at` | timestamptz | nullable |
| `code_version` | text | git SHA |
| `config_hash` | text | hash da configuração |
| `workers_count` | integer | número de concorrentes ativos |
| `error_summary` | text | sanitized; no PII |
| `source_manifest_hash` | text | — |

### 5.4 Reject Record

Contabilização rejeitada por motivo classificável.

| Campo | Tipo | Regras |
|---|---|---|
| `load_id` | text | FK → Ingestion Run |
| `partition_id` | text | — |
| `table` | text | dataset em que ocorreu |
| `reject_type` | enum | `schema_mismatch`, `duplicate`, `invalid_data`, `missing_required_field`, `max_length_exceeded` |
| `reason_detail` | text | sanitized; sem PII |
| `source_file_hash` | text | — |
| `source_row_offset` | bigint | posição relativa na fonte |
| `rejected_at` | timestamptz | — |

### 5.5 Serving Publication

Assinatura de troca da versão ativa no serving.

| Campo | Tipo | Regras |
|---|---|---|
| `publication_id` | text | `{load_id}`; unique |
| `competencia` | text | — |
| `silver_snapshot_id` | text | id do snapshot Iceberg validado |
| `silver_branch` | text | branch de carga |
| `status` | enum | `staging`, `validating`, `published`, `rolled_back` |
| `staging_tables` | list[text] | nomes das tabelas staging |
| `published_at` | timestamptz | nullable |
| `previous_publication_id` | text | FK para rollback |
| `code_version` | text | — |

### 5.6 Dataset Identity Registry

Regras de identidade e unicidade.

| Campo | Tipo | Regras |
|---|---|---|
| `dataset_name` | text | `empresa`, `estabelecimento`, `socio`, `simples`, each lookup |
| `business_keys` | list[text] | ordered list de colunas que definem identidade |
| `uniqueness_rule` | text | descritivo; MUST obrigatória |
| `dedup_policy` | enum | `keep_first`, `keep_last`, `merge`, `reject` |
| `pii_policy` | enum | `mask`, `omit`, `preserve` |
| `active` | boolean | true |

## 6. Regras de Validação

| Regra | Onde é aplicada | Mensagem |
|---|---|---|
| Competência `AAAA-MM` válida | borda | "Competência inválida" |
| Dimensions de arquivo em manifest | stage de validação | "Arquivo ausente ou com hash divergente" |
| `cnpj_basico` = 8 dígitos | borda | "CNPJ básico inválido" |
| `cnpj_completo` = 14 dígitos | borda | "CNPJ inválido" |
| Data `AAAAMMDD` não-zero / None | transform | descarte é contado como reject |
| Capital social decimal | transform | descarte é contado como reject |
| Unique key após dedup | pre-publication | "Unique key violation" (impede publicação) |
| FK entre silver tables | pre-publication | "Orphan referenced" (impede publicação) |
| Source count = Bronze count | pre-publication | "Contagem divergente" |
| Recourse qualificado sem PII | transação | aprende o código |
| `cnpj_completo` = 14 dígitos com DV válido | transform | descarte é contado |
| `socio_representante_legal` texto sem exceder | transform | descarte |
| `load_id` re-execução sem duplicatas | pre-publication | sucesso |

## 7. Transições de Estado

### 7.1 Load Partition

```text
pending → running → succeeded
         → failed → pending (retry)
         → failed → failed (exhausted)
```

### 7.2 Ingestion Run

```text
running → succeeded
running → failed
running → aborted
```

### 7.3 Serving Publication

```text
staging → validating → published
staging → validating → rolled_back (manteve versão anterior)
```

## 8. Relações

```text
SourceManifest 1..N SourceFile
IngestionRun 1..1 SourceManifest
IngestionRun 1..N LoadPartition
LoadPartition 1..1 Checkpoint
LoadPartition 1..N RejectRecord
ServingPublication 1..1 IngestionRun
DatasetIdentityRegistry 1..N ServingDataset
```

## 9. Referências

- [Contrato da fonte](./contracts/source_manifest.md)
- [Contrato Silver](./contracts/lakehouse_silver.md)
- [Contrato Serving](./contracts/serving_publication.md)
- [Spec](./spec.md)
- [Pesquisa](./research.md)
