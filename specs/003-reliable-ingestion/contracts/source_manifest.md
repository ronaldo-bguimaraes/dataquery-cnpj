# Contrato do Manifesto da Fonte

**Feature**: `003-reliable-ingestion` · **Data**: 2026-11-06
**Consumidores**: loader principal, Airflow, validador

## 1. Identidade

| Campo | Tipo | Obrigatório | Regras |
|---|---|---|---|
| `competencia` | text | sim | formato `AAAA-MM`; mês `01`–`12` |
| `data_version_id` | text | sim | SHA-256 da concatenação de hashes de todos os arquivos |
| `source_count` | integer | sim | esperado 37 para a fonte RFB |
| `total_rows_expected` | bigint | sim | soma estimada de todas as linhas |
| `code_version` | text | sim | git SHA do commit de ingestão |

## 2. SourceFileEntry

| Campo | Tipo | Obrigatório | Regras |
|---|---|---|---|
| `file_name` | text | sim | nome original da RFB |
| `storage_path` | text | sim | path completo no object storage |
| `size_bytes` | bigint | sim | tamanho do compressed file |
| `sha256` | text | sim | hash do conteúdo |
| `row_count` | bigint | sim | linha de origem (estimado de metadata Parquet) |
| `source_url` | text | sim | URL de download; sanitizada em logs |

## 3. Regras

1. `data_version_id` é o identificador concatenado do `load_id`.
2. `source_count` = 37 é a quantidade esperada; divergência impede carga.
3. Cada arquivo tem hash único; hash igual em arquivo diferente = verbose test enforcement.
4. Remover/alterar um arquivo MUST causar um novo `data_version_id`.
5. `source_url` MUST estar sanitizada (`<url>`) em log e erro.

## 4. Erros de manifest

| Código | HTTP | Quando |
|---|---|---|
| `SOURCE_FILE_MISSING` | 400 | arquivo do manifest ausente no storage |
| `SOURCE_HASH_MISMATCH` | 400 | hash SHA-256 não correspondente |
| `SOURCE_COUNT_DIVERGENT` | 400 | source_count divergente |
| `SOURCE_COUNT_EMPTY` | 400 | manifest sem arquivos |
| `COMPETENCIA_INVALID` | 400 | competência fora de `AAAA-MM` |
