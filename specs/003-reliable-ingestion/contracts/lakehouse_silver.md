# Contrato Silver (Apache Iceberg)

**Feature**: `003-reliable-ingestion` · **Data**: 2026-11-06
**Consumidores**: serving loader, BI, API analítica

## 1. Tabelas

Um namespace Iceberg por competência e um grupo de tabelas por entidade:

```text
silver_empresas
silver_estabelecimentos
silver_socios
silver_simples
silver_lookup_<entidade>
```

Namespace raiz: `cnpj.<competencia>`

## 2. Particionamento

| Tabela | Coluna de partição | Transform |
|---|---|---|
| `silver_empresas` | `competencia` | identity |
| `silver_estabelecimentos` | `competencia` + `uf` | identity + identity |
| `silver_socios` | `competencia` | identity |
| `silver_simples` | `competencia` | identity |
| `silver_lookup_*` | sem partição | — |

## 3. File properties

| Propriedade | Valor |
|---|---|
| `write.target-file-size-bytes` | 536870912 (512 MB) |
| `write.parquet.compression-codec` | ZSTD |
| `write.format.default` | parquet |
| `format-version` | 2 |

## 4. Propriedades de branch (WAP)

| Propriedade | Valor |
|---|---|
| `loading branch` | `load_<competencia>` |
| `main branch` | `main` |
| `fast-forward` | sim |
| `promotion type` | `fast_forward` |

```
   - Branche criado a partir de main com nome `load_<competencia>`.
   - Todos os loaders do Airflow escrevem na branch.
   - Após validação, a branch é promovida via `fast_forward` para `main`.
   - A branch então os dados não visíveis para `main` até publicação.
   - Retornar\Id de snapshot após promotion deve ser persistido para rollback.
```

## 5. Propriedades de deduplicação e idempotência

Cada tabela tem `business_keys` definidas no **Dataset Identity Registry**.

| Tabela | Business key | Concurrent-safe strategy |
|---|---|---|
| `silver_empresas` | `cnpj_basico` | overwrites only current competencia |
| `silver_estabelecimento` | `cnpj_completo` | overwrites only current competencia |
| `silver_socio` | `cnpj_basico` + `socio_identificador_socio` + `socio_cnpj_cpf_socio` + `socio_razao_social` + `socio_qualificacao_socio` + `socio_data_entrada` | overwrites only current competencia |
| `silver_simples` | `cnpj_basico` | overwrites only current competencia |

- Retries reutilizam o mesmo `load_id` e `partition_id`.
- Sobrescreve apenas as linhas da competência em processamento (nunca reescreve a tabela completa).
- `silver_lookup_*` são simamente imutáveis a cada run; apenas são atualizadas se relevantes.

## 6. Constraints de qualidade validadas antes de publicação

### 6.1 Unicidade

```sql
SELECT business_key_col, count(*)
FROM silver_<table> WHERE competencia = :competencia
GROUP BY business_key_col
HAVING count(*) > 1
```

Deve retornar zero para chave única.

### 6.2 Referência

```sql
SELECT count(*)
FROM silver_estabelecimento e
WHERE e.competencia = :competencia
  AND NOT EXISTS (
    SELECT 1 FROM silver_empresas emp
    WHERE emp.cnpj_basico = e.cnpj_basico AND emp.competencia = :competencia
      AND emp.load_id = :load_id
  )
```

Deve retornar zero.

### 6.3 Contagem

```sql
SELECT count(*) FROM silver_<table> WHERE competencia = :competencia
```

Deve igualar a contagem esperada no source manifest.
