# Convenções de schema — chaves

## Regra padrão

Toda tabela de domínio tem **id sintético** como PK física:

```sql
id bigserial PRIMARY KEY
```

- O `id` **não participa do domínio**: não aparece em API, UI, contratos, filtros,
  joins de negócio nem em relatórios. Serve apenas para identidade física e como
  alvo de FKs internas quando necessário.
- A **chave de negócio** continua declarada e é `UNIQUE`, nunca a PK:

```sql
cnpj_basico char(8) NOT NULL UNIQUE
cnpj_completo char(14) NOT NULL UNIQUE
```

- **FKs referenciam a chave de negócio** (`cnpj_basico`, `cnpj_completo`, `codigo`),
  que o Postgres exige ser `UNIQUE`. O `id` não entra nas relações do domínio.

| Tabela | PK física | Chave de negócio (UNIQUE) |
|---|---|---|
| empresas | id | cnpj_basico |
| estabelecimentos | id | cnpj_completo |
| estabelecimento_cnaes | id | par (cnpj_completo, cnae_codigo) via índice único¹ |
| socios | id | chave composta de sócio (6 colunas) |
| simples | id | cnpj_basico |
| lookups (`cnaes`, `municipios`, ...) | id | codigo |
| query_metadata | id | competencia |

¹ Recomendado `CREATE UNIQUE INDEX uq_estab_cnae ON estabelecimento_cnaes (cnpj_completo, cnae_codigo)`
para preservar a unicidade do par sem transformá-la em PK composta.

## Exceções (read models e artefatos físicos)

Estas tabelas não são entidades de domínio e mantêm a chave natural como PK física:

- `estabelecimentos_listagem` — projeção desnormalizada para listagem;
- `estabelecimentos_filtros_stats` e `estabelecimentos_cnae_uf_stats` — agregados de contagem;
- `municipios_por_uf`, `cnaes_presentes` — derivados;
- `stg_*` — staging de carga (efêmero);
- tabelas de controle de execução (`ingestion_run`, `load_partition`, `serving_publication`,
  `silver_partition_registry`) já usam chaves naturais próprias.

## Cargas futuras

`src/ingest_parallel/bulk.py` cria staging com `INCLUDING IDENTITY`, então cada tabela
carregada recebe sua própria sequência de `id`; o swap preserva a identidade e as
chaves de negócio permanecem `UNIQUE` (com FKs nas chaves naturais).
