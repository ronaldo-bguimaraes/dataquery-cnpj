# Research — Ingestão Confiável e Escalável de Dados CNPJ

**Feature**: `003-reliable-ingestion` · **Data**: 2026-11-06
**Contexto**: carga mensal de aproximadamente **223 milhões de registros** a partir do Bronze
já existente, publicação atômica de Silver, e publicação atômica do serving de consulta.

---

## D1 — Apache Iceberg como camada Silver

- **Decision**: usar Apache Iceberg como camada Silver versionada.
- **Rationale**: Iceberg fornece commit atômico no catálogo, snapshots, branch/tag, schema
  evolution e retomada por partição. Isso satisfaz os requisitos de versionamento,
  rollback e publicação atômica da spec. O objeto pode ser lido por Arrow/DuckDB sem
  exigir um engine JVM no caminho principal.
- **Alternatives considered**: Delta Lake (rejeitado: exige validar log store transacional
  em R2); Parquet + manifests próprios (requer reinventar snapshot/rollback); warehouse
  gerenciado (lock-in e custo).
- **Fontes**: [Apache Iceberg docs](https://iceberg.apache.org/), `apache/iceberg-python`,
  decisões do `docs/roadmap-dados.md`.

## D2 — PyIceberg + REST Catalog

- **Decision**: usar PyIceberg com um catálogo REST de metadados persistidos em
  PostgreSQL (ex.: Lakekeeper, Polaris ou Nessie), mantendo o object storage em
  R2/S3-compatible.
- **Rationale**: PyIceberg suporta branch/tag, append/overwrite por partição, leitura por
  Arrow/DuckDB e fast-forward para publicação. O catálogo, não o object store, é a fonte
  do commit atômico. Isso reduz o risco com object stores que não oferecem rename
  atômico.
- **Alternatives considered**: Spark no caminho de carga (rejeitado por peso); DuckDB
  Iceberg plugin (read-only/limitado); Log/SQL catalog custom. Rest catalog é a opção com
  melhor suporte no PyIceberg.
- **Fontes**: [PyIceberg API](https://py.iceberg.apache.org/api/), docs de Lakekeeper e
  Polaris, `apache/iceberg-python`.

## D3 — PyArrow/DuckDB para transformações

- **Decision**: usar PyArrow e DuckDB no caminho de transformação e leitura por bloco;
  PyIceberg entra na escrita/publish do Silver.
- **Rationale**: PyArrow/DuckDB são streaming-friendly, suportam Arrow e são leves o
  suficiente para 223M registros sem exigir Spark.
- **Alternatives considered**: pandas/polars (não stream-friendly ou rejeitado por suporte
  a `latin1`/formato atual), Spark (introduce infraestrutura extra). Rejected.
- **Fontes**: duckdb.org; apache/arrow; pyiceberg docs.

## D4 — WAP via branch + `fast_forward`

- **Decision**: publicar cada competência em uma branch `load_<competencia>`, validar
  contagens/schema/chaves na branch e então publicar via `fast_forward` para a branch
  `main`.
- **Rationale**: PyIceberg suporta branch/fast-forward; a branch oferece visão isolada até a
  publicação, enquanto a `main` permanece a fonte estável. Isso dá WAP sem exigir o rollback
  por delete manual.
- **Alternatives considered**: overwrite direto na `main` (não dá isolamento/audit), tag
  por competência (bom para histórico, mas não para publish), um snapshot por carga
  (redundancia). Rejected.
- **Fontes**: [PyIceberg API — branching](https://py.iceberg.apache.org/api/), docs de
  branching/tag do Iceberg.

## D5 — Particionamento e layout de arquivos

- **Decision**: particionar por `entity`/`competencia` e agrupar arquivos de Silver em
  target file size de aproximadamente 512 MB.
- **Rationale**: 223M de linhas por competência cabem em 30–60 arquivos de ~512 MB; evita 1.000
  arquivos pequenos e evita tráfego desnecessário de file-open na consulta. Permite
  incremental por partição (competencia) e mantém a leitura por engine de BI simples.
- **Alternatives considered**: 1.000 arquivos/particionamentos (overhead de task e file
  open); particionamento por UF/CNAE (aumenta tamanho/base; desnecessário para o caso de
  leitura), partition por cnpj (cardinality high). Rejected.
- **Fontes**: Apache Iceberg maintenance docs; guias de compaction; docs de file layout sane.

## D6 — Staging em PostgreSQL e publicação atômica

- **Decision**: usar tabelas de staging `UNLOGGED` por `load_id`, carregar com `COPY`,
  validar counts/uniques/referências e então trocar de forma atômica a tabela ativa.
- **Rationale**: `COPY` é o caminho mais rápido para 200M linhas; staging UNLOGGED é apenas
  rebuildável em caso de crash; swap atômico mantém `<base>` consumível e evita embedder de
  versões parciais.
- **Alternatives considered**: INSERT manual (lento); partition swap nativa; `TRUNCATE`+
  `INSERT` (leitura bloqueada por todo o loading); COPY com index/pipeline TA. Staging
  plus swap é mais simples e atômico.
- **Fontes**: [PostgreSQL docs — bulk loading](https://www.postgresql.org/docs/current/populate.html),
  guias de COPY e bulk loading, PlanetScale/EDB practices.

## D7 — Execução paralela no Airflow

- **Decision**: Airflow 3.x com dynamic task mapping para 8 tasks concorrentes dentro de um
  pool dedicado, a partir de 64–128 partições.
- **Rationale**: 8 tasks concorrentes mantêm a carga perto do bottleneck do Postgres sem
  sobrecarregar o scheduler; 64–128 partições mantêm tasks mais coarses e sem 1.000 TIs.
  Pool dedicado evita competir com outros DAGs.
- **Alternatives considered**: 1.000 tasks/partitions (overhead do scheduler); single task
  (lento); workers externos. Rejected.
- **Fontes**: [Airflow dynamic task mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html),
  AIP-104, best practices docs.

## D8 — Identidade e deduplication

- **Decision**: identity by **business keys** para todos os datasets: `cnpj_basico`,
  `cnpj_completo`, `cnpj_basico` + `socio_id`, `cnpj_basico` + lookup key. Hash de conteúdo
  pode guidar dedup/change detection, mas não substitui identity.
- **Rationale**: se a chave de negócio não for estável, o dataset precisa de identity rule
  explicitamente definida no contrato, sem inventar colunas pelo caminho.
- **Alternatives considered**: hash-only identity (rejeited: perde semantics); random
  UUID; natural key inconsistente. Rejected.
- **Fontes**: constitution v2.0.0 principles 2/3/10; spec 003 §FR-003/FR-004.

## D9 — Observability e audit

- **Decision**: cada run registra run manifest, partition manifest, counts, rejects, retries,
  snapshot id, source hash, code version e status. Rejeitados em ledger sem PII.
- **Rationale**: auditability explicit é o que permite reprocessamento determinístico e
  rollback.
- **Alternatives considered**: only logs; sem metrics reporting; a fallback ledger per
  dataset; Rejected for failure recovery.
- **Fontes**: constitution v2.0.0 principle 9; spec 003 §FR-041/FR-042.

## D10 — Data compaction

- **Decision**: usar PyIceberg `table.maintenance.compact()` quando disponível; se a versão
  atual não cobrir o caso completo, usar Spark 4 como fallback para compaction one-off.
- **Rationale**: PyIceberg 4.x tem um path de maintenance (issue #1092), mas fallback
  garante operação mesmo se o release atual não estiver completo.
- **Alternatives considered**: DuckDB (sem write manter/tmp files), Spark no caminho
  principal (heavy) e Trino ( bitterly limited). Rejected.
- **Fontes**: apache/iceberg-python issue #1092; iceberg docs "maintenance".

---

## Referências

- [Apache Iceberg docs](https://iceberg.apache.org/)
- [PyIceberg API](https://py.iceberg.apache.org/api/)
- [PostgreSQL docs — bulk loading](https://www.postgresql.org/docs/current/populate.html)
- [Airflow dynamic task mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html)
- [Airflow best practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- Cybertec/EDB bulk loading guides.
