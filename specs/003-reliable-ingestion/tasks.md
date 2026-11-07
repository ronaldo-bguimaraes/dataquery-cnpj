# Tasks: Ingestão Confiável e Escalável de Dados CNPJ

**Feature Branch**: `003-reliable-ingestion` · **Input**: `/specs/003-reliable-ingestion/`

**Prerequisites**: `plan.md` ✅ · `spec.md` ✅ · `research.md` ✅ · `data-model.md` ✅ · `contracts/` ✅ · `quickstart.md` ✅ · [Constitution v2.0.0](../../.specify/memory/constitution.md)

**Tests**: Incluídos por exigência do workflow TDD da constitution v2.0.0 (princípios no workflow). Escrever cada teste ANTES da implementação e verificar que FALHA antes do código passar.

**Organization**: Tarefas agrupadas por user story (US1 = publicação atômica íntegra; US2 = retomada sem retrabalho; US3 = carga paralela escalável; US4 = rastreabilidade/versionamento/incremental).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Pode rodar em paralelo (arquivos diferentes, sem dependência em tarefa incompleta)
- **[Story]**: User story correspondente (US1–US4)
- Caminhos exatos em cada descrição

## Path Conventions

- Código novo: `src/ingest_parallel/` (package independente, ver plan.md)
- DAG: `airflow/dags/cnpj_ingestion.py`
- Testes: `tests/ingest_parallel/` (`unit/`, `integration/`, `contract/`)
- **Reuse**: `schema.py` (tipos canônicos), `src/query/domain/masking.py` (PII), `src/storage/` (object store)

## User Stories mapeadas

| Story | Prioridade | Entregável | Contratos |
|---|---|---|---|
| US1 | P1 🎯 MVP | Competência publicada atômica e íntegra (Silver Iceberg + serving swap) | `source_manifest.md`, `lakehouse_silver.md`, `serving_publication.md` |
| US2 | P2 | Retomada por checkpoint/lease, retry idempotente | `lakehouse_silver.md` §dedup/idempotência |
| US3 | P3 | Carga paralela com 8 workers dentro da janela (≤2 h) | `serving_publication.md` §swap |
| US4 | P4 | Rastreabilidade, versionamento, incremental e rollback | `source_manifest.md`, `lakehouse_silver.md` |

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Estrutura do package e ferramentas de teste

- [X] T001 Criar esqueleto do package `src/ingest_parallel/` (`__init__.py` + módulos vazios `config.py`, `contracts.py`, `inventory.py`, `partitioning.py`, `normalize.py`, `lakehouse.py`, `serving.py`, `publish.py`, `orchestrate.py`, `quality.py`, `observability.py`, `reject.py`, `benchmark.py`) e diretórios `tests/ingest_parallel/{unit,integration,contract}/` com `conftest.py`
- [X] T002 [P] Adicionar dependências de teste em `pyproject.toml`: `pytest`, `pyiceberg[pyarrow,sql-postgres]`, `pyarrow`, `duckdb`, `psycopg[binary,pool]`, e fixture de Postgres de teste (`docker compose` reusando o `query-db` atual em banco separado `cnpj_ingest_test`)
- [X] T003 [P] Criar `src/ingest_parallel/__init__.py` com export para `load_config()` e definir constantes de ambiente documentadas no README do package (`CNPJ_INGEST_*`)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Identidade de carga, contratos de fonte e observabilidade — pré-requisito de TODAS as user stories

**⚠️ CRITICAL**: Nenhuma user story começa antes desta fase (FR-001/002/003/014/038/041/043/044)

### Testes (escrever primeiro, ver falhar)

- [X] T004 [P] Teste unitário de `load_id` determinístico em `tests/ingest_parallel/unit/test_config.py`: dado `competencia=2026-10` e `data_version_id`, `load_id = "2026-10-{sha8}"` é estável entre execuções (FR-014)
- [X] T005 [P] Teste unitário de registro de identidade em `tests/ingest_parallel/unit/test_contracts.py`: business keys por dataset conforme data-model §5.6 — `empresa=[cnpj_basico]`, `estabelecimento=[cnpj_completo]`, `socio=[cnpj_basico, identificador_socio, documento_mascarado, nome, qualificacao, data_entrada]`, `simples=[cnpj_basico]` (FR-003, FR-004)
- [X] T006 [P] Teste unitário de sanitização em `tests/ingest_parallel/unit/test_observability.py`: mensagem contendo CPF completo `123.456.789-09` sai mascarado (`***...`), log estruturado com `load_id/run_id/etapa` (FR-043, Edge "CPF em log")
- [X] T007 [P] Teste de contrato do manifesto em `tests/ingest_parallel/contract/test_source_manifest.py`: `source_count == 37`, `competencia` válida `AAAA-MM`, arquivo ausente → `SOURCE_FILE_MISSING`, hash divergente → `SOURCE_HASH_MISMATCH`, competência inválida → `COMPETENCIA_INVALID` (contratos/source_manifest.md §4)

### Implementação

- [X] T008 Implementar `src/ingest_parallel/config.py`: carrega `CNPJ_INGEST_*` (competencia, storage root, catalog URL, serving DSN), `load_id` determinístico, `code_version` (git SHA), `max_workers=8`, `target_file_size_bytes=536870912` (depende T004)
- [X] T009 [P] Implementar `src/ingest_parallel/contracts.py`: Dataset Identity Registry (`DATASET_REGISTRY` dict com business keys, `dedup_policy=keep_last`, `pii_policy=mask` para socio), helper `identity_key(dataset, row)` (depende T005)
- [X] T010 [P] Implementar `src/ingest_parallel/observability.py`: logger estruturado JSON, sanitizador de PII/segredos, `MetricsRecorder` por etapa (duração, volume, rejects, retry) gravável em Postgres `ingestion_run`/`load_partition` (FR-041/044) (depende T008)
- [X] T011 Implementar `src/ingest_parallel/reject.py`: ledger de rejeições `ingestion_reject` (load_id, partition_id, table, reject_type, reason_detail sanitizado, source_file_hash, source_row_offset) com tipos `schema_mismatch`, `duplicate`, `invalid_data`, `missing_required_field`, `max_length_exceeded` (FR-038, data-model §5.4) (depende T009, T010)
- [X] T012 Implementar `src/ingest_parallel/inventory.py`: varre Bronze `data/bronze/<competencia>/` (37 Parquet), calcula hashes SHA-256, row counts por metadata Parquet, monta Source Manifest com `data_version_id = sha256(concat(hashes))`, valida contra contracts/source_manifest.md (FR-001) (depende T008; valida T007)

**Checkpoint**: Identidade, manifesto, ledger de rejeitos e observabilidade prontos — stories podem iniciar

---

## Phase 3: User Story 1 — Publicação atômica e íntegra (P1) 🎯 MVP

**Goal**: Carga da competência Bronze → Silver Iceberg (branch) → validação → swap atômico do serving; consumidores só veem versão anterior completa ou nova completa

**Independent Test**: `airflow dags test` (ou runner CLI) em amostra; durante a carga o serving responde com dados da versão anterior; simulando falha em um lote, a versão anterior permanece ativa e nenhum lote fica `succeeded`; reexecução produz mesmas contagens (SC-001/002/003)

### Testes (escrever primeiro, ver falhar)

- [ ] T013 [P] [US1] Teste unitário de normalização em `tests/ingest_parallel/unit/test_normalize.py`: Bronze Parquet → Arrow batch tipado (`schema.py`), CPF mascarado em `documento_socio_mascarado` (~`^[0-9*]{11}` nunca 11 dígitos nus), data `AAAAMMDD` zerada → NULL, decimal válido (FR-033, FR-034)
- [ ] T014 [P] [US1] Teste unitário de validadores em `tests/ingest_parallel/unit/test_quality.py`: unicidade por business key retorna violações, FK estabelecimento→empresa retorna órfãs, contagem divergente do manifesto falha (FR-037)
- [ ] T015 [P] [US1] Teste de contrato Silver em `tests/ingest_parallel/contract/test_lakehouse_silver.py`: tabelas criadas com `write.target-file-size-bytes=536870912`, particionadas por `competencia` (contracts/lakehouse_silver.md §2–3), dedup aplica `keep_last` por business key (FR-004)
- [ ] T016 [P] [US1] Teste de contrato serving em `tests/ingest_parallel/contract/test_serving_publication.py`: staging `UNLOGGED` por `load_id`, erro `STAGING_COUNT_MISMATCH`/`STAGING_FK_ORPHAN`/`STAGING_UNIQUE_DUP`, swap em 1 transação com `SET LOCAL lock_timeout` (contracts/serving_publication.md §3–4)
- [ ] T017 [P] [US1] Teste de integração happy path em `tests/ingest_parallel/integration/test_happy_path.py`: amostra Bronze → Silver + serving; `query_metadata` ativo = 1; contagens ≥1 por tabela; PKs únicas (US1 aceite 1–2, quickstart §4)

### Implementação

- [ ] T018 [US1] Implementar `src/ingest_parallel/normalize.py`: leitura Bronze em batches Arrow (chunking, `row_group_size` limitado), colunas tipadas via `schema.py`, mascarar CPF (reuse `src/query/domain/masking.py`), paredão de campo ausente/estourado → gera RejectRecord e não publique silencioso (FR-005, FR-033/034) (depende T009, T011, T013)
- [ ] T019 [US1] Implementar `src/ingest_parallel/lakehouse.py`: catalog PyIceberg REST, criação/append nas tabelas `silver_*` na branch `load_<competencia>`, commit com retry condicional (refresh + revalidate 3×), nunca escreve na `main` (FR-010, contracts/lakehouse_silver.md §4) (depende T018, T015)
- [ ] T020 [US1] Implementar `src/ingest_parallel/serving.py`: staging `staging_<table>_<load_id>` `UNLOGGED`, COPY server-side streaming do snapshot Iceberg validado, índices/FKs `NOT VALID` pós-carga (FR-039) (depende T019, T016)
- [ ] T021 [US1] Implementar `src/ingest_parallel/quality.py`: executar validadores (unicidade business key, FK empresas/estabelecimentos, count == expected, schema hash) sobre staging e Silver; falha impede publicação (FR-012, FR-037) (depende T020, T014)
- [ ] T022 [US1] Implementar `src/ingest_parallel/publish.py`: 1 transação com `BEGIN; SET LOCAL lock_timeout='2s'` → drop/rename staging → ativa → `fast_forward` branch Iceberg para `main` → atualiza `query_metadata` (mesma operação durável, FR-046); registra `ServingPublication` com `previous_publication_id`; rollback restaura anterior (FR-010, FR-040, FR-045) (depende T021)
- [ ] T023 [US1] Implementar coordenação de lote em `src/ingest_parallel/orchestrate.py` (parte mínima): estado da partição só vira `succeeded` após dado durável + checkpoint atômico; falha de qualquer lote impede publish e deixa versão anterior ativa (FR-008, FR-009, FR-012) (depende T022; valida T017)
- [ ] T024 [US1] Teste de integração de falha no meio da carga em `tests/ingest_parallel/integration/test_abort_midway.py`: aborta runner após 2 lotes → serving continua na versão anterior, 0 lotes `succeeded` à frente do checkpoint, staging permanece não-publicada (US1 aceite 3; SC-003) (depende T023)

**Checkpoint**: US1 independente e testável — MVP: carga completa da amostra publicada atómicamente

---

## Phase 4: User Story 2 — Reprocessamento e retomada (P2)

**Goal**: Interromper e retomar sem repetir trabalho concluído; retries idempotentes por `load_id`/`partition_id`; lease de propriedade

**Independent Test**: interromper carga no meio de uma partição, reiniciar e verificar que partições concluídas não são reprocessadas e resultado final sem duplicatas (SC-004, US2 aceite 1–4)

### Testes (escrever primeiro, ver falhar)

- [ ] T025 [P] [US2] Teste de integração de retry idempotente em `tests/ingest_parallel/integration/test_retry_idempotency.py`: mesma `load_id` executada 2× → contagens idênticas, 0 duplicatas, reject ledger não cresce (US2 aceite 3, FR-013/015; quickstart §6)
- [ ] T026 [P] [US2] Teste de integração de retomada em `tests/ingest_parallel/integration/test_resume_checkpoint.py`: kill após N partições, restart → apenas partições `pending/failed` rodam; concluídas puladas (US2 aceite 1–2, FR-020/021)

### Implementação

- [ ] T027 [US2] Implementar checkpoint durável em `src/ingest_parallel/orchestrate.py`: tabela `load_partition` (Postgres) com `status`, `owner_lease` com TTL, `retry_count`, `completed_at`; claim atômico via `UPDATE ... WHERE status='pending' AND lease IS NULL RETURNING` (FR-020, FR-022, data-model §5.2/§7.1) (depende T023)
- [ ] T028 [US2] Implementar transferência de lease: worker morto → lease expira → nova instância assume a MESMA `partition_id` determinística, sem duas escritas concorrentes (lease held check antes de cada commit) (FR-022, US2 aceite 4) (depende T027)
- [ ] T029 [US2] Implementar retomada no runner CLI `python -m src.ingest_parallel.orchestrate --resume <load_id>` (e path da DAG): reconcilia partições concluídas, lista pendentes, retoma checkpoint (FR-021, Edge "worker morre durante commit") (depende T027)
- [ ] T030 [US2] Implementar idempotência de escrita Iceberg: append na branch sob mesmo `load_id` só se snapshot anterior não contém o mesmo `partition_id`; caso exista, substitui apenas os arquivos daquela partição (FR-013, contracts/lakehouse_silver.md §5) (depende T027; valida T025)
- [ ] T031 [US2] Registrar status/causa sanitizada permanente: `ingestion_run.error_summary` (sem PII), histórico consultável para diagnóstico e nova tentativa (FR-041, US2 aceite 5) (depende T010, T028)

**Checkpoint**: US1 + US2 independentes — falha/retry/retomada seguros

---

## Phase 5: User Story 3 — Carga paralela e escalável (P3)

**Goal**: 223M registros divididos em 64–128 partições processadas por 8 workers Airflow dentro da janela (≤2 h, SC-005/006/012)

**Independent Test**: rodar amostra com 1 vs 8 workers e comparar duração ≥2×; matar um worker no meio e verificar que os outros continuam (US3 aceite 1–6)

### Testes (escrever primeiro, ver falhar)

- [ ] T032 [P] [US3] Teste unitário de particionamento em `tests/ingest_parallel/unit/test_partitioning.py`: manifesto 37 arquivos → 64–128 partições lógicas `{table}-{index}` determinísticas (mesmo input → mesma lista), nenhuma linha duplicada/omitida entre partições (FR-018)
- [ ] T033 [P] [US3] Teste de corrida de competência em `tests/ingest_parallel/integration/test_race_one_publisher.py`: duas execuções simultâneas da mesma competência → apenas 1 publica; a outra rejeitada com `RUN_ALREADY_ACTIVE` (FR-016/017, US3 aceite 4)
- [ ] T034 [P] [US3] Teste de integração paralela em `tests/ingest_parallel/integration/test_parallel_sample.py`: amostra com 8 tarefas mapeadas → contagens == manifesto, 0 duplicatas, limite de concorrência respeitado (max 8 TIs ativos) (SC-005)

### Implementação

- [ ] T035 [US3] Implementar `src/ingest_parallel/partitioning.py`: divide Source Manifest em partições por tabela + faixas de arquivo (tamanho alvo por partição ≈ volume/128), retorna payload JSON serializável para DTM (FR-018, research D7) (depende T012, T032)
- [ ] T036 Implementar DAG `airflow/dags/cnpj_ingestion.py`: pipeline `inventory → partitions(DTM) → validate_silver → publish_serving`; mapped tasks com `pool="ingest_parallel"`, `max_active_tis_per_dag=8`, `map_index_template="load_{table}_{partition}"`; pool criado em Docker init (`airflow/dags/runbook.md` com comando `airflow pools set ingest_parallel 8`) (FR-023, FR-024) (depende T035)
- [ ] T037 [US3] Implementar no-op de memória compartilhada: cada mapped task recebe argumentos próprios (partição + manifest), sem variáveis de processo; estado exclusivamente em Postgres/Iceberg (FR-025) (depende T036)
- [ ] T038 [US3] Implementar guard de concorrência em `src/ingest_parallel/orchestrate.py`: lock por competência (`ingestion_run` unique parcial em `competencia` WHERE status='running'), falha observável em vez de degradar (FR-016, FR-035) (depende T036; valida T033)
- [ ] T039 [US3] Implementar limites de recursos em `src/ingest_parallel/normalize.py` + `serving.py`: batch cap (linhas/bytes), `statement_timeout` e pool de conexões psycopg máx 8, falha observável ao estourar (FR-034, FR-035, US3 aceite 5) (depende T018, T020)
- [ ] T040 [US3] Implementar `src/ingest_parallel/benchmark.py`: mede duração/memória pico/linhas/s por etapa e compara 1 worker vs N workers; gera relatório conforme SC-006/SC-012 (depende T036, T039)

**Checkpoint**: US1+US2+US3 independentes — pipeline paralelo completo na amostra

---

## Phase 6: User Story 4 — Rastreabilidade, versionamento e incremental (P4)

**Goal**: Cada resultado rastreável a fonte/execução/snapshot; anterior acessível; incremental sem reescrever o conjunto todo

**Independent Test**: carregar 2 competências (ou 2 versões da mesma) e verificar metadados distintos com hashes/snapshot/código; reexecutar sem mudanças → reescrita evitada e registrada (SC-007/010/011, US4 aceite 1–7)

### Testes (escrever primeiro, ver falhar)

- [ ] T041 [P] [US4] Teste de integração de rastreabilidade em `tests/ingest_parallel/integration/test_lineage.py`: após publicação, metadados da versão contêm `competencia`, `data_version_id`, hashes por arquivo, `silver_snapshot_id`, `code_version`, contagens (US4 aceite 1; FR-042)
- [ ] T042 [P] [US4] Teste de integração de versionamento/rollback em `tests/ingest_parallel/integration/test_version_rollback.py`: 2 versões da mesma competência coexistem (snapshot anterior legível); rollback de teste restaura versão anterior no serving (FR-045/047, SC-011)

### Implementação

- [ ] T043 [US4] Implementar classificação incremental em `src/ingest_parallel/lakehouse.py` (módulo `changes.py` novo): compara hash por arquivo/partição com competência anterior → classifica `novos/modificados/inalterados/removidos`, grava em `load_partition` e registra decisão de carga completa explícita quando não aplicável (FR-027/028/029) (depende T012, T030)
- [ ] T044 [US4] Implementar aplicação incremental: apenas partições classificadas como novas/modificados são reescritas; inalteradas puladas com registro "sem alteração"; remoções tratadas conforme contrato (delete por partição na branch, auditado) (FR-030/031/032, US4 aceite 3–4) (depende T043)
- [ ] T045 [US4] Implementar validação de evolução de schema em `src/ingest_parallel/quality.py`: diff de schema Bronze vs contrato; incompatível → falha antes da publicação (`SCHEMA_INCOMPATIBLE`); compatível → evolução registrada e versão anterior legível (FR-006, SC-010, US4 aceite 6)
- [ ] T046 [US4] Implementar manutenção e retenção em `src/ingest_parallel/lakehouse.py` (módulo `maintenance.py`): compaction (PyIceberg `table.maintenance.compact()` ou fallback Spark 4 documentado), `expire_snapshots` e limpeza de orphans observáveis, respeitando retenção da versão ativa (FR-036, FR-048, research D10)
- [ ] T047 [US4] Expor histórico em `src/ingest_parallel/observability.py`: endpoint-friendly queries/documentadas (`runs`, `partitions`, `rejects`, `publications`) no runbook `airflow/dags/runbook.md`, sem PII (FR-041/043, US4 aceite 7) (depende T010, T011)
- [ ] T048 [US4] Documentar exceções arquiteturais (se houver) com justificativa/responsável/prazo em `specs/003-reliable-ingestion/plan.md` §Complexity Tracking + decisão de retention policy em `docs/roadmap-dados.md` (FR-049) (depende T046)

**Checkpoint**: Rastreabilidade, rollback e incremental completos — feature integralmente entregável

---

## Phase 7: Polish & Cross-Cutting Concerns

- [ ] T049 [P] Atualizar `docs/roadmap-dados.md` com links para feature 003 (pipeline Iceberg/estudo de definições)
- [ ] T050 Executar `specs/003-reliable-ingestion/quickstart.md` completo na amostra: contagens, unique keys, CPF mascarado, FK, rollback
- [ ] T051 Refatorar: remover duplicação entre `orchestrate.py` e `publish.py` (transações em módulo único), lint/format do package
- [ ] T052 Segurança: revisão de logs/erros contra segredos e PII varredura (grep placeholders CNPJ reais em `tests/`, fixtures sintéticas)
- [ ] T053 [P] Testes de unidade adicionais para edge cases em `tests/ingest_parallel/unit/test_edge_cases.py`: arquivo vazio, hash duplicado, competência sem arquivos (falha de qualidade, não publicação vazia)
- [ ] T054 Executar carga completa da competência `2026-10` (223M, SC-012) conforme runbook e registrar duração/memória em `specs/003-reliable-ingestion/quickstart.md` §Benchmark
- [ ] T055 Validação final da constituição: relatório consolidado vs 10 princípios, gaps abertos anotados em `plan.md`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (1)**: imediato, sem dependências
- **Foundational (2)**: depende do Setup — **BLOQUEIA todas as stories** (config, identity, manifest, observability/reject)
- **US1 (3)**: depende da Foundational
- **US2 (4)**: depende de US1 (orchestrate base T023) — não duplica US1
- **US3 (5)**: depende de US2 (lease/checkpoint T027, guard T038 usa orchestrate)
- **US4 (6)**: depende de US1 (lakehouse/quality) e US2 (idempotência T030); maintenance T046 pode rodar em paralelo a T047/T048
- **Polish (7)**: depende de todas as stories desejadas

### User Story Dependencies

- **US1**: isolada após Foundational — sem dependência de outras stories
- **US2**: usa `orchestrate` da US1; altera apenas checkpoint/lease
- **US3**: usa partitioning + concurrency guard; DAG é arquivo novo
- **US4**: usa lineage sobre publish/lakehouse; módulos novos (`changes.py`, `maintenance.py`)

### Within Each User Story

- Testes primeiro (falhar) → implementação → integração → checkpoint
- Models/contratos antes de serviços; serviços antes de orchestrator/publish

### Parallel Opportunities

- Setup: T002, T003 em paralelo
- Foundational: T005–T007 testes + T009, T010 em paralelo (arquivos distintos)
- US1: T013–T016 testes + T018 (normalize) paralelo a testes de qualidade/serving
- US2: T025, T026; US3: T032–T034; US4: T041, T042, T053 — todos [P]
- Entre stories: US4 parte independente (T046) enquanto US3 finaliza

---

## Implementation Strategy

### MVP First (US1 only)

1. Setup (T001–T003) → Foundational (T004–T012)
2. US1 (T013–T024) → **STOP**: validar quickstart §4 na amostra
3. Demos: `python -m src.ingest_parallel.orchestrate --competencia 2026-10` + validações SQL

### Incremental Delivery

1. MVP US1 → 2. US2 retomada → 3. US3 paralela 8 workers → 4. US4 lineage/rollback
5. Polish: benchmark 223M e validação constitucional

### Parallel Team Strategy

- Dev A: Foundational + US1 (bom p/ MVP)
- Dev B (após T023): US2 checkpoints/lease
- Dev C (após T035): DAG US3 + guard
- Dev D (após T030): US4 lineage/maintenance

## Notes

- Commits por tarefa ou grupo lógico, mensagem referenciando T-ID
- Checkpoints de story: parar e validar independente antes de avançar
- `load_id = {competencia}-{data_version_id[:8]}` em TODO artefato persistido
- Proibido: chaves aleatórias; conteúdo integral de registro em log; publicação sem validadores verdes
