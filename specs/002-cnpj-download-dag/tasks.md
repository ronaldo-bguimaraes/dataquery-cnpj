# Tasks: Download e conversão CNPJ com Airflow (storage genérico)

**Input**: Design documents from `/specs/002-cnpj-download-dag/`

**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md),
[data-model.md](./data-model.md), [contracts/](./contracts/), [quickstart.md](./quickstart.md)

**Tests**: TDD é obrigatório pela Constituição, Princípio V. Cada implementação tem teste
anterior; escreva e execute o teste **falhando** antes de implementar. Não enfraquecer
asserções para obter verde.

**Organization**: T001–T079 (DAG mapeada Raw → Bronze em MinIO) já foram entregues. Não
reexecutar. Este arquivo continua em **T080** e cobre só o incremento da spec revisada:
interface `ObjectStore`, backend filesystem `data/` → `/data`, fábrica de backend
(`filesystem` | `s3`/R2) e desacoplar o Compose local do MinIO.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: pode rodar em paralelo (arquivos diferentes, sem dependência de tarefa incompleta)
- **[Story]**: `[US1]`, `[US2]` ou `[US3]` nas fases de história; Setup/Foundational/Polish sem label
- Toda tarefa cita o caminho exato do arquivo

## Path Conventions

Projeto único: `src/`, `tests/`, `airflow/`. Chaves lógicas `raw/<AAAA-MM>/<nome>` e
`bronze/<AAAA-MM>/<nome-base>.parquet`. Local: `data/` no host, `/data` nos containers.
Produção: as mesmas chaves no bucket R2 via API S3.

## Histórico concluído (não reexecutar)

T001–T079: competência/limite, cliente RFB, download unitário, conversão PyArrow, DAG
`cnpj_download_manual` com mapping, resumo, testes e `S3ObjectStore` em
`src/storage/minio.py`. Reutilizar essa fundação; o incremento adapta o storage, não
reimplementa listagem, schema nem o grafo da DAG.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: configuração e Compose local para filesystem em `data/`, sem lógica de domínio.

- [X] T080 [P] Atualizar `.env.example` com `CNPJ_STORAGE_BACKEND=filesystem`, `CNPJ_DATA_ROOT=/data` e as variáveis `CNPJ_S3_*` documentadas como obrigatórias só quando `backend=s3`; nenhum valor secreto.
- [X] T081 [P] Garantir `data/.gitkeep` e que `.gitignore` ignore `/data/*` exceto `.gitkeep`; não versionar ZIP/Parquet.
- [X] T082 Atualizar `airflow/docker-compose.yaml` e `docker-compose.yaml`: montar `${CNPJ_DATA_HOST_DIR:-../data}:/data` nos serviços que executam tarefas; exportar `CNPJ_DATA_ROOT=/data` e `CNPJ_STORAGE_BACKEND=filesystem`; remover `depends_on` de `minio`/`minio-init` do Airflow; deixar o include MinIO opcional ou fora do caminho feliz local. `docker compose config` deve ser válido.

**Checkpoint**: Compose sobe Airflow sem MinIO; `/data` aponta para `data/` no host.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: protocolo `ObjectStore`, `FilesystemObjectStore`, fábrica por env e adaptação
do `S3ObjectStore`/domínio para o protocolo. Bloqueia todas as histórias.

**⚠️ CRITICAL**: nenhuma história começa antes do checkpoint desta fase.

Contrato: [contracts/storage.md](./contracts/storage.md). Chaves rejeitam `..`, paths
absolutos e URI (`s3://`, `file://`). `head` ausente → `None`; `size == 0` presente mas
**não** reutilizável como Raw. `repr`/exceções sem access key, secret key ou token.

### Tests for Foundational (write first, run failing) ⚠️

- [X] T083 [P] Escrever testes falhando em `tests/unit/test_filesystem_store.py`: `ping` exige raiz existente, diretório e gravável; `head` ausente/`size`; `get_to_path` copia bytes; `put_atomic` usa temporário no mesmo filesystem e `os.replace` — destino anterior intacto se a escrita falhar; arquivo `size == 0` não é Raw válido; chave com `..` ou absoluta é rejeitada; temporários em `root/tmp/` (ou sibling) são removidos em sucesso e falha.
- [X] T084 [P] Escrever testes falhando em `tests/unit/test_store_factory.py`: `CNPJ_STORAGE_BACKEND` omitido ou `filesystem` devolve filesystem com `CNPJ_DATA_ROOT`; `s3` exige `CNPJ_S3_ENDPOINT`, `CNPJ_S3_BUCKET`, `CNPJ_S3_ACCESS_KEY`, `CNPJ_S3_SECRET_KEY` e devolve store S3-compatível; valor inválido falha na fábrica sem I/O de dados; credenciais não aparecem em `repr` nem na mensagem de `StorageConfigError`.
- [X] T085 [P] Estender `tests/unit/test_minio_store.py` com os nomes do protocolo (`ping`, `head`, `get_to_path`, `put_atomic`, `delete`) como aliases ou implementação direta, preservando PUT/multipart e chave final só após complete; objeto `size == 0` continua inválido para reuso.

### Implementation for Foundational

- [X] T086 Implementar o `Protocol` `ObjectStore` e `ObjectMetadata` em `src/storage/protocol.py` (`ping`, `head` → `ObjectMetadata | None`, `get_to_path`, `put_atomic`, `delete`); exportar em `src/storage/__init__.py`. Passar a superfície usada por T083–T085.
- [X] T087 Implementar `FilesystemObjectStore` em `src/storage/filesystem.py` com raiz `CNPJ_DATA_ROOT`, resolução de chave sob a raiz, `put_atomic` via temporário + `os.replace`, `ping` de gravabilidade; passar T083.
- [X] T088 Adaptar `S3ObjectStore` em `src/storage/minio.py` ao protocolo (R2/S3); `ping` = verificação de bucket **sem** criar bucket em produção se a política for só `head_bucket` — documentar: local S3 de teste pode criar; R2 assume bucket já provisionado (fora de escopo). Passar T085.
- [X] T089 Implementar `build_store_from_env` em `src/storage/factory.py`; passar T084.
- [X] T090 Atualizar `tests/fakes.py` (`FakeObjectStore`) para o protocolo; ajustar call sites em `tests/unit/test_download_raw.py`, `tests/unit/test_convert_raw.py`, `tests/unit/test_pipeline_raw_bronze.py`, `tests/unit/test_dag_import.py` e `tests/e2e/test_raw_bronze_local.py` para `ping`/`head`/`get_to_path`/`put_atomic` em vez de `ensure_bucket`/`head_object`/`get_object`/`put_object`.
- [X] T091 Atualizar `src/extracao_rfb/download_raw.py`, `src/conversao/bronze.py` e `src/pipeline/raw_bronze.py` para anotar `store: ObjectStore`, usar só o protocolo (`ping` no preflight; Raw reutilizado se `head` existe e `size > 0`; `get_to_path`/`put_atomic`) e não importar boto3/filesystem. `status_download` permanece `baixado` | `reutilizado` | `falhou`; `bytes` inteiro ≥ 0.

**Checkpoint**: domínio fala só com `ObjectStore`; filesystem e S3 são backends; fábrica seleciona por env.

---

## Phase 3: User Story 1 - Baixar e converter uma competência (Priority: P1) 🎯 MVP

**Goal**: disparar uma competência e gravar Raw + Bronze no backend configurado (localmente
`data/raw/<AAAA-MM>/` e `data/bronze/<AAAA-MM>/`), com o par `baixar_raw → converter_bronze`,
resumo observável e isolamento de falhas. Grafo da DAG inalterado.

**Independent Test**: amostra + `limite=1` gera um ZIP em `data/raw/<AAAA-MM>/`, um Parquet
legível em `data/bronze/<AAAA-MM>/`, colunas do schema central, linhas corretas, resumo com
estados/bytes/linhas/chaves lógicas, sem URL/token/conteúdo em XCom.

### Tests for User Story 1 (write first, run failing) ⚠️

- [X] T092 [P] [US1] Estender `tests/unit/test_dag_import.py`: `_get_store` / callables usam `build_store_from_env` (não instanciam `S3ObjectStore` no módulo); import continua sem I/O; `dag_id == "cnpj_download_manual"`, `schedule is None`, `catchup is False`, `max_active_runs == 1`; par mapeado e run `failed` se qualquer arquivo falhar; XCom só com nomes, chaves lógicas, estados e contagens.
- [X] T093 [P] [US1] Escrever `tests/integration/test_filesystem_pipeline.py` (skip se a raiz não for gravável): round-trip sem RFB — `put_atomic` de ZIP de amostra, `convert_raw_file`, Parquet sob `bronze/<AAAA-MM>/<nome-base>.parquet` na raiz temporária; publicação atômica; `size == 0` não reutilizável; sem boto3.
- [X] T094 [P] [US1] Estender `tests/e2e/test_raw_bronze_local.py` para aceitar `FilesystemObjectStore` em `tmp_path` além do fake: um ZIP de amostra produz as chaves lógicas; Parquet PyArrow com schema e linhas; resumo sem path absoluto de scratch, URI S3 ou token.

### Implementation for User Story 1

- [X] T095 [US1] Atualizar `airflow/dags/cnpj_download.py` para obter o store só dentro dos callables via `build_store_from_env`; docstring/DOC_MD descrevem filesystem local e R2 por configuração, não MinIO obrigatório; passar T092.
- [X] T096 [US1] Executar `docker compose --profile test run --rm airflow-tests tests/unit/test_dag_import.py tests/unit/test_pipeline_raw_bronze.py tests/integration/test_filesystem_pipeline.py tests/e2e/test_raw_bronze_local.py -q` e validar US1 AC1–AC4 e SC-001, SC-002, SC-003, SC-007, SC-008, SC-011.

**Checkpoint**: MVP local em `data/` (ou tmp de teste) validável sem MinIO.

---

## Phase 4: User Story 2 - Limitar os arquivos do primeiro teste (Priority: P2)

**Goal**: limite N após ordenação estável e **antes** de `head` no Raw; no máximo N grupos;
`parcial` no resumo; Raw existente entre os N conta e segue para conversão. Limite inválido
falha antes de RFB e store.

**Independent Test**: M arquivos, `limite=1` → um grupo; repetir com o primeiro Raw já em
`data/raw/...` → não baixa de novo, ainda conta no N e converte.

Campos do resumo (data-model): `encontrados`, `selecionados`, `limite`, `parcial`.

### Tests for User Story 2 (write first, run failing) ⚠️

- [X] T097 [P] [US2] Estender `tests/unit/test_selection.py` e `tests/unit/test_download_raw.py`: `limite=None` todos; `limite=N` os N primeiros lexicográficos; `limite >= total` todos; `limite <= 0` ou não inteiro falha **antes** de `store.ping` e da listagem RFB; seleção **não** chama `head`; Raw existente entre os N não libera vaga.
- [X] T098 [P] [US2] Estender `tests/unit/test_dag_import.py`: Param `limite` `{"type": ["null", "integer"], "minimum": 1}`; mapping com no máximo N grupos; resumo `parcial` identificável quando `selecionados < encontrados`.

### Implementation for User Story 2

- [X] T099 [US2] Confirmar/ajustar `src/extracao_rfb/download_raw.py` (`listar_arquivos`) e `airflow/dags/cnpj_download.py` para a ordem validação → `ping` → RFB → ordenação → limite → mapping, sem `head` na seleção; passar T097/T098.
- [X] T100 [US2] Executar `docker compose --profile test run --rm airflow-tests tests/unit/test_selection.py tests/unit/test_download_raw.py tests/unit/test_dag_import.py -q` e validar US2 AC1–AC3 e SC-005, SC-009.

**Checkpoint**: `limite=1` é o caminho barato; US1 permanece intacta.

---

## Phase 5: User Story 3 - Reexecutar sem baixar Raw novamente (Priority: P3)

**Goal**: reutilizar Raw com `size > 0` sem GET de conteúdo à RFB; reconstruir Bronze a
partir do Raw; substituir Bronze só após `put_atomic` bem-sucedido. `status_conversao`:
`publicado` | `falhou` | `não executado`.

**Independent Test**: duas execuções do mesmo conjunto; a segunda `reutilizado` e zero bytes
RFB; falha de conversão preserva Parquet anterior no filesystem (e no S3 se configurado).

### Tests for User Story 3 (write first, run failing) ⚠️

- [X] T101 [P] [US3] Estender `tests/unit/test_download_raw.py` e `tests/unit/test_convert_raw.py` com `FilesystemObjectStore` em `tmp_path`: Raw `size > 0` → `reutilizado` sem cliente RFB de conteúdo; `size == 0` baixa de novo; retry de conversão não chama download; falha de download não deixa ZIP na chave final.
- [X] T102 [P] [US3] Estender `tests/unit/test_filesystem_store.py` e `tests/unit/test_convert_raw.py`: `put_atomic` falho preserva destino anterior; nenhum `.tmp` visível na chave final; sucesso substitui; scratch de tarefa removido.
- [X] T103 [P] [US3] Estender `tests/e2e/test_raw_bronze_local.py`: primeira execução baixa+converte; segunda reutiliza Raw (zero bytes RFB) e regenera Bronze; sem `.part` em `raw/` ou `bronze/`.

### Implementation for User Story 3

- [X] T104 [US3] Ajustar `src/extracao_rfb/download_raw.py`, `src/conversao/bronze.py` e `src/storage/filesystem.py` para idempotência e atomicidade no filesystem (e manter S3); passar T101/T102.
- [X] T105 [US3] Executar `docker compose --profile test run --rm airflow-tests tests/unit/test_download_raw.py tests/unit/test_convert_raw.py tests/unit/test_filesystem_store.py tests/e2e/test_raw_bronze_local.py -q` e validar US3 AC1–AC3 e SC-004, SC-006.

**Checkpoint**: reexecução segura nos dois backends; histórias independentes.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: documentação, testes S3 opt-in, segurança, cobertura e consistência FR-016–020.

- [X] T106 [P] Reescrever `tests/integration/test_minio_pipeline.py` (ou `test_s3_pipeline.py`) como round-trip S3 **opt-in**: skip a menos que `CNPJ_STORAGE_BACKEND=s3` e endpoint acessível; sem RFB; cobre R2/MinIO se alguém configurar. Não exigir MinIO no Compose local.
- [X] T107 [P] Atualizar `README.md`, `airflow/README.md` e alinhar `specs/002-cnpj-download-dag/quickstart.md`: modo local `data/`/`/data`, `CNPJ_STORAGE_BACKEND`, inspeção `ls data/raw` e `ls data/bronze`, `limite=1`, troca R2 só por env (SC-012/SC-013); remover MinIO como pré-requisito do caminho feliz.
- [X] T108 [P] Revisão de segurança: credenciais e token ausentes de logs, `repr`, XCom e DOC_MD; chaves lógicas apenas; nenhum CPF em mensagens; bucket R2 não tornado público no código. Registrar evidência em `airflow/README.md`.
- [X] T109 Executar a suíte com cobertura (`docker compose --profile test run --rm airflow-tests tests --cov=src --cov-report=term-missing -q`), atualizar o baseline em `airflow/README.md` e garantir que a cobertura total não regride vs. o baseline anterior (75%).
- [X] T110 Revisão final: `git status` sem artefatos em `data/`; `docker compose config`; paths `raw/`/`bronze/` iguais em spec/plan/data-model/contracts/tasks; FR-016–FR-020 e SC-011–SC-013 rastreáveis; nenhuma tarefa pendente in-scope.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: imediato. T080 e T081 em paralelo; T082 depois (Compose).
- **Foundational (Phase 2)**: depende do Setup. T083–T085 em paralelo. T086 antes de T087–T089. T087 e T088 após T086. T089 após T086. T090 após o protocolo (pode seguir T086). T091 após T086–T090.
- **US1 (Phase 3)**: Phase 2 completa. T095 após T092. T096 após T093–T095.
- **US2 (Phase 4)**: pode começar após Phase 2; implementação T099 assume DAG da US1 (T095) para o Param/mapping.
- **US3 (Phase 5)**: após filesystem (T087) e download/conversão no protocolo (T091); E2E T103 após T094.
- **Polish (Phase 6)**: após as histórias desejadas. T109 após T106–T108. T110 após T109.

### User Story Dependencies

- **US1 (P1)**: MVP; não depende de US2/US3.
- **US2 (P2)**: reutiliza listagem/DAG; testável só com `limite`.
- **US3 (P3)**: reutiliza Raw/Bronze; testável com duas execuções.

### Within Each User Story

1. Testes escritos e falhando.
2. Implementação.
3. Validação independente (`docker compose --profile test ...`).
4. Checkpoint antes da próxima prioridade.

### Parallel Opportunities

- Setup: T080, T081
- Foundational tests: T083, T084, T085
- Foundational impl (após T086): T087 e T088; T084/T089 em seguida
- US1 tests: T092, T093, T094
- US2 tests: T097, T098
- US3 tests: T101, T102, T103
- Polish: T106, T107, T108

---

## Parallel Example: User Story 1

```bash
Task: "T092 Estender tests/unit/test_dag_import.py para a fábrica"
Task: "T093 Escrever tests/integration/test_filesystem_pipeline.py"
Task: "T094 Estender tests/e2e/test_raw_bronze_local.py com FilesystemObjectStore"
```

## Parallel Example: User Story 2

```bash
Task: "T097 Estender tests/unit/test_selection.py e test_download_raw.py"
Task: "T098 Estender tests/unit/test_dag_import.py para limite/parcial"
```

## Parallel Example: User Story 3

```bash
Task: "T101 Estender test_download_raw.py e test_convert_raw.py (filesystem)"
Task: "T102 Estender test_filesystem_store.py (atomicidade)"
Task: "T103 Estender test_raw_bronze_local.py (duas execuções)"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Phase 1 Setup (Compose + `data/`).
2. Phase 2 Foundational (protocolo + filesystem + fábrica).
3. Phase 3 US1.
4. **PARAR e VALIDAR**: teste/integração em filesystem; opcionalmente disparo `limite=1`.
5. Só então US2/US3 e Polish.

### Incremental Delivery

1. Setup + Foundational → backends injetáveis.
2. US1 → Raw/Bronze no filesystem (MVP).
3. US2 → limite e `parcial`.
4. US3 → reexecução sem redownload.
5. Polish → docs, S3 opt-in, cobertura, SC-012/013.

### Parallel Team Strategy

1. Time fecha Setup + Foundational.
2. Depois: US1 no caminho crítico; testes de US2/US3 em paralelo; implementação US2/US3 após T091/T095.

---

## Rastreabilidade dos requisitos

| Requisito | Tarefas |
|---|---|
| FR-001–FR-003, FR-005 | já T064/T059; T092, T095 |
| FR-004 | T097, T099 |
| FR-006–FR-007 | T091, T101, T104 |
| FR-008–FR-009 | T091, T093 |
| FR-010–FR-011 | T092, T095 |
| FR-012 | T083, T102, T104 |
| FR-013 | T091, T095 |
| FR-014 | T092, T098 |
| FR-015 | T092, T095 |
| FR-016–FR-020 | T080, T082, T084, T086–T089, T095, T107 |

| Critério | Tarefas |
|---|---|
| SC-001–SC-003, SC-007–SC-008 | T092, T095, T096 |
| SC-004 | T101, T103, T105 |
| SC-005, SC-009 | T097–T100 |
| SC-006 | T083, T102, T104 |
| SC-010 | T092, T095 |
| SC-011 | T093, T096, T107 |
| SC-012–SC-013 | T084, T089, T107, T110 |

## Notes

- `[P]` = arquivos diferentes e sem dependência de tarefa incompleta.
- `[USn]` mapeia a `spec.md`.
- Teste MUST falhar antes da implementação correspondente.
- Chaves lógicas no XCom; nunca path absoluto, `s3://` autenticado, token ou bytes.
- Raw retido por competência (constituição 1.1.0); temporários de tarefa sempre limpos.
- Fora de escopo: provisionar conta/bucket R2, migrar objetos MinIO, Silver/Gold, schedule, frontend, API.
