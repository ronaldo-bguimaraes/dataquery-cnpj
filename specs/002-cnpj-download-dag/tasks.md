# Tasks: Primeira DAG de Download de Dados CNPJ

**Input**: Design documents from `/specs/002-cnpj-download-dag/`

**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md),
[data-model.md](./data-model.md), [contracts/](./contracts/), [quickstart.md](./quickstart.md)

**Tests**: Incluídos. A Constituição do projeto (Princípio V, NON-NEGOTIABLE) e a decisão D11
do plano exigem testes antes da implementação. Cada tarefa de teste MUST ser escrita e
executada com falha antes da tarefa de implementação correspondente (Red-Green-Refactor).

**Organization**: Tarefas agrupadas por história de usuário para permitir implementação e
validação independentes. A ordem de prioridade é US1 (P1, MVP) → US2 (P2) → US3 (P3).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: pode rodar em paralelo (arquivos diferentes, sem dependência de tarefa incompleta)
- **[Story]**: `[US1]`, `[US2]` ou `[US3]`, conforme `spec.md`
- Toda tarefa cita o caminho exato do arquivo ou o alvo de validação

## Path Conventions

Projeto único de pipeline Python: `src/`, `tests/`, `airflow/` na raiz do repositório. A saída
runtime fica em `data/cnpj/` (gitignored) e é montada em `/data/cnpj` nos containers.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: preparar dependências, imagem, mounts e configuração de testes. Nenhuma lógica de
domínio é implementada aqui.

- [X] T001 Create `pytest.ini` at repository root with `testpaths = tests`, `asyncio_mode = auto`, `markers = network: requires RUN_NETWORK_TESTS=1`, `addopts = -p no:cacheprovider` and `pythonpath = .`
- [X] T002 [P] Update root `requirements.txt`: remove the duplicated `numpy` line, remove unused `pyspark`, add `aiohttp==3.14.3`; keep `webdavclient3==3.14.7` and the existing pipeline pins
- [X] T003 [P] Create root `requirements-dev.txt` with `-r requirements.txt`, `pytest`, `pytest-asyncio` and `pytest-cov` for local execution
- [X] T004 [P] Create `airflow/requirements.txt` containing only `webdavclient3==3.14.7` (overlay over the official Airflow image, per research D9)
- [X] T005 [P] Create `airflow/requirements-dev.txt` with `-r requirements.txt`, `pytest`, `pytest-asyncio` and `pytest-cov`
- [X] T006 Create `airflow/Dockerfile` as multi-stage `base`/`test`: `FROM apache/airflow:3.3.2`, install `airflow/requirements.txt` as `airflow` user; `test` stage additionally installs `airflow/requirements-dev.txt` (depends on T004, T005)
- [X] T007 [P] Add `data/` to `.gitignore` so downloaded RFB files are never committed
- [X] T008 Update `airflow/docker-compose.yaml` `x-airflow-common`: add `build` pointing to `airflow/Dockerfile`, mount repository root at `/opt/dataquery-cnpj:ro`, mount `${CNPJ_DATA_HOST_DIR:-../data/cnpj}` at `/data/cnpj`, and set `PYTHONPATH=/opt/dataquery-cnpj`, `PYTHONDONTWRITEBYTECODE=1`, `CNPJ_DOWNLOAD_DIR=/data/cnpj` (depends on T006)
- [X] T009 Update `airflow/docker-compose.yaml` `airflow-init`: create `/data/cnpj` and `chown` it to `AIRFLOW_UID:0` before the stack starts, so the bind mount is writable by the task container
- [X] T010 Add service `airflow-tests` to `airflow/docker-compose.yaml`: profile `test`, `test` build target, same repo mount, `entrypoint: python -m pytest -p no:cacheprovider`, default command targeting `tests/` and writable `/tmp` for pytest fixtures

**Checkpoint**: `docker compose build` succeeds and `docker compose --profile test run --rm airflow-tests --collect-only` starts without import errors.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: primitivas compartilhadas por todas as histórias — redaction, validação de
competência, cliente RFB, downloader atômico e fixtures de teste. Nenhuma história pode
começar antes desta fase.

**⚠️ CRITICAL**: nenhuma tarefa de US1/US2/US3 pode iniciar antes do checkpoint desta fase.

- [X] T011 Create `tests/conftest.py` with fixtures: `tmp_download_dir`; `local_http_server` (start/stop an `aiohttp.web` app on `127.0.0.1`, ephemeral port); `fake_client_factory` returning `list_files`/`get_file_url` stubs; and `block_network` monkeypatching `socket.socket.connect` to raise
- [X] T012 [P] Write failing tests in `tests/unit/test_redaction.py`: `redact_urls` replaces `https://…/public.php/dav/files/<token>/…` with `<url>`; `describe_exception` maps `aiohttp.ClientResponseError` to `HTTP <status> <reason>`, `aiohttp.ClientError` to its class name, and generic exceptions to a message without URL; `repr(DownloadTarget(...))` does not contain the URL
- [X] T013 Implement `src/download_utils/redaction.py` with `redact_urls(text: str) -> str`, `describe_exception(exc: BaseException) -> str` and a URL-free helper for logging, passing T012
- [X] T014 [P] Write failing tests in `tests/unit/test_rfb_client.py` using an injected host/base path: `list_files("2026-02")` returns basenames in lexicographic order with directory entries removed; a `RemoteResourceNotFound` for an unpublished competência returns `[]`; any other failure raises `ListingError` whose message contains neither URL nor token; `get_file_url` builds the path without logging it
- [X] T015 Implement `src/extracao_rfb/rfb_client.py`: accept `hostname`/`base_path` in the constructor for testability; `list_files` sorts, filters entries ending in `/` and translates `webdav3.exceptions.RemoteResourceNotFound` to `[]`; wrap other failures in `ListingError` using `describe_exception`, raised with `from None`; `get_file_url` returns the full URL for internal use only, passing T014
- [X] T016 [P] Harden `src/webdav/auto_token.py`: use `timeout=30` on the redirect request and raise an explicit `RuntimeError` (without URL/token) when the public token cannot be extracted
- [X] T017 [P] Harden `src/webdav/auto_webdav_client.py`: replace the bare `except:` with `except Exception`, log only `describe_exception(exc)`, and raise `RuntimeError(...) from None` so the original URL/token never reaches the traceback
- [X] T018 [P] Write failing tests in `tests/unit/test_download_files.py` against `local_http_server`: streaming download writes exact bytes and returns status `baixado`; HTTP 500 yields `falhou` with sanitized error while other files still download; content shorter than `Content-Length` yields `falhou` and leaves no `.part`; results are ordered by `nome`; `concorrencia <= 0` or `chunk_size <= 0` raises `ValueError` before any request
- [X] T019 Implement `src/download_utils/download_files.py` with `DownloadTarget` (`nome`, `url` with `repr=False`), `FileResult` (`nome`, `status` in `baixado|ignorado|falhou`, `bytes >= 0`, `erro` sanitized), `DownloadReport` and `download_files(targets, output_dir, concorrencia=3, chunk_size=1MiB)`: queue + workers, `iter_chunked` streaming into `.<nome>.<pid>.<uuid>.part`, `os.replace` on success, removal in `finally`, per-file exception isolation, no bare `except:`, passing T018
- [X] T020 [P] Write failing tests in `tests/unit/test_competencia.py`: `validate_competencia` accepts `2026-02` (and trims surrounding spaces) and rejects `None`, empty, `2026-2`, `02-2026`, `2026-13`, `2026-00`, `2026/02` and `2026-02-01` with `InvalidCompetenciaError`; the valid value is returned normalized
- [X] T021 Implement `src/extracao_rfb/competencia.py` with `InvalidCompetenciaError`, `validate_competencia(value: str | None) -> str` using `^\d{4}-(0[1-9]|1[0-2])$` after trim, and a module docstring stating it performs no I/O, passing T020

**Checkpoint**: Foundation ready — downloader, client and validation are unit-tested locally; user stories can begin.

---

## Phase 3: User Story 1 - Executar manualmente o download de uma competência (Priority: P1) 🎯 MVP

**Goal**: disparar manualmente a competência `AAAA-MM`, gravar os arquivos no diretório
persistente e receber um resumo com encontrados, baixados, ignorados, falhas, bytes e duração.
Qualquer falha de arquivo marca a execução como falha, preservando os arquivos concluídos.

**Independent Test**: com `2026-02` e diretório vazio, rodar a DAG e verificar todos os
arquivos no diretório persistente e `encontrados = baixados`, `ignorados = 0`, `falhas = 0`;
importar a DAG sem disparar não faz nenhuma requisição de rede.

### Tests for User Story 1 (write first, run failing) ⚠️

- [X] T022 [P] [US1] Write failing tests in `tests/unit/test_download_competencia.py` with a fake client and `local_http_server`: creates `<output_dir>/<AAAA-MM>/`; empty listing raises `EmptyCompetenciaError`; listing failure raises `ListingError` without touching files; one failing file raises `DownloadError` with `summary.falhas >= 1` while successful files remain; non-writable `output_dir` raises `OutputDirectoryError` before any listing; successful summary has `competencia`, absolute `output_dir`, `encontrados`, `selecionados`, `baixados`, `ignorados`, `falhas`, `bytes_baixados`, `duracao_segundos`, `parcial`, `arquivos`; no URL/token in summary or error text
- [X] T023 [P] [US1] Write failing tests in `tests/unit/test_dag_import.py`: importing `airflow/dags/cnpj_download.py` with `block_network` succeeds; `dag_id == "cnpj_download_manual"`, `schedule is None`, `catchup is False`, one task `baixar_arquivos`, tags contain `cnpj`, `download`, `manual`
- [X] T024 [P] [US1] Write failing e2e test in `tests/e2e/test_download_local_server.py`: `download_competencia("2026-02", tmp_path, client=fake)` against `local_http_server` writes every listed file with exact content and returns `encontrados == baixados`, `falhas == 0`, `bytes_baixados == sum(sizes)`, `parcial is False`

### Implementation for User Story 1

- [X] T025 [US1] Implement `src/extracao_rfb/download_competencia.py`: `DownloadSummary` with `to_dict()` (JSON types only), errors `OutputDirectoryError`, `EmptyCompetenciaError`, `DownloadError` (carrying `.summary`), `download_competencia_async` and sync `download_competencia`; flow = validate competência → create/preflight `<CNPJ_DOWNLOAD_DIR>/<AAAA-MM>/` → list (empty ⇒ `EmptyCompetenciaError`) → select all → mark existing non-empty files as `ignorado` without HTTP → call `download_files` → aggregate → raise `DownloadError` when `falhas > 0`; log only names/status/counts/duration, passing T022
- [X] T026 [US1] Create `airflow/dags/cnpj_download.py` with `dag_id="cnpj_download_manual"`, `schedule=None`, `catchup=False`, `max_active_runs=1`, `retries=0`, `Param("competencia", type=["null","string"], pattern=r"^\d{4}-(0[1-9]|1[0-2])$")`, one `PythonOperator(task_id="baixar_arquivos")` that imports `download_competencia` **inside** the callable, reads `CNPJ_DOWNLOAD_DIR` from the environment (no default), calls it, logs the summary as JSON and returns `summary.to_dict()`; add `doc_md` with the manual trigger command, passing T023
- [X] T027 [US1] Rewrite `src/extracao_rfb/extract_cnpj_files.py` as a thin argparse CLI (`--competencia`, `--output-dir` defaulting to `CNPJ_DOWNLOAD_DIR`, `--concorrencia`) that runs the same function, prints `to_dict()` as JSON and exits `0` on success, `1` on `DownloadError`, `2` on validation/config/listing/empty errors; no work at import time
- [X] T028 [US1] Run `pytest tests/unit/test_download_competencia.py tests/unit/test_dag_import.py tests/e2e/test_download_local_server.py -q` and `airflow dags list | grep cnpj_download_manual` in `airflow-tests`/`airflow-scheduler`; confirm the MVP acceptance scenarios 1–5 of US1

**Checkpoint**: US1 is functional and independently validated. This is the MVP.

---

## Phase 4: User Story 2 - Limitar a quantidade de arquivos no primeiro teste (Priority: P2)

**Goal**: aceitar `limite` inteiro positivo, selecionar os N primeiros nomes em ordem
lexicográfica estável e marcar a execução como parcial; limite inválido falha antes de
qualquer acesso à fonte.

**Independent Test**: executar com `limite=1` e verificar exatamente 1 arquivo baixado,
`encontrados` igual ao total, `parcial=true`; repetir com o mesmo limite sem baixar nada novo;
`limite=0` falha antes de listar.

> Depende de US1 para a função de orquestração e a DAG existirem, mas mantém critérios de
> teste próprios.

### Tests for User Story 2 (write first, run failing) ⚠️

- [X] T029 [P] [US2] Extend `tests/unit/test_competencia.py` with failing tests for `validate_limit`: `None` → `None`; `1`, `37` accepted; `0`, `-1`, `True`, `False`, `"1"`, `1.5` raise `InvalidLimitError`
- [X] T030 [P] [US2] Write failing tests in `tests/unit/test_selection.py` for `select_files(paths, limite)`: lexicographic order regardless of input order; `limite=None` returns all; `limite=N` returns the first N; `limite >= total` returns all; `limite <= 0` raises `InvalidLimitError`; a pre-existing file among the N is not replaced by the next one (selection happens before existence checks)
- [X] T031 [P] [US2] Extend `tests/unit/test_download_competencia.py` with failing limit tests: 3 listed files + `limite=1` ⇒ `encontrados=3`, `selecionados=1`, `parcial=True`, exactly 1 processed; existing first file + `limite=1` ⇒ `baixados=0`, `ignorados=1`, and the 2nd file is NOT downloaded; `limite=100` ⇒ `parcial=False`; `limite=0` raises `InvalidLimitError` before listing (fake client not called)
- [X] T032 [P] [US2] Extend `tests/unit/test_dag_import.py` to assert the `limite` Param schema is `{"type": ["null", "integer"], "minimum": 1}` and that the task receives `params["limite"]`

### Implementation for User Story 2

- [X] T033 [US2] Add `InvalidLimitError`, `validate_limit(value: int | None) -> int | None` (reject `bool`, require integer `>= 1`, `None` allowed) and `select_files(paths, limite) -> list[str]` (sort, then truncate) to `src/extracao_rfb/competencia.py`, passing T029/T030
- [X] T034 [US2] Wire the limit into `src/extracao_rfb/download_competencia.py`: validate before creating/list accessing, select the N first names, set `selecionados = min(limite, encontrados)`, compute `parcial = limite is not None and limite < encontrados`, and never select files outside N, passing T031
- [X] T035 [US2] Add `Param("limite", type=["null","integer"], minimum=1)` and pass `params["limite"]` to `download_competencia` in `airflow/dags/cnpj_download.py`, passing T032
- [X] T036 [US2] Add `--limite` (int, `>= 1`) to `src/extracao_rfb/extract_cnpj_files.py` and forward it to the domain function
- [X] T037 [US2] Run `pytest tests/unit/test_competencia.py tests/unit/test_selection.py tests/unit/test_download_competencia.py tests/unit/test_dag_import.py -q` and validate US2 scenarios 1–4

**Checkpoint**: US1 and US2 work independently; a `limite=1` run is the cheap validation path.

---

## Phase 5: User Story 3 - Reexecutar a mesma competência sem duplicar trabalho (Priority: P3)

**Goal**: garantir reexecução idempotente após interrupção, tratamento de arquivo de 0 byte e
de `.part` remanescente, e segurança contra execuções simultâneas da mesma competência.

**Independent Test**: executar duas vezes a mesma competência; a segunda termina com
`baixados=0`, todos `ignorados`, arquivos com mtime inalterado; um `.part` deixado pela
primeira execução não é tratado como completo; a segunda execução concorrente falha com
`ExecutionInProgressError` sem tocar arquivos.

### Tests for User Story 3 (write first, run failing) ⚠️

- [X] T038 [P] [US3] Extend `tests/unit/test_download_files.py` with failing tests: final file with `st_size > 0` is never requested and returns `ignorado`; final file with `st_size == 0` is downloaded again and replaced; leftover `.part` is removed at start and never consumed as complete; failed download leaves no `.part`
- [X] T039 [P] [US3] Extend `tests/unit/test_download_competencia.py` with failing tests: two concurrent calls for the same competência — one succeeds, the other raises `ExecutionInProgressError` after the lock timeout and touches no file; stale `.part` files are removed under the lock before listing; a second full run returns `baixados=0`, `ignorados=encontrados`, `bytes_baixados=0`; a run after a partial failure downloads only the missing files and preserves the rest
- [X] T040 [P] [US3] Extend `tests/e2e/test_download_local_server.py` with a two-run scenario: first run downloads, second run returns 0 bytes and leaves file contents and mtimes unchanged

### Implementation for User Story 3

- [X] T041 [US3] Add competência locking to `src/extracao_rfb/download_competencia.py`: exclusive `fcntl.flock` on `<competencia_dir>/.download.lock` with 5 s timeout, `ExecutionInProgressError` on timeout, and stale `.part` cleanup only while holding the lock, passing T039
- [X] T042 [US3] Refine `src/download_utils/download_files.py` and the orchestration for idempotency: skip only `is_file() and st_size > 0`, re-download 0-byte files, keep the unique `.part` name and `os.replace`, and remove temporary files in `finally`, passing T038
- [X] T043 [US3] Set `max_active_runs=1` and `retries=0` explicitly in `airflow/dags/cnpj_download.py` and document in `doc_md` that the lock covers CLI/`airflow dags test` runs outside the scheduler
- [X] T044 [US3] Run `pytest tests/unit/test_download_files.py tests/unit/test_download_competencia.py tests/e2e/test_download_local_server.py -q` and validate US3 scenarios 1–3

**Checkpoint**: All three stories are independently functional; reexecution is safe and observable.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: integração opcional com a RFB, documentação operacional e validação final do
quickstart/segurança.

- [X] T045 [P] Create `tests/integration/test_rfb_listing.py`: `pytest.mark.network`, skipped unless `RUN_NETWORK_TESTS=1`; assert the real `2026-02` listing has 37 files, is sorted and contains no directory entries
- [X] T046 [P] Update `airflow/README.md` with the DAG operation: build/up, trigger via UI/CLI, `competencia`/`limite`, `CNPJ_DOWNLOAD_DIR`, output layout `data/cnpj/<AAAA-MM>/`, test commands and the `airflow-tests` profile
- [X] T047 [P] Update root `README.md` with the new CLI usage (`python -m src.extracao_rfb.extract_cnpj_files`), the persistent output directory and the note that `data/cnpj/` MUST NOT be published (raw CPF data)
- [X] T048 Run the full suite with coverage: `docker compose --profile test run --rm airflow-tests tests --cov=src --cov-report=term-missing`; record the baseline in `airflow/README.md`
- [X] T049 Run scenarios 2–8 of `specs/002-cnpj-download-dag/quickstart.md` in the container: DAG loaded without network, `limite=1` first run, idempotent rerun, invalid competência (`2026-13`), invalid limit (`0`), unpublished competência (`2099-01` ⇒ `EmptyCompetenciaError`), unwritable directory and download failure
- [X] T050 Verify token safety in `airflow/logs/`, the task XCom and messages raised by `src/download_utils/redaction.py`: grep for `public.php/dav/files/`, `Authorization` and the token value; confirm zero occurrences and that `DownloadTarget.__repr__` hides the URL
- [X] T051 Final consistency review: `git status` shows no `data/cnpj/` or `.part` files, `pytest.ini`/compose/README match the implemented paths, and `specs/002-cnpj-download-dag/contracts/` has no drift from the code

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: can start immediately; T006 depends on T004/T005; T008 depends on T006; T009/T010 depend on T008 (same compose file)
- **Foundational (Phase 2)**: depends on Setup; each implementation task depends on its failing-test task (T012→T013, T014→T015, T018→T019, T020→T021); T011 precedes all tests
- **US1 (Phase 3)**: depends on Foundational (especially T015, T019, T021)
- **US2 (Phase 4)**: depends on US1 (T025/T026) for the orchestrator and DAG
- **US3 (Phase 5)**: depends on US1 and US2 (selection and summary shape already in place)
- **Polish (Phase 6)**: depends on all desired stories being complete

### User Story Dependencies

- **US1 (P1)**: no dependency on US2/US3 — delivers the MVP
- **US2 (P2)**: builds on US1's `download_competencia`/DAG; independently testable with `limite`
- **US3 (P3)**: builds on US1/US2; independently testable through reexecution and lock scenarios

### Within Each User Story

1. Tests written and failing
2. Domain/validation changes
3. Orchestration
4. DAG/CLI wiring
5. Story validation run

### Parallel Opportunities

- Setup: T002–T005 and T007 are independent files
- Foundational tests T012, T014, T018, T020 target different files; T016/T017 touch different hardening modules
- US1 tests T022–T024 are three different files
- US2 tests T029–T032 are four different files
- US3 tests T038–T040 are three different files
- Polish T045–T047 are three different documentation/test files

---

## Parallel Example: User Story 1

```bash
# Write the three failing test suites together:
Task: "T022 Write failing orchestration tests in tests/unit/test_download_competencia.py"
Task: "T023 Write failing DAG import tests in tests/unit/test_dag_import.py"
Task: "T024 Write failing e2e test in tests/e2e/test_download_local_server.py"
```

## Parallel Example: User Story 2

```bash
# All limit tests are in different files:
Task: "T029 Extend tests/unit/test_competencia.py with validate_limit cases"
Task: "T030 Write tests/unit/test_selection.py for select_files"
Task: "T031 Extend tests/unit/test_download_competencia.py with limit behavior"
Task: "T032 Extend tests/unit/test_dag_import.py with the limite Param schema"
```

## Parallel Example: User Story 3

```bash
Task: "T038 Extend tests/unit/test_download_files.py with idempotency/temp cases"
Task: "T039 Extend tests/unit/test_download_competencia.py with lock/resume cases"
Task: "T040 Extend tests/e2e/test_download_local_server.py with the two-run scenario"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: US1
4. **STOP and VALIDATE**: run T028; exercise the `2026-02` download with a small competência or the full set as capacity allows
5. Demo the DAG and the persistent output directory

### Incremental Delivery

1. Setup + Foundational → foundation ready
2. US1 → MVP: manual download, summary, failures visible
3. US2 → cheap first test with `limite` and explicit partial flag
4. US3 → safe reexecution, interruption recovery and concurrency protection
5. Polish → optional real-RFB integration test, docs and quickstart validation

### Parallel Team Strategy

1. One developer completes Setup + Foundational (shared primitives)
2. After Foundational:
   - Developer A: US1 (orchestrator + DAG + CLI)
   - Developer B: US2 tests and `competencia.py` selection once US1 lands
   - Developer C: US3 tests and lock/idempotency once US1/US2 land
3. Integrate and run the full suite in `airflow-tests`

---

## Notes

- `[P]` means different files and no dependency on an incomplete task
- `[USn]` maps each task to a user story for traceability
- Every test task MUST fail before its implementation task; do not weaken assertions to make it pass
- Preserve RFB file names exactly; never rename or derive names from URLs
- Never log URLs, tokens, credentials or CPF; log only names, status, counts, bytes and duration
- Do not implement Parquet, R2/MinIO, web or API here (FR-020)
- Commit after each task or logical group; do not commit `data/cnpj/` or `.part` files
