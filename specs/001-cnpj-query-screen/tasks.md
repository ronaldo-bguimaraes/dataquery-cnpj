---
description: "Task list for the CNPJ visual explorer MVP"
---

# Tasks: Explorador Visual de Dados CNPJ

**Input**: Design documents from `/specs/001-cnpj-query-screen/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/, quickstart.md

**Tests**: Obrigatórios por decisão da constituição (Princípio V, TDD). Cada implementação tem
teste anterior; escreva e execute o teste **falhando** antes de implementar. Não enfraquecer
asserções para obter verde.

**Organization**: Tarefas agrupadas por história de usuário (US1 e US2), com setup e
fundação compartilhados. O MVP é a US1; a US2 adiciona a página de detalhe.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: pode rodar em paralelo (arquivos diferentes, sem dependência de tarefa incompleta)
- **[Story]**: `[US1]` ou `[US2]` nas fases de história; setup/fundação/polish não têm label
- Toda tarefa cita caminho exato de arquivo

## Path Conventions

Projeto único: `src/query/`, `tests/query/` e `query/` na raiz do repositório. Os serviços
do portal usam o profile Compose `query`; os testes usam `query-test`.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Criar a estrutura do portal, dependências, imagem, banco e configuração base.

- [X] T001 Criar a estrutura de pacotes `src/query/__init__.py`, `src/query/domain/__init__.py`, `src/query/ingest/__init__.py`, `src/query/db/__init__.py`, `src/query/web/__init__.py` e os diretórios `src/query/web/templates/` e `src/query/web/static/`.
- [X] T002 Adicionar as dependências de runtime em `query/requirements.txt` (FastAPI, Uvicorn, Jinja2, `psycopg[binary,pool]`) e as de teste em `query/requirements-dev.txt` (pytest, pytest-cov, httpx, Playwright), compatíveis com Python 3.13+.
- [X] T003 [P] Criar `query/Dockerfile` com estágios para loader/web/testes, instalação de `query/requirements.txt` e `query/requirements-dev.txt`, execução como usuário não-root e sem credenciais embutidas.
- [X] T004 [P] Criar `query/initdb/roles.sh` para criar `cnpj_query_loader` (escrita/carga) e `cnpj_query_app` (somente leitura), lendo senhas de variáveis de ambiente e sem segredo hardcoded.
- [X] T005 [P] Criar `query/docker-compose.yaml` com `query-db` (PostgreSQL 16 + volume + healthcheck), `query-loader`, `query-web` e `query-tests`; profiles `query`/`query-test`, mount `${CNPJ_DATA_HOST_DIR:-../data}:/data` no loader e `depends_on` saudável.
- [X] T006 [P] Incluir `query/docker-compose.yaml` no `docker-compose.yaml` raiz, mantendo o caminho padrão `docker compose up -d` restrito ao Airflow.
- [X] T007 [P] Atualizar `.env.example` com placeholders de `CNPJ_QUERY_COMPETENCIA`, `CNPJ_QUERY_DATABASE_URL`, `CNPJ_QUERY_PAGE_SIZE`, `CNPJ_QUERY_SOURCE`, `POSTGRES_QUERY_*` e senhas dos papéis, sem valores reais.
- [X] T008 [P] Atualizar `pytest.ini` com os markers `integration`, `e2e` e `perf`, preservando o marker `network` e o `testpaths = tests` existentes.

**Checkpoint**: estrutura, imagem, banco, papéis e configuração prontos.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Domínio, schema PostgreSQL, loader idempotente e fixtures de teste. Nenhuma
história pode começar antes desta fase.

**⚠️ CRITICAL**: sem o loader e o schema de consulta, US1 e US2 não têm dados.

### Tests for Foundational Layer (write first, run failing) ⚠️

- [X] T009 [P] Criar `tests/query/fixtures/parquets.py` com builders de Parquets sintéticos no formato do pipeline (todas as colunas como texto, colunas de `schema.py`), incluindo um helper `escrever_inventario_completo` para as 37 famílias, datas `AAAAMMDD`, `00000000`, capital com vírgula, CNAEs secundários separados por vírgula e campos opcionais vazios.
- [X] T010 Criar `tests/query/conftest.py` com fixtures `source_dir` (usa T009) e `query_db` (conecta em `CNPJ_QUERY_TEST_DATABASE_URL`, aplica `src/query/db/schema.sql` e limpa as tabelas entre testes).
- [X] T011 [P] Escrever `tests/query/unit/test_normalization.py` cobrindo CNPJ completo 8+4+2=14 dígitos, datas `AAAAMMDD` com `00000000`/vazio → `NULL`, capital `5000,00` → `NUMERIC(18,2)`, CNAEs separados por `,` ou `;` e nome normalizado sem caixa/acentos.
- [X] T012 [P] Escrever `tests/query/unit/test_masking.py` cobrindo CPF completo, CPF já mascarado, vazio e redação defensiva de padrões de CPF em texto livre; nenhuma saída pode conter 11 dígitos.
- [X] T013 [P] Escrever `tests/query/unit/test_codebook.py` cobrindo situação `01/02/03/04/05/08`, porte `00/01/03/05`, matriz/filial `1/2` e fallback para código desconhecido.
- [X] T014 [P] Escrever `tests/query/unit/test_inventory.py` cobrindo descoberta das famílias `Empresas*`, `Estabelecimentos*`, `Socios*`, `Simples` e 6 tabelas de apoio; falha para família ausente, `Simples.parquet` duplicado e colunas divergentes de `schema.py`.
- [X] T015 [P] Escrever `tests/query/integration/test_load.py` para o caminho feliz do loader: staging + troca atômica, contagens de empresas/estabelecimentos/sócios, `query_metadata` ativo, `estabelecimento_cnaes` com principal/secundários, `optante_simples`/`optante_mei` até o fim da competência e idempotência de recarga.
- [X] T016 [P] Escrever `tests/query/integration/test_load_failures.py` cobrindo falha de validação de schema, família ausente, contagem divergente, rollback sem publicar e ausência de CPF em claro em `socios`.
- [X] T017 Executar `docker compose --profile query-test run --rm query-tests pytest tests/query/unit tests/query/integration/test_load.py tests/query/integration/test_load_failures.py -q` e confirmar que os testes falham antes da implementação.

### Implementation for Foundational Layer

- [X] T018 [P] Implementar `src/query/domain/normalization.py` com CNPJ completo 8+4+2, datas `AAAAMMDD`/`00000000`, capital com vírgula, split de CNAEs por `,`/`;`, normalização de nome (minúsculas, sem acentos/pontuação) e validação de código CNAE de 7 dígitos.
- [X] T019 [P] Implementar `src/query/domain/masking.py` com máscara `***.***.***-**`, preservação de vazio e redação defensiva de padrões de CPF em texto livre.
- [X] T020 [P] Implementar `src/query/domain/codebook.py` com os catálogos oficiais de situação, porte e matriz/filial, incluindo fallback para código desconhecido.
- [X] T021 Implementar `src/query/config.py` com leitura e validação de competência `AAAA-MM`, URL do banco, página 25, fonte `Receita Federal`, raiz de dados e backend de storage; falhar com mensagem clara sem expor credenciais.
- [X] T022 Implementar `src/query/db/schema.sql` com `CREATE EXTENSION pg_trgm`, tabelas base, `estabelecimento_cnaes`, sócios, Simples/MEI, tabelas de apoio, catálogo, `municipios_por_uf`, `cnaes_presentes`, `estabelecimentos_listagem` e `query_metadata`, chaves/índices do data-model e `GRANT SELECT` para o papel `cnpj_query_app`.
- [X] T023 Implementar `src/query/ingest/inventory.py` para descobrir e validar os 37 Parquets da competência, comparar colunas com `schema.py` e falhar antes de qualquer escrita no banco.
- [X] T024 Implementar `src/query/ingest/transform.py` para ler os Parquets em lotes, derivar CNPJ completo, datas, capital, CNAEs, nome normalizado, CPF mascarado, optantes e rótulos de domínio, mantendo contadores de valores descartados.
- [X] T025 Implementar `src/query/ingest/postgres.py` com DDL, `COPY` em staging, criação de índices, validação de contagens/referências e troca atômica das tabelas; falha deve fazer rollback.
- [X] T026 Implementar `src/query/ingest/load.py` como entrypoint `python -m src.query.ingest.load`, orquestrando inventory → transform → staging → validação → publicação e registrando duração/memória/contagens sem PII.
- [X] T027 Implementar `src/query/db/connection.py` com pool psycopg para a web usando o papel somente leitura e configuração de timeout; não usar `cnpj_query_loader` na aplicação.
- [X] T028 Implementar `src/query/db/repositories.py` com helpers de consulta parametrizada, mapeamento de linhas e acesso ao `query_metadata`; as consultas específicas de listagem/detalhe entram nas histórias.
- [X] T029 Executar `tests/query/unit` e `tests/query/integration/test_load.py`/`test_load_failures.py` e corrigir até ficar verde.
- [X] T030 Validar `src/query/ingest/load.py` com `source_dir` de fixtures: executar `python -m src.query.ingest.load`, conferir `query_metadata` ativo e confirmar que uma segunda execução substitui a competência sem duplicar linhas.

**Checkpoint**: domínio, schema, loader e fixtures prontos; US1 e US2 podem começar.

---

## Phase 3: User Story 1 - Explorar e filtrar registros (Priority: P1) 🎯 MVP

**Goal**: listagem de estabelecimentos com competência/fonte, filtros combináveis, contagem
total, paginação e estados vazio/erro.

**Independent Test**: abrir o site com a competência `2026-10`, confirmar competência/fonte,
aplicar UF + situação + CNAE, ver contagem e lista, navegar entre páginas e alterar filtros.

### Tests for User Story 1 (write first, run failing) ⚠️

- [X] T031 [P] [US1] Escrever `tests/query/unit/test_filters.py` cobrindo CNPJ 14 dígitos/verificadores, UF, município, CNAE 7 dígitos, situação `01/02/03/04/05/08`, porte `00/01/03/05`, matriz `1/2`, datas `AAAA-MM-DD` inclusivas com `abertura_de <= abertura_ate`, página ≥ 1, nome com ≥ 3 caracteres, Simples/MEI `sim`/`nao`/`qualquer` e parâmetro repetido.
- [X] T032 [P] [US1] Escrever `tests/query/contract/test_listing_pages.py` cobrindo `GET /` (303), `GET /estabelecimentos` (200), competência/fonte/aviso de retrato mensal, contagem, linhas, paginação, estado vazio, `400`, `503`, `GET /health`, ausência de exportação/download e requisições sequenciais independentes.
- [X] T033 [P] [US1] Escrever `tests/query/integration/test_listing.py` cobrindo filtros cumulativos, CNAE principal ou secundário, UF + município, porte, matriz/filial, intervalo de abertura inclusivo, Simples/MEI, busca literal por CNPJ/nome, contagem exata, ordenação por CNPJ, paginação sem repetir/omitir e contagem zero.
- [X] T034 [US1] Executar `tests/query/unit/test_filters.py`, `tests/query/contract/test_listing_pages.py` e `tests/query/integration/test_listing.py` e confirmar que falham antes da implementação.

### Implementation for User Story 1

- [X] T035 [P] [US1] Implementar `src/query/web/filters.py` para validar/normalizar parâmetros da query string conforme T031, rejeitando parâmetro repetido e sem consultar o banco quando inválido.
- [X] T036 [US1] Implementar `listar_estabelecimentos` e `contar_estabelecimentos` em `src/query/db/repositories.py`, com SQL parametrizado, `EXISTS` em `estabelecimento_cnaes`, `ORDER BY cnpj_completo`, `LIMIT 25 OFFSET`, contagem exata e uso de `query_metadata.total_estabelecimentos` quando não houver filtros.
- [X] T037 [US1] Implementar `opcoes_filtro` em `src/query/db/repositories.py` para UFs, municípios por UF, CNAEs presentes, situações, portes, matriz/filial e opções de Simples/MEI, sem inventar valores.
- [X] T038 [P] [US1] Implementar `src/query/web/presenters.py` com view models da listagem, formatação de contagem/datas/rótulos, links de paginação e redação defensiva de CPF; não colocar SQL nos presenters.
- [X] T039 [US1] Implementar `src/query/web/app.py` com app factory FastAPI, Jinja2, static files, lifespan do pool PostgreSQL e handlers `400/404/503/500`; não registrar valores de filtros nos logs.
- [X] T040 [US1] Implementar `GET /`, `GET /estabelecimentos` e `GET /health` em `src/query/web/routes.py`, usando `filters.py`, `repositories.py` e `presenters.py`.
- [X] T041 [P] [US1] Criar `src/query/web/templates/base.html` com competência `2026-10`, fonte `Receita Federal`, aviso de retrato mensal, navegação e nenhum controle de exportação/download.
- [X] T042 [P] [US1] Criar `src/query/web/templates/estabelecimentos.html` com formulário de filtros, contagem total, linhas (CNPJ, razão social, nome fantasia, situação, UF, município, CNAE principal, matriz/filial, abertura, Simples/MEI), paginação e estado vazio com limpar filtros.
- [X] T043 [P] [US1] Criar `src/query/web/templates/erro.html` para `400/404` genérico/`503/500`, com mensagem compreensível, sem detalhe interno, e ação de tentar novamente quando aplicável.
- [X] T044 [P] [US1] Criar `src/query/web/static/estilo.css` com layout legível, formulário acessível, tabela responsiva e foco visível.
- [X] T045 [US1] Executar `tests/query/unit/test_filters.py`, `tests/query/contract/test_listing_pages.py` e `tests/query/integration/test_listing.py`; validar manualmente no navegador competência/fonte, filtros, CNAE principal/secundário, paginação, estado vazio, `400` e `503`.

**Checkpoint**: US1 funcional e testável isoladamente; é o MVP demonstrável.

---

## Phase 4: User Story 2 - Consultar detalhes do estabelecimento (Priority: P2)

**Goal**: página de detalhe com estabelecimento, empresa, sócios e Simples/MEI, preservando
filtros e página ao retornar.

**Independent Test**: a partir de um resultado da listagem, abrir um estabelecimento conhecido
e conferir dados do estabelecimento, empresa, sócios e Simples/MEI, com CPF mascarado e link de
retorno preservando a URL.

### Tests for User Story 2 (write first, run failing) ⚠️

- [X] T046 [P] [US2] Escrever `tests/query/contract/test_detail_pages.py` cobrindo `GET /estabelecimentos/{cnpj}` (200), seções de estabelecimento/empresa/sócios/Simples, contatos com aviso de RFB, CPF mascarado, campos opcionais ausentes, `400` para CNPJ inválido, `404` para inexistente, link de retorno e ausência de exportação/download.
- [X] T047 [P] [US2] Escrever `tests/query/integration/test_detail.py` cobrindo consulta por CNPJ completo, empresa, sócios, CNAEs principal/secundários, Simples/MEI, lookups, ausência de CPF em claro e seções vazias sem erro.
- [X] T048 [US2] Executar `tests/query/contract/test_detail_pages.py` e `tests/query/integration/test_detail.py` e confirmar que falham antes da implementação.

### Implementation for User Story 2

- [X] T049 [US2] Implementar em `src/query/db/repositories.py` as consultas `obter_estabelecimento`, `listar_socios`, `listar_cnaes` e `obter_simples`, com parâmetros seguros, lookup de descrições e CPF somente mascarado.
- [X] T050 [US2] Implementar em `src/query/web/presenters.py` o view model de detalhe, seções opcionais, redação defensiva de CPF e URL de retorno com filtros/página.
- [X] T051 [US2] Implementar `GET /estabelecimentos/{cnpj}` em `src/query/web/routes.py`, normalizando máscara, retornando `400` para CNPJ inválido e `404` para inexistente.
- [X] T052 [P] [US2] Criar `src/query/web/templates/detalhe.html` com dados do estabelecimento, endereço, CNAEs, contatos com aviso de RFB, empresa, sócios e Simples/MEI.
- [X] T053 [P] [US2] Criar `src/query/web/templates/nao_encontrado.html` para CNPJ válido inexistente, com ação de voltar à listagem.
- [X] T054 [US2] Garantir em `src/query/web/presenters.py` e `src/query/web/routes.py` que o link de retorno do detalhe reconstrua `/estabelecimentos` com os mesmos filtros e página; cobrir em `tests/query/contract/test_detail_pages.py`.
- [X] T055 [US2] Executar `tests/query/contract/test_detail_pages.py` e `tests/query/integration/test_detail.py`; validar manualmente o fluxo listagem → detalhe → retorno.

**Checkpoint**: US1 e US2 funcionam independentemente e integradas.

---

## Phase 5: Polish & Cross-Cutting Concerns

**Purpose**: E2E, segurança, benchmark, documentação e validação final.

- [X] T056 [P] Criar `tests/query/e2e/test_explorer_flow.py` com Playwright cobrindo filtro UF+CNAE, paginação, detalhe, retorno com filtros/página, CPF mascarado e ausência de exportação/download; marcar com `pytest.mark.e2e` e skip explícito quando o navegador não estiver disponível.
- [X] T057 [P] Implementar `src/query/benchmark.py` com CLI `python -m src.query.benchmark --requisicoes N`, medindo p95 de listagem/detalhe e sem registrar filtros/CPF.
- [X] T058 [P] Criar `tests/query/integration/test_security.py` cobrindo CPF ausente em claro, logs sem termos de busca/CPF, papel web sem INSERT/UPDATE/DELETE, HTML escapado, SQL parametrizado e ausência de rotas de exportação/download.
- [X] T059 Executar `tests/query/integration/test_security.py`, corrigir falhas e repetir até verde.
- [X] T060 [P] Adicionar logging estruturado e medição de duração em `src/query/web/app.py`, registrando rota/status/tempo e **nunca** valores de filtros, CPF ou conteúdo de ficha.
- [X] T061 [P] Atualizar `README.md` e criar `query/README.md` com comandos, variáveis, profiles, arquitetura, operação do loader, baseline e nota de que API para terceiros/Superset são evoluções futuras.
- [X] T062 Executar `docker compose --profile query-test run --rm query-tests pytest tests/query -q -m "not e2e"` com cobertura, depois `-m e2e` quando Playwright estiver disponível, e corrigir regressões.
- [ ] T063 Executar `python -m src.query.benchmark` com a competência completa `2026-10`, registrar p95 de listagem/detalhe, duração e memória da carga no `README.md`; ajustar índices/consultas se o alvo não for atingido.
- [ ] T064 Executar o teste de usabilidade de `quickstart.md` §4.9 com participantes sem perfil predefinido e registrar protocolo/resultado em `docs/validacao-usabilidade.md` (meta ≥ 90%).
- [ ] T065 Executar o quickstart completo e validar todas as caixas de `specs/001-cnpj-query-screen/quickstart.md`; atualizar comandos divergentes.
- [X] T066 Auditoria final: conferir em `specs/001-cnpj-query-screen/quickstart.md` e no código ausência de exportação/download, dados de Lucro/PGFN/CNO, comparação histórica e CPF em claro; rodar `git diff --check` e confirmar que `data/` não contém artefatos versionáveis.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: sem dependências; começa imediatamente.
- **Foundational (Phase 2)**: depende do Setup; bloqueia US1 e US2.
- **US1 (Phase 3)**: depende da Fundação; é o MVP.
- **US2 (Phase 4)**: depende da Fundação para as consultas; a integração de retorno depende da US1. Na ordem de entrega, concluir US1 antes de US2.
- **Polish (Phase 5)**: depende de US1 e US2.

### User Story Dependencies

- **US1 (P1)**: independente; entrega listagem, filtros, contagem, paginação, competência/fonte.
- **US2 (P2)**: testável com um CNPJ conhecido; a navegação de retorno é validada junto da US1.
- **API para terceiros/Superset**: fora do MVP; não criar tarefas de API, autenticação, BI ou dashboards aqui.

### Within Each User Story

- Testes primeiro; executar e confirmar falha.
- Models/domínio antes de repositórios.
- Repositórios antes de rotas/presenters.
- Rotas antes dos templates.
- Integração antes do checkpoint.

### Parallel Opportunities

- Setup: T003, T004, T007 e T008.
- Fundação: T009 em paralelo com a estrutura; depois T011–T016 em paralelo (arquivos diferentes).
- US1: T031–T033 em paralelo; T041–T044 em paralelo após o contrato de dados da listagem.
- US2: T046–T047 em paralelo; T052–T053 em paralelo.
- Polish: T056–T058 e T060–T061 em paralelo.

### Parallel Example: User Story 1

```bash
# Testes de US1 em paralelo
Task: "Escrever tests/query/unit/test_filters.py"
Task: "Escrever tests/query/contract/test_listing_pages.py"
Task: "Escrever tests/query/integration/test_listing.py"

# Templates em paralelo, após o view model da listagem
Task: "Criar src/query/web/templates/base.html"
Task: "Criar src/query/web/templates/estabelecimentos.html"
Task: "Criar src/query/web/templates/erro.html"
Task: "Criar src/query/web/static/estilo.css"
```

---

## Implementation Strategy

### MVP First (US1)

1. Setup (Phase 1).
2. Fundação (Phase 2) — loader, schema, fixtures.
3. US1 (Phase 3) — listagem, filtros, contagem, paginação.
4. **STOP and VALIDATE**: executar testes de US1 e validar no navegador.
5. Demonstrar o MVP com a competência `2026-10`.

### Incremental Delivery

1. Setup + Fundação → base pronta.
2. US1 → MVP demonstrável.
3. US2 → detalhe e navegação de retorno.
4. Polish → E2E, segurança, benchmark, documentação e usabilidade.
5. Cada etapa mantém os testes anteriores verdes.

### Parallel Team Strategy

1. Time conclui Setup + Fundação.
2. Um desenvolvedor pode assumir US1; outro pode preparar fixtures/testes de US2.
3. US2 integra após a listagem estar funcional.
4. Polish em paralelo com revisão de segurança/documentação.

---

## Notes

- `[P]` indica arquivos diferentes; nunca executar em paralelo tarefas que editam o mesmo arquivo (`repositories.py`, `presenters.py`, `routes.py`, `app.py`).
- Testes são obrigatórios; não marcar tarefa como concluída sem o teste correspondente verde.
- Não implementar exportação/download, API pública, autenticação, BI, histórico ou busca fuzzy.
- Manter `estabelecimentos_listagem` e os repositórios desacoplados da interface para a futura API/Superset.
- Rodar `git diff --check` e garantir que `data/` não receba ZIP/Parquet versionável.
- Commits por grupo lógico, seguindo Conventional Commits.
