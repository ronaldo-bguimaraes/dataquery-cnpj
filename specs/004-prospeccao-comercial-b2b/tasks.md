---
description: "Task list for the B2B prospecting feature"
---

# Tasks: Prospecção Comercial B2B

**Input**: Design documents from `/specs/004-prospeccao-comercial-b2b/`

**Prerequisites**: spec.md, plan.md, data-model.md, quickstart.md

**Tests**: Obrigatórios (Constituição, Princípio V — TDD). Cada comportamento novo tem
teste antes da implementação; asserções não são enfraquecidas para obter verde.

**Organization**: Tarefas agrupadas por história: US1 (lista segmentada + CSV),
US2 (perfil/indicadores), US3 (agregações por segmento/UF).

## Phase 1: Setup

- [X] T001 Criar `specs/004-prospeccao-comercial-b2b/` com spec, plan, data-model, quickstart e tasks.
- [X] T002 Adicionar `tests/query/unit/test_segmentos.py` com o catálogo esperado e casos de CNAE ausente/fora do mapa.
- [X] T003 [P] Adicionar `tests/query/unit/test_indicadores.py` para idade até o fim da competência, idade ausente e presença de contato.

## Phase 2: Foundational (bloqueia todas as histórias)

- [X] T004 Implementar `src/query/domain/segmentos.py` (catálogo + mapa divisão→segmento + funções).
- [X] T005 [P] Implementar `src/query/domain/indicadores.py` (idade, deslocamento de anos, contato).
- [X] T006 Evoluir `src/query/db/schema.sql`: colunas novas em `estabelecimentos_listagem`, tabelas `segmentos`/`segmento_divisoes`, índices e `ALTER TABLE ... IF NOT EXISTS` para bancos existentes.
- [X] T007 [P] Estender `src/query/web/filters.py` com `segmento`, `idade_min/max`, `filiais_min/max`, `com_contato` e validação (`tests/query/unit/test_filters.py` primeiro).
- [X] T008 [P] Popular as tabelas de segmentos na carga local (`src/query/ingest/postgres.py`) e no bulk (`src/ingest_parallel/bulk.py`), com inclusão em `_truncate`.
- [X] T009 Estender o build do read model (loader local e `DERIVED_LISTAGEM_SQL`) com contatos, CEP, natureza jurídica, capital social, segmento, `qtd_cnaes`, `qtd_estabelecimentos` e `qtd_filiais`.
- [X] T010 Atualizar fixtures de teste (`tests/query/fixtures/parquets.py`) com um CNAE de varejo e um estabelecimento sem contato; incluir as tabelas novas no `conftest`.

## Phase 3: US1 — Lista segmentada e CSV (P1)

- [X] T011 [US1] Testes de integração em `tests/query/integration/test_prospeccao.py`: filtros de segmento, contato, filiais e idade; contagem; lista com novas colunas.
- [X] T012 [US1] Implementar `listar_prospeccao` e `_where` estendido em `src/query/db/repositories.py`, mantendo a contagem limitada.
- [X] T013 [US1] Testes de contrato em `tests/query/contract/test_prospeccao_page.py`: página, padrão ativo, filtro inválido, CSV, aviso de teto.
- [X] T014 [US1] Implementar rota `/prospeccao` e template `prospeccao.html` com formulário, tabela, vazio e paginação.
- [X] T015 [US1] Implementar `exportar_prospeccao` com cursor nomeado e a rota `/prospeccao.csv` em streaming, com escape e neutralização de fórmulas.
- [X] T016 [US1] Configurar `CNPJ_QUERY_EXPORT_LIMIT` em `src/query/config.py`, `.env.example` e Compose.

## Phase 4: US2 — Perfil e indicadores (P2)

- [X] T017 [US2] Exibir idade (até o fim da competência), qtd. CNAEs, qtd. estabelecimentos, qtd. filiais, segmento, naturezas e capital social na listagem e no CSV.
- [X] T018 [US2] Adicionar detalhe de prospecção (link para o estabelecimento) preservando filtros e página.

## Phase 5: US3 — Agregações (P3)

- [X] T019 [US3] Implementar `agrupar_prospeccao` (segmento e UF) com o mesmo `WHERE`.
- [X] T020 [US3] Renderizar os dois painéis de contagem e testar a consistência com o total.

## Phase 6: Polish

- [X] T021 Rodar `pytest tests/query -q -m "not e2e"` e corrigir regressões.
- [X] T022 Atualizar `query/README.md`, `README.md` e `docs/roadmap-dados.md` com a página, o CSV e as medições.
- [X] T023 Registrar medições locais de listagem e exportação.
- [X] T024 Revisar segurança: sem CPF no CSV/página/logs; inputs sempre parametrizados.
