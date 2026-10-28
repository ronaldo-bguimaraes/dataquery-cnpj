# Implementation Plan: Explorador Visual de Dados CNPJ

**Branch**: `001-cnpj-query-screen` | **Date**: 2026-10-27 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-cnpj-query-screen/spec.md`

## Summary

Entregar um explorador web somente leitura dos dados públicos de CNPJ da competência
`2026-10`: listagem de estabelecimentos com filtros combináveis, contagem total,
paginação e uma página de detalhes com empresa, sócios e Simples/MEI.

A camada de consulta será um **PostgreSQL** local, preparado a partir dos Parquets Bronze
já existentes (decisão registrada em `docs/roadmap-dados.md`). Um job de carga lê as
famílias `Empresas*`, `Estabelecimentos*`, `Socios*`, `Simples` e as tabelas de apoio,
normaliza os valores textuais, mascara CPF e materializa tabelas/índices orientados às
consultas. A aplicação web será **Python + FastAPI + Jinja2**, com formulários GET e
páginas renderizadas no servidor; não há SPA, API pública, autenticação, exportação,
busca aproximada ou comparação histórica no MVP. A camada de consulta é mantida separada
da interface para permitir uma futura API para terceiros e ferramentas de BI, sem refazer
a carga nem duplicar regras.

## Technical Context

**Language/Version**: Python 3.13+ (runtime do projeto; o venv local usa 3.15), SQL, HTML/CSS

**Primary Dependencies**:
- Aplicação: FastAPI, Uvicorn, Jinja2, psycopg 3 (`psycopg[binary,pool]`)
- Carga: DuckDB e PyArrow (já presentes no projeto), psycopg 3
- Banco: PostgreSQL 16 (mesma versão do serviço já usada no Compose)
- Testes: pytest, httpx/TestClient, PostgreSQL de teste; Playwright opcional para E2E
- Extensão PostgreSQL: `pg_trgm` (índice para busca literal por substring)

**Storage**: PostgreSQL como camada consultável; Parquets Bronze como fonte imutável em
`/data/bronze/<AAAA-MM>/` (filesystem) ou no bucket S3/R2 configurado pelo pipeline

**Testing**: pytest (unitário, integração com PostgreSQL e contrato HTTP); Playwright
para o fluxo E2E principal; benchmark separado para desempenho

**Target Platform**: Linux + Docker Compose; navegador web (desktop como alvo principal,
layout básico responsivo)

**Project Type**: aplicação web + job de preparação/carga de dados

**Performance Goals**: listagem filtrada com contagem + primeira página em ≤ 5 s (p95);
detalhe de estabelecimento em ≤ 3 s (p95); página padrão de 25 registros

**Constraints**: uma única competência (`2026-10`); somente leitura; sem exportação ou
download; sem autenticação; sem busca tolerante a erros; CPF sempre mascarado; sem dados
de fontes externas; sem comparação histórica

**Scale/Scope**: 37 Parquets da competência `2026-10` (10 `Empresas*`, 10
`Estabelecimentos*`, 10 `Socios*`, 1 `Simples`, 6 tabelas de apoio); 70,5 M empresas,
73,8 M estabelecimentos, 28,5 M sócios e 50,8 M registros de Simples; duas páginas
principais (listagem e detalhe) e um job de carga

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Princípio / Restrição | Verificação | Resultado |
|---|---|---|
| I. Estágios Desacoplados e Independentes | Carga e aplicação web são artefatos separados; a carga lê Parquet e grava PostgreSQL; a web só lê o banco | ✅ PASS |
| II. Contrato de Dados Explícito e Schema-First | `schema.py` continua sendo a fonte dos nomes de coluna; o loader valida o schema de origem e versiona o schema de consulta em SQL | ✅ PASS |
| III. Streaming e Uso Mensurável de Recursos | Carga em lotes com PyArrow/DuckDB e `COPY`; nenhuma tabela completa é carregada na memória da web; consultas paginadas | ✅ PASS |
| IV. Falhas Visíveis e Execução Observável | Loader valida contagens e referências antes de publicar; falhas fazem rollback; web tem estados de erro e logs sem PII | ✅ PASS |
| V. Testes Primeiro (NON-NEGOTIABLE) | Testes de normalização, carga, filtros, paginação, detalhe, máscara e E2E serão escritos antes da implementação | ✅ PASS |
| VI. Simplicidade e Dependências Disciplinadas | PostgreSQL já é a decisão do projeto; FastAPI/Jinja2 evitam uma segunda linguagem e um SPA; sem busca fuzzy, ORM ou cache distribuído | ✅ PASS |
| Segurança e LGPD | CPF de sócios é mascarado na carga e nunca chega ao banco de consulta em claro; web usa papel somente leitura; logs não registram termos de busca nem CPF | ✅ PASS |
| Infra via `docker compose` | `query-db`, `query-loader`, `query-web` e `query-tests` serão serviços versionados no Compose | ✅ PASS |
| Storage pela API S3 | O loader lê os Parquets pelo backend de storage já configurado (filesystem ou S3/R2); a web não acessa armazenamento | ✅ PASS |
| Baseline de performance | Carga e consultas terão medição de duração, memória e p95 registradas no README | ✅ PASS |
| Escopo v1 | Exportação, histórico, dashboards, IA, cobrança, autenticação e fontes externas ficam fora | ✅ PASS |

**Resultado**: aprovado sem violações que exijam justificativa.

## Project Structure

### Documentation (this feature)

```text
specs/001-cnpj-query-screen/
├── plan.md              # This file
├── spec.md              # Feature specification
├── research.md          # Phase 0 decisions
├── data-model.md        # Phase 1 logical/physical model
├── quickstart.md        # Phase 1 validation guide
├── tasks.md             # Phase 2 task list (/speckit-tasks)
├── checklists/
│   └── requirements.md
└── contracts/
    ├── README.md
    ├── query-api.md     # HTTP page contract (HTML, no public JSON API)
    └── data-contracts.md
```

### Source Code (repository root)

```text
src/query/
├── __init__.py
├── config.py                 # ambiente, competência, página, conexão
├── domain/
│   ├── __init__.py
│   ├── codebook.py           # situação, porte, matriz/filial, Simples/MEI
│   ├── masking.py            # CPF e redação defensiva de texto livre
│   └── normalization.py      # nome, CNAE, datas, capital social
├── ingest/
│   ├── __init__.py
│   ├── inventory.py          # descobre e valida os Parquets da competência
│   ├── transform.py          # leitura em lotes + regras de derivação
│   ├── postgres.py           # DDL, COPY, índices e swap atômico
│   └── load.py               # entrypoint do job de carga
├── db/
│   ├── __init__.py
│   ├── connection.py         # pool psycopg somente leitura na web
│   ├── repositories.py       # consultas reutilizáveis por HTML e futura API
│   └── schema.sql            # tabelas, views e índices
├── benchmark.py              # medição p95 da listagem/detalhe
└── web/
    ├── __init__.py
    ├── app.py                # aplicação FastAPI
    ├── routes.py             # listagem, detalhe e saúde
    ├── filters.py            # validação e normalização dos filtros
    ├── presenters.py         # view models e máscaras de exibição
    ├── templates/
    └── static/

query/
├── Dockerfile
├── docker-compose.yaml       # query-db, query-loader, query-web, query-tests
├── README.md                 # comandos, variáveis e operação do portal
├── initdb/
│   ├── roles.sh              # papéis read-only (web) e de carga (loader)
│   └── 02-test-db.sh        # banco isolado para a suíte de testes
├── requirements.txt          # dependências exclusivas do portal
└── requirements-dev.txt      # pytest, httpx, Playwright (testes)

tests/query/
├── fixtures/                 # Parquets sintéticos pequenos
├── unit/
├── integration/
├── contract/
└── e2e/
```

**Structure Decision**: o código novo fica em `src/query/`, separado do pipeline
(`src/pipeline/`, `src/conversao/`, `src/storage/`). O job de carga e a aplicação web
compartilham o mesmo pacote, mas são executados por entrypoints distintos. A stack
Docker do portal fica em `query/docker-compose.yaml` e é incluída pelo Compose raiz,
como já ocorre com `airflow/docker-compose.yaml`.

## Complexity Tracking

| Decisão | Por que é necessária | Alternativa mais simples rejeitada |
|---|---|---|
| PostgreSQL como segunda camada de dados | Decisão registrada em `docs/roadmap-dados.md`; índices relacionais são necessários para filtros combinados, contagem e paginação sobre dezenas de milhões de linhas | Consultar Parquet diretamente com DuckDB a cada requisição: simples, mas sem o modelo relacional/indexado já decidido e com risco de varredura completa em consultas amplas |
| Tabela materializada `estabelecimentos_listagem` | Evita repetir joins e cálculos de descrição/máscara em cada consulta da página principal | View com todos os joins: menos armazenamento, porém maior custo por consulta no volume alvo |
| Tabela `estabelecimento_cnaes` | O CNAE secundário é uma lista textual; normalizar permite filtrar principal ou secundário com índice, sem `LIKE` em lista | Filtrar por `LIKE` na coluna original: simples, mas lento e sujeito a falsos positivos |
| Job de carga separado da web | Mantém a web leve, somente leitura, e permite validar/rollback antes de publicar a competência | Carregar sob demanda na primeira requisição: acopla disponibilidade e latência ao primeiro acesso |
| Catálogo estático de domínio | Situação cadastral, porte e matriz/filial são códigos oficiais sem tabela no Parquet; a interface precisa de rótulos legíveis | Exibir apenas códigos: reduz manutenção, mas piora a compreensão e os filtros |

## Phase 0: Outline & Research

As decisões completas, alternativas e referências estão em [research.md](./research.md).
Resumo:

- **D1 — PostgreSQL como camada de consulta**: alinhado à decisão do projeto, oferece
  índices, joins, contagem e paginação previsíveis; os Parquets continuam como fonte.
- **D2 — Carga a partir dos Parquets atuais**: um job lê as famílias de arquivos da
  competência, valida o schema e materializa o modelo relacional; não altera o pipeline
  nem cria novas fontes.
- **D3 — Modelo de consulta normalizado**: empresas, estabelecimentos, CNAEs,
  sócios, Simples/MEI, tabelas de apoio e uma tabela de listagem denormalizada.
- **D4 — Web server-rendered**: FastAPI + Jinja2 com formulários GET, sem SPA e sem API
  pública; o estado dos filtros vive na URL, o que preserva a listagem ao voltar do detalhe.
- **D5 — Busca literal**: CNPJ exato e nome por substring normalizada (maiúsculas/acentos),
  sem tolerância a erros; `pg_trgm` apenas como índice de substring, não como similaridade.
- **D6 — Paginação por offset**: página de 25 registros, ordenação estável por CNPJ e
  contagem exata; a primeira página é o alvo de desempenho.
- **D7 — Privacidade**: CPF mascarado na carga; banco de consulta sem CPF em claro; papel
  somente leitura para a web; logs sem termos de busca ou PII.
- **D8 — Operação local**: serviços `query-db`, `query-loader`, `query-web` e
  `query-tests` no Compose, nos profiles `query`/`query-test`, com volume próprio e
  healthchecks.
- **D9 — Testes e medição**: fixtures sintéticas, testes de carga/consulta/E2E e
  benchmark com a competência completa.

## Phase 1: Design & Contracts

### Data model

O modelo lógico e o mapeamento físico estão em [data-model.md](./data-model.md). Pontos
centrais:

- **Competência**: `2026-10`, fonte `Receita Federal`; uma linha em `query_metadata`.
- **Empresas**: `cnpj_basico` como chave; razão social normalizada para busca.
- **Estabelecimentos**: CNPJ completo = básico (8) + ordem (4) + DV (2); endereço, UF,
  município, situação, CNAEs, contatos e datas.
- **CNAEs do estabelecimento**: uma linha por código (principal e secundários), com
  delimitador de origem tratado como vírgula ou ponto e vírgula.
- **Sócios**: CPF armazenado somente mascarado; demais campos cadastrais.
- **Simples/MEI**: flags e datas; `optante_simples`/`optante_mei` derivados até o fim da
  competência.
- **Tabelas de apoio**: CNAE, município, natureza jurídica, qualificação, motivo e país.
- **Catálogo de domínio**: situação cadastral, porte e matriz/filial, com os códigos
  oficiais da Receita Federal.
- **Listagem**: tabela materializada com os campos necessários à página, para evitar
  joins repetidos.

### Contracts

- [contracts/query-api.md](./contracts/query-api.md): contrato HTTP das páginas
  (`GET /`, `GET /estabelecimentos`, `GET /estabelecimentos/{cnpj}`, `GET /health`),
  parâmetros, estados, erros e regras de privacidade. Não há API JSON pública no MVP.
- [contracts/data-contracts.md](./contracts/data-contracts.md): contrato dos Parquets de
  origem, transformações de carga, schema de consulta, índices e compatibilidade.
- [contracts/README.md](./contracts/README.md): índice e fontes de verdade.

### Quickstart

[quickstart.md](./quickstart.md) descreve como subir o PostgreSQL, carregar a competência,
iniciar a web e validar os cenários de aceitação, incluindo a suíte de testes e o
benchmark de desempenho.

## Implementation Strategy

### Slice 1 — P1: explorar, filtrar e paginar

1. Criar fixtures sintéticas de Parquet e testes falhando para inventário, normalização,
   carga e consulta.
2. Implementar o schema PostgreSQL e o loader idempotente com staging + swap atômico.
3. Implementar a listagem server-rendered com filtros combináveis, contagem, paginação,
   competência/fonte e estados vazio/erro.
4. Validar CPF mascarado, ausência de exportação/download e preservação de filtros.

### Slice 2 — P2: detalhar o estabelecimento

1. Testes falhando para detalhe, empresa, sócios, Simples/MEI e retorno à listagem.
2. Implementar a página de detalhe e as consultas relacionadas.
3. Garantir que o link de retorno reconstrua a URL com filtros e página.

### Slice 3 — Qualidade e operação

1. E2E com Playwright cobrindo o fluxo principal e os estados de borda.
2. Teste de usabilidade com participantes sem perfil predefinido, medindo a conclusão da
   tarefa principal na primeira tentativa (SC-004).
3. Benchmark com a competência completa; registrar p95, duração de carga e memória.
4. Revisar segurança/privacidade (CPF, logs, papel read-only, exposição local).
5. Atualizar README com comandos, variáveis de ambiente e baseline.

## Dependencies and Justification

| Dependência | Licença | Necessidade | Alternativa avaliada |
|---|---|---|---|
| FastAPI | MIT | Rotas HTTP, validação e injeção de dependências com pouco código | Flask (viável, mas sem tipagem/validação integrada); Django (excessivo para duas páginas) |
| Uvicorn | BSD-3 | Servidor ASGI para FastAPI | Hypercorn (viável, sem ganho para o MVP) |
| Jinja2 | BSD-3 | Renderização server-side das páginas | SPA React/Next (mais peças, sem necessidade no MVP) |
| psycopg 3 | LGPL-3 | Acesso PostgreSQL com pool e parâmetros seguros | asyncpg (viável; menos direto para o pool/uso síncrono); SQLAlchemy (ORM desnecessário) |
| PostgreSQL 16 | PostgreSQL License | Camada relacional/indexada já decidida no projeto | DuckDB direto (não é a decisão registrada); SQLite (não suporta o volume/concorrência alvo) |
| pg_trgm | PostgreSQL contrib | Índice para busca literal por substring | Varredura completa (não atende p95); busca fuzzy (fora de escopo) |
| Playwright | Apache-2.0 | E2E opcional do fluxo principal | Testes apenas de contrato (não cobrem navegação/voltar) |

DuckDB, PyArrow e pytest já fazem parte do projeto. As dependências da web ficam em
`query/requirements.txt` e não entram na imagem do Airflow.

## Risks and Mitigations

| Risco | Impacto | Mitigação |
|---|---|---|
| Bronze incompleto para `2026-10` | Loader não publica a competência | Validar presença e schema dos 37 arquivos; falhar com mensagem explícita; rodar o pipeline corrigido antes da carga |
| Carga longa/uso de disco | Janela de indisponibilidade e espaço no volume | Carga em staging, `COPY`, índices após a carga, métricas de duração/memória; executar fora do horário de uso |
| `COUNT(*)` e offset profundo | Consultas amplas podem passar de 5 s | Tabela de listagem, índices compostos, contagem sem filtros via metadados e benchmark; page size 25 |
| Códigos oficiais sem lookup no Parquet | Rótulos incorretos ou desatualizados | Catálogo versionado com fonte oficial, testes de mapeamento e fallback para o código |
| CPF em texto livre de razão social | Exposição de dado pessoal | Redação defensiva de padrões de CPF na apresentação e nos logs; CPF de sócio mascarado na carga |
| Exposição sem autenticação | Acesso indevido em rede pública | MVP local/interno; não publicar na internet sem proxy/controle de acesso, que está fora do escopo |
| Variações no arquivo da RFB | Falha de parsing ou CNAEs secundários mal separados | Validar schema e formatos; aceitar vírgula e ponto e vírgula; registrar contadores de valores inválidos |

## Out of Scope

- Exportação, download e geração de CSV (melhoria futura).
- API pública para terceiros, autenticação de API e contratos versionados.
- Comparação histórica, seleção de competência e séries temporais.
- Dashboards, gráficos agregados, relatórios e ferramentas de BI.
- Busca tolerante a erros, similaridade e ordenação por relevância.
- IA, recomendações, cobrança, autenticação e perfis.
- Integração com fontes externas, Lucro Real/Presumido/Arbitrado, PGFN, CNO e contatos
  enriquecidos.
- Edição, upload ou correção de dados.

## Future Evolution

O MVP não implementa API pública nem BI, mas a arquitetura deve evitar retrabalho quando
esses consumidores forem priorizados.

### API para terceiros

- A futura API deve ser uma **feature separada**, com contrato versionado (`/api/v1`),
  autenticação de clientes, rate limiting, quotas, CORS, documentação OpenAPI e política
  de depreciação.
- As rotas JSON devem reutilizar `src/query/db/repositories.py`, `domain/` e as mesmas
  validações da interface; a página HTML e a API não devem duplicar SQL ou regras.
- A API deve usar o mesmo papel PostgreSQL somente leitura e o mesmo mascaramento de CPF.
- A exposição pública exige uma decisão de segurança/termos de uso; o MVP permanece
  local/interno.

### Apache Superset e BI

- Superset pode conectar-se diretamente ao PostgreSQL de consulta com um papel
  **somente leitura** e usar as tabelas/views estáveis (`estabelecimentos_listagem`,
  tabelas base e apoio).
- O Superset deve ter seu **próprio banco de metadados** (não reutilizar o banco de
  consulta nem o do Airflow).
- Consultas de BI não devem competir com a web no mesmo banco em produção; a evolução
  prevista é usar uma **réplica de leitura** ou uma base analítica separada para dashboards
  e agregações.
- Dashboards, agregações e permissões do Superset ficam fora do MVP; a camada de consulta
  atual já fornece a base relacional para essa evolução.

## Post-Design Constitution Check

Após o desenho de dados e contratos, os gates permanecem atendidos: a carga é isolada da
web, o schema de origem é validado, o CPF não existe em claro no banco de consulta, a web
usa papel somente leitura e todos os fluxos do MVP têm cenário de teste. Não há violação
que exija registro em Complexity Tracking.

## Next Steps

1. Iniciar a implementação por `tasks.md` (T001–T066), começando pelos testes falhando.
2. Implementar o loader e a listagem (P1), depois os detalhes (P2).
3. Rodar o benchmark com a competência completa e registrar o baseline.
4. Atualizar o README e o `.env.example` com as variáveis do portal.
