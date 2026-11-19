# Implementation Plan: Prospecção Comercial B2B

**Branch**: `004-prospeccao-comercial-b2b` | **Date**: 2026-11-18 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/004-prospeccao-comercial-b2b/spec.md`

## Summary

Ampliar o portal CNPJ existente com uma camada de prospecção comercial sobre o mesmo
read model `estabelecimentos_listagem`. A feature adiciona ao read model contatos
cadastrais, CEP, natureza jurídica, capital social, segmento comercial, quantidade de
CNAEs e quantidade de estabelecimentos/filiais do CNPJ raiz; adiciona filtros comerciais
(segmento, idade, filiais, presença de contato); publica uma página `/prospeccao` com
agregações por segmento e UF; e permite gerar um CSV filtrado em streaming.

Nada disso cria um segundo pipeline: a carga local (`src/query/ingest/`) e a carga
massiva (`src/ingest_parallel/bulk.py`) continuam sendo as únicas entradas, e ambas
passam a preencher as novas colunas. O catálogo de segmentos é código versionado
(`src/query/domain/segmentos.py`) e é publicado como tabela de apoio na carga, evitando
duplicar a regra em SQL e Python.

## Technical Context

**Language/Version**: Python 3.13+ (venv local 3.15), SQL, HTML/CSS

**Primary Dependencies**: FastAPI, Uvicorn, Jinja2, psycopg 3 (`psycopg[binary,pool]`),
PyArrow, PostgreSQL 16

**Storage**: PostgreSQL (read model `estabelecimentos_listagem`); Parquets Bronze como
fonte imutável da competência `2026-10`

**Testing**: pytest (unitário, integração PostgreSQL, contrato HTTP); Playwright opcional
para E2E

**Target Platform**: Linux + Docker Compose; navegador desktop

**Project Type**: aplicação web + job de carga

**Performance Goals**: listagem filtrada ≤ 5 s (p95); CSV de 1.000 linhas ≤ 10 s; sem
`count(*)` irrestrito sobre 73,8 M (mantém `COUNT_CAP`)

**Constraints**: competência única; somente leitura; sem enriquecimento de contato; sem
inferência de intenção de compra; CPF nunca exportado; CSV limitado a 50.000 linhas por
padrão

**Scale/Scope**: read model com ≈73,8 M estabelecimentos no cenário completo; 20
segmentos; duas agregações (segmento e UF); uma página e um endpoint de exportação

## Constitution Check

| Princípio | Verificação | Resultado |
|---|---|---|
| I. ACID | Carga local e bulk publicam o read model na mesma transação/swap atômico já existente | ✅ |
| II. Idempotência | `ALTER TABLE ... IF NOT EXISTS` + truncate/rebuild; reexecutar a carga reproduz o mesmo read model | ✅ |
| III. Deduplicação e integridade | PK `cnpj_completo`; contagens por CNPJ raiz via agregação; catálogo de segmentos com PK | ✅ |
| IV. Incremental | A feature não altera o protocolo de carga; apenas enriquece o build derivado | ✅ |
| V. Tolerância a falhas | CSV em streaming com cursor nomeado; falha no meio não publica estado parcial | ✅ |
| VI. Lote atômico | `qtd_*` e `segmento` são calculados dentro do build derivado antes do swap | ✅ |
| VII. Execução distribuída | Bulk continua paralelo; o enriquecimento é SQL pós-carga no processo publicador | ✅ |
| VIII. Recursos limitados | Exportação em streaming e com teto; agregações limitadas ao recorte | ✅ |
| IX. Observabilidade | Logs de rota/status/duração, sem filtros/contatos/CPF | ✅ |
| X. Reprodutibilidade | Segmentos versionados em código; idade referenciada ao fim da competência | ✅ |
| Segurança e LGPD | Nenhum CPF no read model de listagem nem no CSV; papel read-only na web | ✅ |
| Escopo v1 | Sem intenção de compra, orçamento, enriquecimento ou histórico | ✅ |

**Resultado**: aprovado sem violações que exijam justificativa.

## Project Structure

### Documentation (this feature)

```text
specs/004-prospeccao-comercial-b2b/
├── spec.md
├── plan.md
├── data-model.md
├── quickstart.md
├── tasks.md
└── checklists/requirements.md
```

### Source Code

```text
src/query/domain/segmentos.py          # catálogo de segmentos por divisão CNAE
src/query/domain/indicadores.py        # idade, faixa etária, presença de contato
src/query/db/schema.sql                # colunas/tabelas/índices de prospecção
src/query/db/repositories.py           # filtros, listagem, agregações e cursor de exportação
src/query/ingest/postgres.py           # carga local preenche o read model ampliado
src/ingest_parallel/bulk.py            # carga massiva preenche o read model ampliado
src/query/web/filters.py               # validação dos filtros de prospecção
src/query/web/presenters.py            # view models, links e CSV
src/query/web/routes.py                # /prospeccao e /prospeccao.csv
src/query/web/templates/prospeccao.html
src/query/web/templates/base.html      # navegação
src/query/web/static/estilo.css        # painéis de agregação
tests/query/unit/test_segmentos.py
tests/query/unit/test_indicadores.py
tests/query/unit/test_filters.py       # casos de prospecção
tests/query/integration/test_prospeccao.py
tests/query/contract/test_prospeccao_page.py
```

## Design decisions

### Segmento como catálogo em código

A divisão CNAE (dois primeiros dígitos) é a chave estável para classificação comercial.
O catálogo vive em `src/query/domain/segmentos.py` e é publicado nas tabelas
`segmentos` e `segmento_divisoes` durante a carga. Assim, filtros, exibição e testes usam
a mesma fonte; CNAE ausente/fora do mapa vira `NAO_CLASSIFICADO`.

### Indicadores materializados no read model

`qtd_cnaes`, `qtd_estabelecimentos`, `qtd_filiais` e `segmento_codigo` são calculados no
build derivado, não em cada requisição. Isso mantém a listagem rápida e permite filtrar
por estrutura. A idade permanece derivada de `data_inicio_atividade` + fim da
competência, para não envelhecer dentro do snapshot.

### Cubo de contagens com segmento

`estabelecimentos_filtros_stats` passa a incluir `segmento_codigo`. Assim, os recortes
mais comuns da prospecção (segmento, UF, porte, situação, matriz/filial, Simples/MEI)
têm contagem e agregações por segmento/UF lidas de um cubo pequeno, em vez de varrer
73,8 M de linhas. Recortes com idade, filiais, contato, município, CNAE ou texto usam o
caminho de varredura indexada e contagem limitada (`COUNT_CAP`). Se o cubo estiver vazio
porque o schema foi migrado sem recarga, a contagem cai para o caminho seguro.

### Exportação em streaming

O endpoint usa cursor nomeado do PostgreSQL e escreve o CSV linha a linha. O teto padrão
é `CNPJ_QUERY_EXPORT_LIMIT=50000`, configurável entre 1 e 100.000. Campos são escapados e
neutralizados contra injeção de fórmula.

## Complexity Tracking

Nenhuma violação de constituição. A única complexidade nova é o cálculo de agregados por
CNPJ raiz no build derivado, justificado pelos requisitos FR-007 e FR-013.
