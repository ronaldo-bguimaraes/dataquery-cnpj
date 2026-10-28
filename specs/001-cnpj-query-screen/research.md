# Pesquisa Técnica — Explorador Visual de Dados CNPJ

**Feature**: `001-cnpj-query-screen` · **Data**: 2026-10-27
**Contexto**: consulta e filtragem somente leitura dos Parquets Bronze da competência
`2026-10`, com listagem paginada e detalhe de estabelecimento. O MVP não tem exportação,
histórico, autenticação, busca aproximada nem fontes externas.

## 1. Decisões

### D1 — PostgreSQL como camada de consulta

- **Decision**: usar PostgreSQL 16 como banco relacional de consulta local e primeira
  versão, preparado a partir dos Parquets atuais. A aplicação web acessa o banco com um
  papel somente leitura.
- **Rationale**: a decisão já está registrada em `docs/roadmap-dados.md`; os Parquets
  continuam como snapshots da fonte. Filtros combinados, contagem exata, joins com tabelas
  de apoio e paginação são operações naturais em um banco relacional indexado. PostgreSQL
  também permite separar a carga (papel de escrita) da consulta (papel read-only), o que
  reduz o risco de a interface alterar dados. O roadmap menciona a escolha de competência
  como direção futura; o MVP atual tem um único snapshot (`2026-10`) e segue a spec, sem
  seletor de competência nem comparação histórica.
- **Alternatives considered**:
  - **DuckDB lendo Parquet a cada requisição**: simples e sem carga, mas não é a decisão
    registrada; consultas amplas podem varrer os arquivos inteiros e a contagem/paginação
    fica dependente do layout físico atual (sem estatísticas).
  - **SQLite**: suficiente para uma cópia local pequena, mas não é o banco já decidido e
    tem concorrência/gerenciamento menos adequados ao volume e ao acesso web.
  - **Elasticsearch/Meilisearch**: bons para busca textual, mas adicionam um segundo motor
    e não são necessários para filtros exatos e listagem relacional.
  - **MySQL/MariaDB**: viáveis, sem vantagem sobre o PostgreSQL já adotado no projeto.

### D2 — Carga a partir dos Parquets Bronze existentes

- **Decision**: um job de carga lê as famílias de Parquets da competência disponível
  (`Empresas*`, `Estabelecimentos*`, `Socios*`, `Simples`, `Cnaes`, `Motivos`,
  `Municipios`, `Naturezas`, `Paises`, `Qualificacoes`), valida o schema e grava o modelo
  relacional. Nenhuma fonte nova é consultada e o pipeline não é alterado.
- **Rationale**: os Parquets já são o contrato produzido pelo pipeline e a competência
  `2026-10` é o único inventário disponível. A carga permite normalizar tipos, criar
  índices e materializar a listagem sem mudar os arquivos de origem. O job pode ser
  reexecutado de forma idempotente para a mesma competência.
- **Alternatives considered**:
  - **Consultar Parquet diretamente em toda requisição**: elimina a carga, mas mantém o
    custo de parsing/joins e depende de um layout físico que hoje não tem estatísticas.
  - **Criar tabelas derivadas no próprio pipeline**: aumentaria o escopo da feature de
    ingestão e misturaria responsabilidades; a camada de consulta é o lugar natural para
    o modelo de serving.
  - **Carregar histórico completo**: o inventário atual só tem `2026-10`; histórico está
    fora do escopo e seria uma decisão futura.

### D3 — Modelo relacional de serving

- **Decision**: materializar:
  - tabelas base `empresas`, `estabelecimentos`, `socios`, `simples`;
  - `estabelecimento_cnaes` (uma linha por CNAE principal/secundário);
  - tabelas de apoio `cnaes`, `municipios`, `naturezas_juridicas`,
    `qualificacoes_socios`, `motivos`, `paises`;
  - catálogo de domínio `situacoes_cadastrais`, `portes`, `matriz_filial`;
  - tabela denormalizada `estabelecimentos_listagem` para a página principal;
  - `query_metadata` com competência, fonte, data de carga e contagens.
- **Rationale**: a listagem precisa de empresa, situação, UF, município, CNAE, porte,
  matriz/filial, data de abertura e Simples/MEI. Materializar esses campos evita repetir
  joins e cálculos em cada consulta. A tabela de CNAEs resolve o filtro "principal ou
  secundário" com índice, sem `LIKE` sobre uma lista textual. O catálogo de domínio é
  necessário porque situação cadastral, porte e matriz/filial são códigos oficiais sem
  tabela correspondente nos Parquets.
- **Alternatives considered**:
  - **Somente views**: menos armazenamento, mas joins repetidos e maior latência no volume
    alvo.
  - **JSONB com todos os dados**: menos tabelas, porém filtros e índices menos claros e
    sem ganho para um modelo cadastral estável.
  - **Guardar CNAEs secundários como array**: possível no PostgreSQL, mas a tabela
    normalizada é mais simples de indexar e testar.

### D4 — Aplicação web server-rendered com FastAPI + Jinja2

- **Decision**: aplicação Python com FastAPI, Jinja2 e formulários GET. A listagem e o
  detalhe são páginas HTML renderizadas no servidor; filtros e página vivem na URL.
- **Rationale**: o projeto já é Python; não é necessário introduzir Node.js, SPA ou uma
  API pública para duas páginas. Formulários GET preservam o estado na URL, o que atende
  ao retorno dos detalhes para a listagem e simplifica testes de contrato. FastAPI fornece
  validação de parâmetros e TestClient; Jinja2 mantém o HTML próximo do servidor.
- **Alternatives considered**:
  - **Next.js/React**: melhor interatividade, mas adiciona runtime, build e dependências
    sem requisito de SPA no MVP.
  - **Streamlit**: rápido para protótipo, mas controle limitado de rotas, paginação e
    estados de erro; menos adequado ao contrato de páginas.
  - **Django**: completo, porém pesado para duas páginas e um job de carga.
  - **Spring Boot**: mencionado como possível consumidor futuro em `docs/roadmap-dados.md`,
    mas não há código Java no projeto e não é necessário para o MVP.

### D5 — Busca literal por CNPJ ou nome

- **Decision**: CNPJ completo é busca exata, com ou sem máscara. Nome é busca por
  substring sobre uma coluna normalizada (minúsculas, sem acentos), sem correção de erros
  e sem similaridade. `pg_trgm` indexa a busca por substring; não é usada para ranquear
  por proximidade.
- **Rationale**: o escopo removeu busca tolerante a erros. A normalização de caixa/acentos
  melhora a usabilidade sem introduzir fuzzy. A coluna normalizada evita depender de
  `unaccent` em tempo de consulta (a função não é imutável para índice gerado).
- **Alternatives considered**:
  - **`ILIKE` direto na razão social**: funciona, mas sem índice trigram a varredura fica
    caro no volume alvo.
  - **Full-text search**: tokenização/stemming não é o comportamento pedido e pode trazer
    correspondências que não são substring literal.
  - **Similaridade por trigramas**: atenderia typos, mas foi explicitamente removida do
    MVP.

### D6 — Paginação e contagem

- **Decision**: página padrão de 25 registros, ordenação estável por CNPJ completo,
  `LIMIT/OFFSET` e contagem exata. Para a listagem sem filtros, a contagem pode vir de
  `query_metadata`; com filtros, é executada uma consulta de contagem indexada.
- **Rationale**: a interface mostra página atual e total, e a ordenação por chave única
  evita repetição/omissão entre páginas. O offset é simples e suficiente para o uso
  inicial; a primeira página é o alvo de p95.
- **Alternatives considered**:
  - **Keyset/cursor**: melhor para páginas profundas, mas complica a numeração de páginas
    e a contagem exata; pode ser evolução futura.
  - **Scroll infinito**: não atende à indicação de página/total nem à navegação previsível.
  - **Contagem estimada**: mais rápida, mas o requisito pede quantidade total exata.

### D7 — Privacidade e segurança

- **Decision**: o CPF de sócios é mascarado no loader; o banco de consulta não armazena
  CPF em claro. A web usa um papel PostgreSQL somente leitura. Logs registram nomes de
  filtros e métricas, não os valores pesquisados. A apresentação aplica redação defensiva
  a padrões de CPF em texto livre (por exemplo, razões sociais), sem alterar a origem.
- **Rationale**: a constituição trata CPF como dado pessoal sob LGPD. Mascarar na carga
  reduz a superfície de exposição; um papel read-only impede escrita acidental pela
  aplicação; não registrar termos de busca evita que um CPF digitado por engano vá para
  o log.
- **Alternatives considered**:
  - **Mascarar só na interface**: deixaria CPF em claro no banco e em consultas.
  - **Guardar CPF completo e restringir a coluna**: maior risco operacional sem benefício
    para o MVP, que só precisa exibir a máscara.
  - **Autenticação no MVP**: fora de escopo; a exposição deve ficar restrita ao ambiente
    local/interno até uma decisão de deployment.

### D8 — Operação local em Docker Compose

- **Decision**: adicionar `query-db`, `query-loader`, `query-web` e `query-tests` em
  `query/docker-compose.yaml`, incluído pelo Compose raiz. Os serviços do portal ficam no
  profile `query` e os testes no profile `query-test`, para que `docker compose up -d`
  continue subindo apenas a stack do Airflow. O banco usa volume próprio; o loader recebe o
  mount de `/data` e as variáveis de storage já usadas pelo pipeline; a web recebe apenas a
  URL do banco read-only.
- **Rationale**: mantém o portal reproduzível no mesmo fluxo do Airflow, sem depender de
  instalação manual. O banco do portal não compartilha o PostgreSQL de metadados do
  Airflow. O loader e a web têm imagens/entrypoints separados.
- **Alternatives considered**:
  - **Um único container rodando tudo**: mais simples, mas mistura carga, banco e web e
    dificulta o controle de permissões.
  - **Serviços gerenciados (RDS/Neon)**: desnecessário para o MVP local e adiciona custo
    e configuração externa.
  - **MinIO no caminho da web**: a web não precisa acessar o storage; o loader resolve a
    origem.

### D9 — Testes e medição

- **Decision**: TDD com fixtures sintéticas de Parquet, testes unitários das
  transformações, testes de integração com PostgreSQL, testes de contrato das páginas e
  E2E opcional com Playwright. Benchmark separado com a competência completa.
- **Rationale**: a carga tem regras de parsing e privacidade que precisam de testes
  isolados; o banco precisa de testes com dados reais de filtros/paginação; o E2E cobre
  navegação e retorno. O benchmark valida SC-002/SC-003 e registra custo/memória.
- **Alternatives considered**:
  - **Só testes de unidade**: não cobrem SQL, índices e páginas.
  - **Só E2E**: lento e difícil de diagnosticar regras de carga.
  - **Banco real em todos os testes**: mais fiel, porém lento; usar fixtures pequenas na
    integração e a competência completa apenas no benchmark.

### D10 — Evolução futura: API para terceiros e Apache Superset

- **Decision**: manter a API pública e o BI fora do MVP, mas desenhar a camada de consulta
  para ser reutilizada por ambos. A futura API será uma feature separada, servida pela
  mesma aplicação FastAPI ou por um serviço próprio, reutilizando os repositórios e as
  validações. O Superset conectará diretamente ao PostgreSQL com papel somente leitura,
  usando tabelas/views estáveis e seu próprio banco de metadados.
- **Rationale**: o PostgreSQL já é a camada consultável do portal; a separação
  loader → banco → web permite adicionar um adaptador JSON sem reescrever a carga. O
  Superset é uma ferramenta de BI que fala PostgreSQL nativamente e mantém metadados
  próprios; usá-lo diretamente no banco evita criar uma API intermediária só para
  dashboards. Para terceiros, porém, uma API versionada é necessária para controlar
  autenticação, quotas, CORS, formatos e depreciação.
- **Alternatives considered**:
  - **Expor somente a API e fazer o portal consumi-la**: desacoplamento maior, mas duplica
    a camada HTTP e adiciona latência/complexidade ao MVP; pode ser reavaliado quando a API
    existir.
  - **Superset consumindo a API do portal**: permitiria centralizar regras, mas BI precisa
    de SQL/agregações e a API do MVP não tem esse contrato; conexão direta read-only é mais
    simples.
  - **Banco analítico separado desde o MVP**: isola carga, porém adiciona outro sistema sem
    demanda atual; uma réplica de leitura é a evolução natural quando o BI for priorizado.
  - **Expor o PostgreSQL publicamente**: rejeitado; a API deve ser a fronteira pública, com
    autenticação/rate limiting, e o banco deve permanecer privado.

**Consequências para o MVP**

- Não criar rotas `/api` de dados, autenticação de API, CORS ou quotas agora.
- Não colocar SQL diretamente nos templates; concentrar consultas em repositórios
  reutilizáveis.
- Manter nomes de tabelas/colunas e o `query_metadata` estáveis e documentados.
- Não usar o banco de consulta como banco de metadados do Superset.
- Em produção, planejar réplica de leitura/base analítica para BI antes de ativar
  dashboards concorrentes com a web.

## 2. Fatos Verificados

| # | Fato | Fonte |
|---|---|---|
| 1 | PostgreSQL é a camada consultável decidida para o portal; Parquets continuam como snapshots da fonte | `docs/roadmap-dados.md` |
| 2 | O inventário local contém a competência `2026-10` | `docs/roadmap-dados.md`, `data/raw/2026-10/` |
| 3 | O pipeline grava `bronze/<AAAA-MM>/<nome-base>.parquet`; a competência atual tem 37 arquivos: 10 `Empresas*`, 10 `Estabelecimentos*`, 10 `Socios*`, 1 `Simples` e 6 tabelas de apoio | `data/raw/2026-10/`, `src/conversao/bronze.py` |
| 3a | Volume verificado nos metadados Parquet: 70.517.438 empresas, 73.809.379 estabelecimentos, 28.495.961 sócios e 50.806.102 registros de Simples | `data/bronze/2026-10/` (metadados Parquet) |
| 4 | Todos os valores dos Parquets são gravados como texto (`pa.string`); datas, capital e códigos precisam de parsing no loader | `conversor.py`, `utils.py` |
| 5 | O conversor grava com `write_statistics=False`; índices/estatísticas do Parquet não são confiáveis para poda | `conversor.py` |
| 6 | O CNPJ do estabelecimento é básico (8) + ordem (4) + DV (2); o arquivo traz ordem com 4 dígitos e DV com 2 | amostra de `data/raw/2026-10/Estabelecimentos0.zip` |
| 7 | CNAEs secundários vêm em uma string separada por vírgula (ex.: `4781400,5229099,4782201`); o schema documenta `;`, então o loader deve aceitar ambos | amostra de `Estabelecimentos0.zip`, `schema.py` |
| 8 | Datas vêm como `AAAAMMDD`; `00000000` representa ausência | amostra de `Estabelecimentos0.zip`, `Simples.zip` |
| 9 | Capital social vem como texto com vírgula decimal (ex.: `5000,00`) | amostra de `Empresas0.zip` |
| 10 | O CPF de sócio pode vir mascarado na origem; o loader deve aplicar máscara defensiva mesmo assim | amostra de `Socios1.zip` |
| 11 | Porte oficial: `00` não informado, `01` microempresa, `03` empresa de pequeno porte, `05` demais | layout oficial de dados abertos do CNPJ |
| 12 | Situação cadastral oficial: `01` nula, `02` ativa, `03` suspensa, `04` inapta, `05` ativa não regular, `08` baixada | layout oficial de dados abertos do CNPJ |
| 13 | Matriz/filial: `1` matriz, `2` filial | layout oficial de dados abertos do CNPJ |
| 14 | PostgreSQL 16 já é usado no Compose do Airflow; o portal pode usar a mesma versão em serviço separado | `airflow/docker-compose.yaml` |
| 15 | A aplicação não tem autenticação no MVP; a exposição deve ficar restrita ao ambiente local/interno | spec, `docs/roadmap-dados.md` |
| 16 | O volume é da ordem de dezenas de milhões de estabelecimentos; a página precisa de índices e materialização | spec, README |
| 17 | Superset conecta a PostgreSQL com driver SQLAlchemy e pode usar um banco de metadados próprio | [Superset — Connecting to Databases](https://superset.apache.org/user-docs/databases/) |
| 18 | A arquitetura do Superset separa aplicação, banco de metadados, cache e workers; o banco de metadados não é o banco analítico | [Superset — Architecture](https://superset.apache.org/docs/installation/architecture) |
| 19 | Para produção, o Superset recomenda banco de metadados dedicado e suporta réplicas de leitura para consultas analíticas | [Superset — Architecture](https://superset.apache.org/docs/installation/architecture), [Connection pooling](https://www.d23.io/blog/apache-superset-database-connection-pooling-at-scale) |

## 3. Referências

- [Layout oficial dos dados abertos do CNPJ](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf)
- [Porte da empresa — API Consulta CNPJ](https://apicenter.estaleiro.serpro.gov.br/documentacao/consulta-cnpj/pt/porte_empresa/)
- [Tipos de situação cadastral — API Consulta CNPJ](https://apicenter.estaleiro.serpro.gov.br/documentacao/consulta-cnpj/pt/tipos_situacao_cadastral/)
- [Roadmap de dados do projeto](../../docs/roadmap-dados.md)
- [Especificação da feature](./spec.md)
- [Modelo de dados](./data-model.md)
- [Contratos](./contracts/)
