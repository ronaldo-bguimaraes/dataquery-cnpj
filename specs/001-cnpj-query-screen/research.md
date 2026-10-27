# Pesquisa Técnica — Camada de Consulta de CNPJ sobre Parquet no Cloudflare R2

**Contexto:** 26.228.745 empresas (811 MB de Parquet no README) + estabelecimentos, sócios e
simples. Spec `001-cnpj-query-screen` (P1 ficha por CNPJ, P2 busca filtrada e fuzzy).
**Data:** 2026-10-22 · Docs consultadas: DuckDB 2.0, Cloudflare (Workers/R2/D1/Basin SQL),
SQLite 3, Hetzner, Oracle Cloud, Next.js/OpenNext/vinext.
**Restrição do usuário:** Next.js no front, dados no R2, Workers ou "outra tecnologia barata".

---

## 1. Decisões

### D1 — Armazenamento canônico: Cloudflare R2 com Parquet; MinIO local só para desenvolvimento

- **Decision:** O bucket R2 guarda o Parquet canônico, gravado pelo pipeline via API S3 (boto3).
  O MinIO continua em `docker compose` como espelho de desenvolvimento.
- **Rationale:** R2 fala a API S3, então `src/storage/minio.py` (boto3) funciona sem mudança de
  código — é exatamente o que a constituição exige. Egress é **grátis**, o free tier de 10 GB
  cobre 811 MB, e a operação é US$0/mês. A alternativa "Parquet numa VM" obrigaria a pagar
  banda de saída para servir a tela.
- **Alternatives considered:** Hetzner Spaces (egress cobrado a partir de 10 TB, preço de
  storage maior); S3 (egress ~US$0,09/GB, mais caro); Glacier (latência impraticável);
  continuar só com MinIO (exige banda da VM e tira o custo marginal zero da arquitetura).

### D2 — Layout dos Parquet: statistics habilitadas, particionamento por UF, ordenação por CNPJ

- **Decision:** `conversor.py` passa a gravar com `write_statistics=True` (hoje está
  `False`), particiona por `uf` (coluna **derivada** do CNPJ para `empresas`/`socios`, coluna
  real em `estabelecimentos`) e ordena cada partição por `cnpj_basico`. Row groups de 100 mil a
  1 milhão de linhas, compressão ZSTD.
- **Rationale:** Sem *statistics* não existe zonemap e **toda consulta vira varredura completa
  dos 811 MB** — é o item de maior impacto de todo o plano. Com statistics + ordenação, um
  CNPJ cai em pouquíssimos row groups. O dígito 9 do CNPJ (ramo) identifica a UF
  (0=SP, 1=RS, 2=MG, 3=ES, 4=BA, 5=PR, 6=SC, 7=PE, 8=CO, 9=DF), então a consulta por CNPJ
  deriva a UF e abre **uma** partição só. Filtro por UF abre uma partição; município, CNAE e
  porte usam os statistics de min/max dentro dela.
- **Alternatives considered:** Arquivo único ordenado por `cnpj_basico` (lookup perfeito, mas
  filtro por UF perde a poda); dois recortes do mesmo dado (um por CNPJ, um por UF — 2× de
  armazenamento, R$ de custo desprezível, mas duplica a superfície de manutenção); particionar
  por município (explosão de arquivos, ~5570 diretórios). Particionar por UF gera ~27 arquivos
  de ~30 MB, abaixo da faixa de 100 MB–10 GB recomendada — aceito porque o gargalo de leitura
  remota é o *row group*, não o arquivo, e 30 MB ainda permite leitura de poucos row groups.

### D3 — Motor de consulta: DuckDB nativo em host sempre ligado, lendo o R2 pela API S3

- **Decision:** Serviço Python com DuckDB em uma VM barata (Hetzner CX23, €5,49/mês) ou
  Oracle Always Free (US$0), lendo o Parquet do R2 via `httpfs` com segredo `TYPE r2`.
- **Rationale:** O DuckDB fala a API S3, então o **mesmo SQL roda contra MinIO local e R2 de
  produção** — conformidade direta com a constituição. É multi-thread, vetorizado e faz
  predicate pushdown sobre Parquet remoto. O custo é ~€5/mês contra uma base de 26 M linhas.
- **Alternatives considered:** **DuckDB-WASM em Worker** — 128 MB de RAM, single-thread,
  sem extensão dinâmica: serve ao caminho exato por CNPJ, mas **não** atende FR-012 (fuzzy)
  nem SC-003 (5 s). **Basin SQL** (sucessor do R2 SQL, GA em 01/10/2026) — exige tabelas
  **Iceberg** no Basin Catalog, não lê Parquet puro; sem full-text/trigram; US$0,0025/GB
  escaneado; sem números públicos de timeout e concorrência. **Cloudflare D1** — teto rígido
  de 10 GB por database contra 26 M linhas: inviável. **Meilisearch/Typesense/Elastic** —
  a partir de US$20/mês, desproporcional para o time interno. **ClickHouse Cloud** — US$300
  de crédito e complexidade desnecessária.

### D4 — Busca fuzzy: índice de trigramas derivado, construído pelo pipeline

- **Decision:** O pipeline produz artefatos derivados — `busca_documentos` (uma linha por
  empresa: CNPJ, razão social normalizada, lista de trigramas, colunas de filtro) e
  `busca_postings` (CNPJ → trigrama). A consulta extrai os trigramas do termo, escolhe os
  **mais raros** (menor frequência no corpus) para gerar candidatos e ranqueia por
  **similaridade de dice** sobre o conjunto de trigramas.
- **Rationale:** FR-012 exige tolerância a erro de digitação e nenhuma alternativa barata
  entrega isso nativamente. O `fts` do DuckDB é BM25 com stemmer (tem `portuguese`, mas
  **não tem tokenizer trigram**) e não casa "PETRBRAS" com "PETROBRAS". O FTS5 do SQLite com
  `tokenize='trigram'` casa **substring**, não similaridade — sozinho também não tolera o typo.
  A combinação postings + dice é o caminho clássico de recuperador tolerante a erro, e roda
  inteiramente no DuckDB (um só motor).
- **Alternatives considered:** Levenshtein sobre os candidatos (custo O(n·m) por candidato);
  embeddings + Vectorize (custo de embedding de 26 M documentos e latência de rede — caro
  demais para o budget); trigram no SQLite FTS5 (dois motores, cópia extra dos dados);
  busca por prefixo apenas (não atende SC-007).

### D5 — Camada web: Next.js em Cloudflare Workers

- **Decision:** Next.js (App Router) rodando em Workers, deploying com `vinext` (caminho
  recomendado pela Cloudflare para app novo) e OpenNext como plano B; deploy via Wrangler.
- **Rationale:** Atende à direção do usuário e resolve a camada de interface: SSR, route
  handlers, assets estáticos e cache de borda sem manter servidor de aplicação. Custo
  Workers Paid = US$5/mês (o free tier, com 10 ms de CPU, fica abaixo do consumo típico de
  SSR de 10–20 ms). O plano B é rodar o Next.js na própria VM — mais simples — caso o
  runtime de Workers bloqueie alguma funcionalidade necessária.
- **Alternatives considered:** `next-on-pages` (arquivado — não iniciar); Cloudflare Pages
  (a própria documentação alerta contra uso em app novo); Vercel Hobby (proíbe uso comercial);
  auto-hospedar Next.js na VM (mais simples e com menos peças, mas perde cache de borda e
  obriga a gerir versão de Node na VM); Railway/Fly (free tier insuficiente para o Parquet).

### D6 — API de consulta: serviço Python (FastAPI) com DuckDB, contrato REST versionado

- **Decision:** O Worker chama o serviço de consulta por HTTPS (server-side, no route handler
  do Next.js) — o browser **nunca** fala direto com a API de dados. O CPF é mascarado na
  fronteira da API, não na interface.
- **Rationale:** Mantém o contrato de dados (constituição, Princípio II) num único lugar,
  permite auditar e versionar a API, e evita expor a porta do DuckDB. Mascarar no servidor
  garante que nenhum cliente — nem um `curl` acidental — receba CPF em claro.
- **Alternatives considered:** Query direto no route handler do Next.js (só possível se a UI
  estiver auto-hospedada na VM; acopla a camada web ao motor); expor o DuckDB para o browser
  via WASM (rejeitado em D3).

### D7 — Pipeline: escrita no R2 e produção dos artefatos derivados

- **Decision:** O pipeline existente (`conversor.py`, `read_files.py`) passa a gravar no R2
  pela API S3 e ganha um estágio novo que produz as tabelas derivadas de busca. Nenhuma
  mudança no formato canônico: Parquet continua o formato de verdade.
- **Rationale:** O Parquet é portátil (DuckDB, Spark, Trino, Iceberg) e o R2 não amarra o
  projeto a um motor. O custo é de engenharia, não de infra.
- **Alternatives considered:** Escrever Iceberg para usar Basin SQL (implica catálogo,
  snapshots e compactação —运维 extra para um ganho que a tela não precisa).

### D8 — Custo e exposição

- **Decision:** Workers Paid US$5/mês + VM €5,49/mês (ou Oracle Always Free US$0) + R2
  ~US$0/mês = **~US$11/mês**. A aplicação **não** fica pública: acesso pela rede interna via
  Cloudflare Tunnel/Zero Trust (gratuito) ou VPN, sem autenticação na aplicação (FR-018).
- **Rationale:** O orçamento do usuário era "barato"; esse total fica abaixo de um plano de
  VPS único equivalente quando se soma o custo de operar o app web na Cloudflare, e a camada
  de dados fica em hardware que se pode substituir sem tocar na UI.
- **Alternatives considered:** Tudo numa VM (mais simples, ~€6/mês, sem Workers);
  Cloudflare Containers para rodar o DuckDB (incluso no Paid, mas 25 GiB-h de memória
  cobrem pouco para 24/7 e o custo real não foi verificado).

---

## 2. Fatos Verificados

| # | Fato | Fonte | Data |
|---|---|---|---|
| 1 | R2: US$0,015/GB-mês, Classe A US$4,50/M, Classe B US$0,36/M, **egress grátis**; free tier 10 GB + 1 M A + 10 M B | developers.cloudflare.com/r2/pricing | 01/10/2026 |
| 2 | R2 range-read = 1 `GetObject` = 1 operação Classe B; Parquet não é cacheado por default | developers.cloudflare.com/r2/api/s3/api | 14/10/2026 |
| 3 | Workers: Free 100 K req/dia, 10 ms CPU, 128 MB, 50 subrequests; Paid US$5/mês, 30 s CPU (máx. 5 min), 128 MB, 10 K subrequests, bundle 64 MiB; SSR consome 10–20 ms de CPU | developers.cloudflare.com/workers/platform/{limits,pricing} | 12/10/2026 e 02/10/2026 |
| 4 | Cloudflare recomenda **vinext** (beta) para Next.js novo; OpenNext só para apps existentes; `next-on-pages` arquivado | developers.cloudflare.com/workers/framework-guides/web-apps/nextjs | 25/08/2026 |
| 5 | OpenNext exige `nodejs_compat` e `compatibility_date` ≥ 2024-09-23; middleware Node.js não suportado | opennext.js.org/cloudflare | 12/10/2026 |
| 6 | Basin SQL (ex-R2 SQL) é GA desde 01/10/2026, mas **só consulta tabelas Iceberg** do Basin Catalog; sem `read_parquet`; `LIMIT` padrão 500, máx. 10.000; sem full-text/trigram/UDF | developers.cloudflare.com/basin-sql/* | 01–21/10/2026 |
| 7 | Basin SQL cobra US$0,0025/GB escaneado (10 GB/mês incluídos, mínimo 10 MB por query) | developers.cloudflare.com/basin-sql/platform/pricing | 01/10/2026 |
| 8 | D1: 10 GB por database (Paid) / 500 MB (Free); 1 DO por DB (serializa) | developers.cloudflare.com/d1/platform/limits | 09/2026 |
| 9 | DuckDB lê R2 via `httpfs` com `CREATE SECRET (TYPE r2, ...)` e `read_parquet('r2://bucket/file')`; **não existe** extensão `duckdb-r2` | duckdb.org/docs/2.0/guides/network_cloud_storage/cloudflare_r2_import.html | docs 2.0 |
| 10 | Footer do Parquet vem por HTTP range request; só os row groups necessários são baixados; a poda depende das *statistics* | duckdb.org/docs/2.0/core_extensions/httpfs/overview.html | docs 2.0 |
| 11 | Diretrizes de performance: 100 mil–1 M linhas por row group, arquivos de 100 MB–10 GB, row groups < 5.000 linhas são 5–10× mais lentos; particionamento Hive é útil em remoto | duckdb.org/docs/2.0/guides/performance/file_formats.html | docs 2.0 |
| 12 | Caso real de degradação: `SELECT *` em Parquet de 17,4 M linhas × 282 colunas → ~150.000 requisições e ~19 min | github.com/duckdb/duckdb-httpfs#172 | — |
| 13 | `httpfs` nativo não é compilado no WASM; wasm-wasm: `builtin_httpfs=true` baixa o arquivo **inteiro**; memória WASM 4 GB, single-thread | duckdb.org/docs/2.0/clients/wasm/* · duckdb-wasm#2153 | — |
| 14 | DuckDB `fts`: `PRAGMA create_fts_index` + `match_bm25`; stemmers incluem `portuguese`; **tokenizers são apenas `regex` e `opensearch_standard`** — não há trigram | duckdb.org/docs/2.0/core_extensions/full_text_search.html · github.com/duckdb/duckdb-fts | — |
| 15 | SQLite FTS5 `trigram` casa **substring**; opções `case_sensitive` e `remove_diacritics`; trechos < 3 caracteres nunca casam; `detail=none` reduz o índice (1.636 → 134 MiB num corpus de e-mails) | sqlite.org/fts5.html | — |
| 16 | Hetzner: a partir de €5,99/mês (cost-optimized) / €6,49 (regular); egress incluído | hetzner.com/cloud | 15/06/2026 |
| 17 | Oracle Always Free: 2 OCPUs / 12 GB RAM, 200 GB de volume; instância ociosa pode ser **destruída** (CPU/rede/mem < 20 % por 7 dias) | docs.oracle.com/en-us/iaas/Content/FreeTier | — |
| 18 | Meilisearch Cloud a partir de US$20/mês (estimador US$23–30); Typesense Cloud por calculadora; Elasticsearch Serverless US$0,14/VCU-h ingest | meilisearch.com/pricing · elastic.co/pricing | — |
| 19 | Motherduck Lite US$0 (10 GB, 10 Pulse CU-h/mês); Neon free 0,5 GB | motherduck.com/pricing · neon.com/pricing | — |
| 20 | `@ducklings/workers` roda DuckDB-WASM em Worker via httpfs sobre `fetch()`; limitado a 128 MB, single-thread, in-memory | tobilg.com/posts/custom-duckdb-wasm-builds-for-cloudflare-workers | 27/01/2026 |
| 21 | **No repositório:** `conversor.py:19` → `write_statistics=False` → Parquet atual **sem zonemap** | `conversor.py` | leitura local |
| 22 | **No repositório:** README → 2.047 MB de CSV → 811 MB de Parquet / 26.228.745 linhas (~32 B/linha) | `README.md` | leitura local |
| 23 | **No repositório:** `schema.py` → empresas (7 colunas, **sem UF**), estabelecimentos (30, com UF), socios (11), simples (7) + 7 lookups | `schema.py` | leitura local |
| 24 | **No repositório:** `read_files.py:39` → um Parquet por ZIP (`{tabela}_{n}.parquet`), sem partição nem ordenação | `read_files.py` | leitura local |

---

## 3. Viabilidade por Cenário

| Cenário | Opção viável | Custo | Latência esperada | Observação |
|---|---|---|---|---|
| P1 — ficha por CNPJ | DuckDB na VM, Parquet ordenado por CNPJ com statistics | €5,49 + ~US$0 | 0,1–2 s (estimativa) | Depende de D2; hoje sem statistics a busca seria full scan |
| P1 — via Workers | DuckDB-WASM no Worker | US$5/mês | 100 ms–s | 128 MB; possível, mas frágil e não escala para P2 |
| P2 — listagem filtrada | DuckDB + partição por UF + statistics | idem P1 | 1–10 s (estimativa) | município/CNAE/porte usam min/max do row group |
| P2 — fuzzy por razão social | Índice de trigramas derivado + dice | +1–3 GB de disco na VM | 50–500 ms (estimativa) | Depende de D4; sem índice, `ILIKE` vira full scan |
| P3 — agregados/painel | fora do escopo da v1 | — | — | Excluído pela spec |

---

## 4. Custos Mensais Comparados

| Opção | Custo mensal | Ops | Veredicto para 26 M linhas |
|---|---|---|---|
| **Workers (vinext) + R2 + DuckDB na VM** | ~US$11 | Média | **Escolhida** (D1–D8): UI na borda, dados em hardware barato |
| Workers + R2 + DuckDB-WASM no Worker | US$5 | Alta (CORS, SigV4, 128 MB) | Serve a P1, não a P2 |
| Next.js auto-hospedado na VM + DuckDB + R2 | ~€6 | Baixa | Mais simples; perde a borda; plano B registrado em D5 |
| Basin SQL (R2) | ~US$0,0025/query | Média | Exige converter o pipeline para Iceberg; sem fuzzy; imaturo |
| Cloudflare Containers + DuckDB | US$5 + variável | Média | 25 GiB-h inclusos não cobrem 24/7; custo real não verificado |
| D1 + Next.js | ≥ US$5 | Baixa | **Inviável**: teto de 10 GB por DB |
| Meilisearch / Typesense / Elastic | ≥ US$20 | Média | Desproporcional para uso interno |

---

## 5. Riscos Registrados

1. **`vinext` está em beta** — risco de API instável. Mitigação: `npx vinext check` antes de
   comprometer e OpenNext documentado como plano B.
2. **Sem autenticação (FR-018) + Workers** — publicar em `workers.dev` exporia o dataset
   publicamente. Mitigação: acesso pela rede interna (Cloudflare Tunnel/Zero Trust ou VPN).
3. **Full scan silencioso** — sem statistics, qualquer consulta custa 811 MB de leitura
   (Classe B) e alguns segundos. Mitigação: D2 é pré-requisito, e há teste de performance.
4. **Custo de memória da VM** — o índice de trigramas e o cache local do DuckDB competem pela
   RAM. Mitigação: CX23 tem 4 GB; se faltar, subir para CX33 (€8,49) ou usar Oracle
   Always Free (12 GB).
5. **Oracle Always Free pode recolher a instância ociosa** — a máquina precisa ter uso
   periódico acima de 20 % de CPU/rede/memória por 7 dias.
6. **Ganho de 2× no armazenamento** se o layout final adopter dois recortes — mitigado por
   R2 a US$0,015/GB, mas aumenta o tempo de ingestão.
7. **Qualidade da busca fuzzy é uma estimativa** — SC-007 exige 90 % de acerto no top 10; a
   validação é o primeiro item do quickstart.

---

## 6. Não Verificado

- Latências reais de DuckDB sobre R2 com este corpus: **não há benchmark público**; os
  números da §3 são estimativas de engenharia.
- Tamanho e tempo de build do índice de trigramas com 26 M documentos: **estimado** por
  extrapolação linear; a doc do FTS5 só dá a razão sobre um corpus de e-mails.
- Custo real de Cloudflare Containers em regime 24/7.
- Custo/tempo da conversão Parquet → Iceberg exigida pelo Basin SQL.
- Se 10 ms de CPU do plano Free cobrem este app (depende do bundle final; medir com
  `wrangler deploy --dry-run`).
- Limites de timeout e concorrência do Basin SQL: **não publicados** pela Cloudflare.
- SigV4 do R2 no browser não é documentado (o httpfs WASM suporta apenas bearer token).

---

## 7. Fontes

1. https://developers.cloudflare.com/r2/pricing/
2. https://developers.cloudflare.com/r2/api/s3/api/
3. https://developers.cloudflare.com/r2/platform/limits/
4. https://developers.cloudflare.com/workers/platform/limits/
5. https://developers.cloudflare.com/workers/platform/pricing/
6. https://developers.cloudflare.com/workers/framework-guides/web-apps/nextjs/
7. https://developers.cloudflare.com/workers/framework-guides/web-apps/opennext/
8. https://developers.cloudflare.com/pages/
9. https://opennext.js.org/cloudflare
10. https://developers.cloudflare.com/basin-sql/index/
11. https://developers.cloudflare.com/basin-sql/query-data/
12. https://developers.cloudflare.com/basin-sql/reference/limitations-best-practices/
13. https://developers.cloudflare.com/basin-sql/troubleshooting/
14. https://developers.cloudflare.com/basin-sql/platform/pricing/
15. https://developers.cloudflare.com/basin-catalog/platform/pricing/
16. https://developers.cloudflare.com/d1/platform/limits/
17. https://developers.cloudflare.com/d1/platform/pricing/
18. https://developers.cloudflare.com/r2/buckets/public-buckets/
19. https://developers.cloudflare.com/r2/buckets/cors/
20. https://duckdb.org/docs/2.0/core_extensions/httpfs/overview.html
21. https://duckdb.org/docs/2.0/guides/network_cloud_storage/cloudflare_r2_import.html
22. https://duckdb.org/docs/2.0/data/parquet/overview.html
23. https://duckdb.org/docs/2.0/data/partitioning/hive_partitioning.html
24. https://duckdb.org/docs/2.0/guides/performance/file_formats.html
25. https://duckdb.org/docs/2.0/core_extensions/full_text_search.html
26. https://duckdb.org/docs/2.0/clients/wasm/overview.html
27. https://duckdb.org/docs/2.0/clients/wasm/troubleshoot.html
28. https://duckdb.org/docs/2.0/core_extensions/quack.html
29. https://github.com/duckdb/duckdb-httpfs/issues/172
30. https://github.com/duckdb/duckdb-wasm/issues/2153
31. https://github.com/duckdb/duckdb-wasm/issues/1967
32. https://github.com/duckdb/duckdb-wasm/issues/1932
33. https://github.com/duckdb/duckdb-wasm/issues/2228
34. https://github.com/duckdb/duckdb-fts
35. https://sqlite.org/fts5.html
36. https://www.hetzner.com/cloud/
37. https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier.htm
38. https://motherduck.com/pricing
39. https://neon.com/pricing
40. https://clickhouse.com/pricing
41. https://www.elastic.co/pricing/serverless-search
42. https://www.meilisearch.com/pricing
43. https://www.meilisearch.com/docs/resources/help/known_limitations
44. https://typesense.org/pricing
45. https://typesense.org/docs/guide/organizing-collections.html
46. https://railway.com/pricing
47. https://fly.io/pricing-update/
48. https://vercel.com/docs/plans/hobby
49. https://tobilg.com/posts/custom-duckdb-wasm-builds-for-cloudflare-workers

### D9 — Camada web unificada: Next.js auto-hospedado (Node) + DuckDB nativo em TypeScript

- **Decision:** UI e API de consulta ficam juntas no Next.js (App Router), em Node.js (`export const runtime = 'nodejs'`), com `@duckdb/node-api`. O DuckDB lê direto do R2 via `httpfs` com segredo `TYPE r2`. Acesso restrito à rede interna via Cloudflare Tunnel/Zero Trust.
- **Rationale:** Atende à sugestão ("usar a API do Next.js mesmo, com TypeScript"). Bindings nativos exigem Node runtime, não Edge. Ganha-se multi-thread, httpfs completo, capacidade para fuzzy (trigramas), removemos o serviço Python separado e passamos a ter TS + Python no pipeline (menos peças). R2 e Tunnel continuam do Cloudflare.
- **Alternatives considered:** Workers + API Python (B) — mais artefatos, risco do vinext. Workers + DuckDB-WASM (C) — serverless, mas 128MB, single-thread e fuzzy frágil. Httpfs no Node bindings: os binários oficiais geralmente trazem httpfs estático (a confirmar em POC).

## 6. Não Verificado

- Se `@duckdb/node-api` traz `httpfs` estático (ou se precisa `INSTALL httpfs` em runtime)
- Latências reais com este corpus em Node + R2
- Tamanho/tempo de build do índice de trigramas com 26M documentos
- Custo real de Cloudflare Containers 24/7
- Conversão Parquet → Iceberg para Basin SQL
- Limites/timeout/concorrência do Basin SQL
- SigV4 direto do browser para R2

## 7. Fontes

50. https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/
51. https://developers.cloudflare.com/cloudflare-one/policies/access/
52. https://www.npmjs.com/package/@duckdb/node-api
53. https://github.com/duckdb/duckdb-node-neo
54. https://leonardovida.github.io/drizzle-duckdb/integrations/nextjs.html
