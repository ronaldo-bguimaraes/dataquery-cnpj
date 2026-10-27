# Implementation Plan: Tela de Consulta de Dados CNPJ

**Branch**: `001-cnpj-query-screen` | **Date**: 2026-10-22 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/speckit.specify` (spec `001-cnpj-query-screen`)

## Summary

Uma tela web para consultar os dados de CNPJ já ingeridos pelo pipeline: ficha completa da
empresa por CNPJ (P1) e busca por filtros com tolerância a erro de digitação (P2).

**Abordagem técnica**: Next.js (App Router) **auto-hospedado em Node.js** na VM, com
route handlers em TypeScript usando `@duckdb/node-api` (bindings nativos) para consultar o
Parquet diretamente no **Cloudflare R2** via `httpfs`. O pipeline Python continua produzindo
os Parquet canônicos e os artefatos derivados de busca. O acesso fica restrito à rede interna
via **Cloudflare Tunnel/Zero Trust** (sem autenticação na aplicação, conforme FR-018).

**Por que unificar**: `@duckdb/node-api` precisa de módulos nativos → runtime Node.js. Route
handlers com `export const runtime = 'nodejs'` não rodam em Cloudflare Workers (Edge). Com
DuckDB nativo temos multi-thread, httpfs completo e memória suficiente para o índice de
trigramas (fuzzy), sem serviço Python separado. Custo: **~€6/mês** (Hetzner CX23) ou
**US$0** (Oracle Always Free).

## Technical Context

**Language/Version**: Python 3.12 (pipeline) · TypeScript 5.x / Next.js 15+ (aplicação + API)

**Primary Dependencies**:
- Pipeline: `pyarrow`, `boto3`, `duckdb`, `psutil`
- Aplicação web: `next`, `react`, `@duckdb/node-api`, `zod` (validação de query), `date-fns`
- Infra: `pm2` ou `systemd` para rodar o Next.js; **Cloudflare Tunnel** para acesso interno
- Testes: `pytest` (pipeline), `vitest` + `@testing-library/react` + `msw` (web + API routes)

**Storage**: Cloudflare R2 (Parquet canônico + artefatos derivados, API S3) · MinIO local em
`docker compose` (desenvolvimento) · cache local do DuckDB na VM

**Testing**: pytest (pipeline), vitest (componentes e route handlers), testes de contrato para
as rotas de API

**Target Platform**: Linux (VM) + navegador desktop; **sem Cloudflare Workers**

**Project Type**: web app full-stack (Next.js App Router) + pipeline de dados Python

**Performance Goals**: ficha por CNPJ ≤ 3 s (p95) · busca filtrada ≤ 5 s (p95) · tempo até
resposta ≤ 60 s · fuzzy com empresa correta no top 10 em ≥ 90% dos casos com typo

**Constraints**: sem autenticação na aplicação (rede interna via Tunnel) · VM 4 GB RAM / 2 vCPU
mínimo recomendado · R2 egress grátis · custo ≤ €10/mês · CPF mascarado nas respostas ·
sem PII em log · `@duckdb/node-api` só funciona com `runtime = 'nodejs'` (não Edge)

**Scale/Scope**: 26.228.745 empresas · ~30 M estabelecimentos · ~10 M sócios · 2 telas +
API routes internas · time interno

## Constitution Check

| Princípio / Restrição | Verificação | Resultado |
|---|---|---|
| I. Estágios Desacoplados e Independentes | Pipeline → R2 → Next.js (API + UI) são artefatos separados; rotas da API são a fronteira interna | ✅ PASS |
| II. Contrato de Dados Explícito e Schema-First | `schema.py` fonte única; schemas derivados registrados em `schema.py`; contrato REST mantido em `contracts/` | ✅ PASS |
| III. Streaming e Uso Mensurável de Recursos | Conversão streaming; DuckDB lê row groups sob demanda; indexação de trigramas streaming por chunks | ✅ PASS |
| IV. Falhas Visíveis e Execução Observável | Rotas da API devolvem erros tipados; corrigir `except:` genérico em `download_files.py`; logs sem PII | ✅ PASS |
| V. Testes Primeiro (NON-NEGOTIABLE) | Testes de contrato para rotas de API, unitários para normalização/dice, e2e com amostra | ✅ PASS |
| VI. Simplicidade e Dependências Disciplinadas | **Unificado**: 1 app em vez de app + serviço separado. `pyspark` deve ser removido | ✅ PASS |
| Segurança e LGPD | CPF mascarado nas rotas da API; R2 privado; acesso via Cloudflare Tunnel/Zero Trust | ✅ PASS |
| Infra via `docker compose` | MinIO para dev; Next.js roda via `npm`/`pm2` em dev/prod; Tunnel configurado | ✅ PASS |
| Storage só pela API S3 | boto3 no pipeline; DuckDB `httpfs` usa API S3 do R2/MinIO | ✅ PASS |
| Baseline de performance | Registrar memória/tempo do novo estágio | ✅ PASS |
| Escopo v1 | Exportação e painel fora (FR-019) | ✅ PASS |

**Resultado**: aprovado.

## Project Structure

### Documentação
```text
specs/001-cnpj-query-screen/
├── plan.md
├── spec.md
├── research.md
├── data-model.md
├── quickstart.md
├── checklists/requirements.md
└── contracts/
    ├── README.md
    ├── query-api.md
    └── data-contracts.md
```

### Código-fonte
```text
# Pipeline (Python) — inalterado na arquitetura, estendido
schema.py
conversor.py                # ganha write_statistics=True, partição/ordenação
read_files.py
src/
├── extracao_rfb/
├── download_utils/         # corrigir except: genérico
├── storage/minio.py
└── indexacao/              # normalizar.py, construir_indice.py (trigramas)

# Aplicação Next.js (TypeScript) — UI + API unificadas
web/
├── next.config.ts          # serverExternalPackages: ['@duckdb/node-api']
├── package.json
├── app/
│   ├── layout.tsx
│   ├── page.tsx                    # busca por CNPJ + filtros
│   ├── empresa/[cnpj]/page.tsx     # ficha
│   └── api/
│       ├── empresas/[cnpj]/route.ts  # GET ficha
│       ├── empresas/route.ts        # GET listagem filtrada
│       ├── busca/razao-social/route.ts # GET fuzzy
│       ├── competencias/route.ts
│       ├── lookups/[recurso]/route.ts
│       └── saude/route.ts
├── components/
├── lib/
│   ├── duckdb.ts           # conexão singleton com @duckdb/node-api
│   ├── consultas.ts        # SQL parametrizado (ts)
│   ├── normalizar.ts       # normalização + trigramas (espelha pipeline)
│   ├── similaridade.ts     # dice
│   ├── mascarar.ts         # máscara de CPF
│   └── tipos.ts
└── testes/

tests/                       # testes Python do pipeline
compose.yaml                 # MinIO para dev local
.env.example
```

**Nota sobre httpfs**: os binários oficiais do `@duckdb/node-api` geralmente trazem `httpfs` estático. Caso não traga, precisaremos instalar/carregar a extensão — a confirmar na POC (item em research.md D9).

## Complexity Tracking

| Violation | Por que é Necessário | Alternativa Mais Simples Rejeitada Porque |
|-----------|---------------------|-------------------------------------------|
| Índice de trigramas duplicando parte do dado | FR-012 exige tolerância a erro; `fts` do DuckDB não tem tokenizer trigram e não garante similaridade; preciso de postings + dice | `ILIKE`/prefixo = full scan (811 MB) e não atende SC-007. FTS5 do SQLite adicionaria outro engine e cópia de dados |

A arquitetura ficou **mais simples** que o plano anterior: removemos o serviço FastAPI/Python separado e o deploy em Workers/vinext. Menos peças operacionais.

## Referências

- Spec: [spec.md](./spec.md)
- Pesquisa: [research.md](./research.md) (D1–D9)
- Modelo: [data-model.md](./data-model.md)
- Contratos: [contracts/](./contracts/)
- Validação: [quickstart.md](./quickstart.md)
- Constituição: `.specify/memory/constitution.md` v1.0.0
