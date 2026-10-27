# Quickstart — Validação da Tela de Consulta de Dados CNPJ

**Feature**: `001-cnpj-query-screen` · **Data**: 2026-10-22
**Objetivo**: guia executável para provar que a feature funciona ponta a ponta, requisito por
requisito. Cada cenário cita o requisito da [spec](./spec.md) que valida.
**Não contém**: código de implementação, migrações ou suítes de teste completas — essas coisas
pertencem a [tasks.md](./tasks.md) e à fase de implementação.

---

## 1. Pré-requisitos

| Item | Versão mínima | Como verificar |
|---|---|---|
| Python | 3.12 | `python --version` |
| Node.js | 20 LTS | `node --version` |
| Docker + Compose v2 | — | `docker compose version` |
| Conta Cloudflare (R2 e Workers) | — | bucket criado, token com permissão de leitura e escrita |
| `wrangler` | — | `npx wrangler --version` |

```shell
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Subir o ambiente local

```shell
cp .env.example .env      # preencha as credenciais do MinIO local
docker compose up -d      # MinIO (dev) + serviço de consulta
docker compose ps         # ambos em estado healthy
```

O MinIO local fala a mesma API S3 do R2, então **o mesmo SQL e o mesmo código** servem para
desenvolvimento e produção — é o que garante a conformidade com a restrição de storage da
constituição.

## 3. Obter uma amostra dos dados

```shell
python -m src.indexacao.construir_indice \
  --origem .tmp/2026-02 \
  --destino .tmp/target \
  --competencia 2026-02 \
  --limite-linhas 50000
```

Saída esperada: arquivos `empresas/part-00000.parquet` e `busca_documentos/part-00000.parquet`
em `.tmp/target`, com logs informando linha por tabela, duração e contagem de linhas.

## 4. Verificar o layout físico (D2 — pré-requisito de tudo)

```shell
python -m src.indexacao.verificar_layout --destino .tmp/target
```

| Verificação | Resultado esperado |
|---|---|
| `write_statistics` ativo | **true** em todos os arquivos (hoje o conversor grava `False`) |
| Row groups por arquivo | entre 100 mil e 1 milhão de linhas por row group |
| Ordenação interna | CNPJ em ordem crescente dentro de cada partição |
| Partições | uma por UF, sem partição vazia |

> Sem statistics, toda consulta vira varredura de 811 MB. **Este passo bloqueia os cenários 5
> e 6** — se falhar, o desempenho medido não significa nada.

## 5. Validar a API de consulta

```shell
curl -s localhost:8000/v1/saude | jq .
```

Esperado: `{"dados":{"status":"ok","competencia":"2026-02"}}`

### 5.1 Ficha por CNPJ (P1, FR-001 a FR-007)

```shell
curl -s localhost:8000/v1/empresas/00038174000143 | jq '.dados.empresa.razao_social'
```

| Cenário | Entrada | Esperado |
|---|---|---|
| CNPJ válido existente | `00038174000143` | 200 com `empresa`, `estabelecimentos`, `socios`, `regime_tributario` |
| CNPJ com máscara | `00.038.174/0001-43` | mesma resposta da entrada sem máscara |
| CNPJ inválido | `00038174000144` | 400 `CNPJ_INVALIDO`, **sem** consulta ao motor |
| CNPJ válido inexistente | `99999999999999` | 404 `EMPRESA_NAO_ENCONTRADA` |
| Competência visível | qualquer uma | `competencia` presente na resposta |

### 5.2 Mascaramento de CPF (FR-017, FR-018, SC-004)

```shell
curl -s localhost:8000/v1/empresas/00038174000143 | jq '.dados.socios[].cpf_socio'
```

Esperado: **sempre** no formato `***456789**` — nunca 11 dígitos. Para o CPF mascarado não
aparecer em log:

```shell
grep -R "\d{11}" logs/ | grep -v "cpf_socio"   # esperado: nenhuma ocorrência
```

### 5.3 Listagem por filtros (P2, FR-008 a FR-011)

```shell
curl -s "localhost:8000/v1/empresas?uf=DF&cnae=4631701&tamanho_pagina=20" | jq '.dados.paginacao'
```

| Cenário | Esperado |
|---|---|
| Filtro combinado | só empresas que atendem a **todos** os filtros |
| `filtros_aplicados` | devolve os filtros normalizados usados |
| Paginação | `pagina=2` devolve a segunda página, com os mesmos filtros |
| Ordenação | `ordenar_por=municipio&direcao=desc` reordena a lista |
| CNAE hierárquico (FR-009) | `cnae=46` retorna empresas de CNAE 46xxx |
| Sem resultado | 200 com `itens: []` e `total: 0` (não é erro) |
| `tamanho_pagina=500` | 400 `PARAMETRO_INVALIDO` |

### 5.4 Busca fuzzy (P2, FR-012 a FR-014, SC-007)

```shell
curl -s "localhost:8000/v1/busca/razao-social?termo=BRASLIA" | jq '.dados.itens[0]'
```

| Cenário | Esperado |
|---|---|
| Typo | a empresa pretendida aparece entre os 10 primeiros |
| Normalização | `BRASILIA`, `brasília` e `BRASÍLIA` devolvem os mesmos candidatos |
| `trecho_destacado` | preenchido e indicando o trecho que casou |
| `similaridade` | valor entre 0 e 1, ordenação decrescente |
| Termo < 3 caracteres | 400 `PARAMETRO_INVALIDO` |
| `total_candidatos` | maior ou igual ao número exibido |

**Meta de qualidade (SC-007)**: com a lista de 50 casos de teste com erro de digitação, a
empresa pretendida aparece no top 10 em **≥ 90%** dos casos.

### 5.5 Lookups e competências

```shell
curl -s localhost:8000/v1/lookups/municipio?uf=DF | jq '.dados.itens | length'
curl -s localhost:8000/v1/competencias | jq '.dados.atual'
```

Esperado: lista de municípios da UF e a competência mais recente disponível.

## 6. Validar a camada web

```shell
cd web
npx vinext check          # se usar vinext; `npx opennextjs-cloudflare build` se usar OpenNext
npx wrangler deploy --dry-run   # confere o bundle e o consumo de CPU/memória
```

| Cenário | Esperado |
|---|---|
| Busca por CNPJ na UI | abre a ficha com cadastro, endereços, sócios e regime |
| Competência na tela | mês/ano visível em toda tela com dados (SC-005) |
| Busca com filtros | lista paginada, com o total de resultados |
| Erro do motor | mensagem compreensível, sem detalhe interno |
| CPF na tela | sempre mascarado |
| Acessibilidade do bundle | dentro de 64 MiB descomprimidos e 128 MB de memória |

> O `wrangler deploy --dry-run` é o que evidencia se o plano gratuito (10 ms de CPU) dá conta
> do SSR. Se não der, o plano B (auto-hospedar o Next.js na VM) está registrado em D5.

## 7. Medir desempenho (SC-002, SC-003)

```shell
python -m scripts.medir_desempenho \
  --endpoint http://localhost:8000 \
  --cnpj 00038174000143 \
  --filtros "uf=DF&cnae=4631701" \
  --requisicoes 50
```

| Métrica | Alvo |
|---|---|
| `GET /v1/empresas/{cnpj}` — p95 | ≤ 3 s |
| `GET /v1/empresas` (filtrada) — p95 | ≤ 5 s |
| Tempo até a resposta na UI | ≤ 60 s (SC-001, SC-008) |

O relatório gerado MUST ser anexado ao README, junto do baseline de conversão já existente —
a constituição exige que qualquer novo estágio registre seu custo e sua memória de pico.

## 8. Executar a suíte

```shell
pytest                                   # pipeline, serviço de consulta, contrato da API
cd web && npm test                       # componentes e route handlers
```

Critérios de saída (gates do plano):

- [ ] Todos os testes passam
- [ ] `write_statistics` verificado como `true` (§4)
- [ ] Nenhum CPF em claro em resposta ou em log (§5.2)
- [ ] Medidas de p95 dentro do alvo (§7)
- [ ] SC-007 ≥ 90% no top 10 com typos (§5.4)
- [ ] Nenhuma implementação de exportação ou painel de agregados (FR-019)

## 9. Referências

- Requisitos: [spec.md](./spec.md) · [checklists/requirements.md](./checklists/requirements.md)
- Plano: [plan.md](./plan.md) · Pesquisa: [research.md](./research.md)
- Modelo: [data-model.md](./data-model.md) · Contratos: [contracts/](./contracts/)
