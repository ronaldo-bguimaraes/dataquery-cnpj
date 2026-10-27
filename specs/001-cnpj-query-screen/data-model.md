# Modelo de Dados — Tela de Consulta de Dados CNPJ

**Feature**: `001-cnpj-query-screen` · **Data**: 2026-10-22
**Fontes**: [spec.md](./spec.md) (entidades de domínio) · [research.md](./research.md) D2 e D4
(layout físico e índice de busca) · `schema.py` (schemas canônicos vigentes)

---

## 1. Entidades de Domínio

### 1.1 Empresa

Registro cadastral de uma pessoa jurídica, identificado pelo CNPJ.

| Campo | Tipo lógico | Obrigatório | Regras de validação | Origem |
|---|---|---|---|---|
| `cnpj_basico` | texto | sim | 8 dígitos; prefixo da base de registro na Receita | `schema.py::empresas` |
| `razao_social` | texto | sim | 1–150 caracteres | `schema.py::empresas` |
| `natureza_juridica` | código | sim |FK → `naturezas_juridicas.natureza_juridica_codigo` | `schema.py::empresas` |
| `qualificacao_responsavel` | código | sim | FK → `qualificacoes_socios.qualificacao_socio_codigo` | `schema.py::empresas` |
| `capital_social` | decimal | sim | ≥ 0; scale 2 | `schema.py::empresas` |
| `porte` | enum | sim | conjunto fechado (microempresa, empresa de pequeno porte, médio, grande, Simples Nacional, pessoa física, outros) | `schema.py::empresas` |
| `ente_federativo` | texto | não | texto livre | `schema.py::empresas` |

- **Derivado**: `uf` (sigla de 2 letras) — **não existe** na tabela `empresas`; é deduzida do
  dígito 9 do CNPJ (ramo): 0=SP, 1=RS, 2=MG, 3=ES, 4=BA, 5=PR, 6=SC, 7=PE, 8=CO, 9=DF.
- **Relacionamentos**: 1 → N Estabelecimento · 1 → N Sócio · 1 → 1 Regime Tributário.
- **Chave natural**: CNPJ completo (14 dígitos) = `cnpj_basico` + 2 dígitos de ramo + 1 dígito
  de ordem + 1 dígito verificador.

### 1.2 Estabelecimento

Endereço operacional da empresa (matriz ou filial).

| Campo | Tipo lógico | Obrigatório | Regras de validação | Origem |
|---|---|---|---|---|
| `estabelecimento_cnpj_basico` | texto | sim | FK → Empresa.`cnpj_basico` | `schema.py::estabelecimentos` |
| `estabelecimento_cnpj_ordem` | texto | sim | 2 dígitos | idem |
| `estabelecimento_cnpj_dv` | texto | sim | 1 dígito | idem |
| `estabelecimento_identificador_matriz_filial` | enum | sim | 1 = matriz, 2 = filial | idem |
| `estabelecimento_nome_fantasia` | texto | não | texto livre | idem |
| `estabelecimento_situacao_cadastral` | código | sim | FK → `motivos.motivo_codigo` | idem |
| `estabelecimento_data_situacao_cadastral` | data | sim | AAAA-MM-DD | idem |
| `estabelecimento_data_inicio_atividade` | data | sim | AAAA-MM-DD | idem |
| `estabelecimento_cnae_fiscal_principal` | código | sim | 7 dígitos; hierarquia 2.2.2-2 | idem |
| `estabelecimento_cnae_fiscal_secundaria` | texto | não | lista separada por `;` de códigos CNAE | idem |
| `estabelecimento_logradouro`, `_numero`, `_complemento`, `_bairro` | texto | — | endereço | idem |
| `estabelecimento_cep` | texto | sim | 8 dígitos | idem |
| `estabelecimento_uf` | enum | sim | sigla válida; **é a UF real do endereço** | idem |
| `estabelecimento_municipio` | código | sim | FK → `municipios.municipio_codigo` | idem |
| `estabelecimento_correio_eletronico` | texto | não | sem máscara em log | idem |
| `estabelecimento_ddd_1`, `_telefone_1`, `_ddd_2`, `_telefone_2`, `_ddd_fax`, `_numero_fax` | texto | — | contato | idem |

- **Relacionamentos**: N → 1 Empresa · N → 1 Município.
- **Nota**: `estabelecimento_uf` (UF do endereço) pode divergir da UF derivada do CNPJ (UF do
  registro). As duas são mantidas; a partição usa a UF do **estabelecimento** para esta tabela
  e a UF derivada do CNPJ para `empresas` e `socios`.

### 1.3 Sócio

| Campo | Tipo lógico | Obrigatório | Regras de validação | Origem |
|---|---|---|---|---|
| `socio_cnpj_basico` | texto | sim | FK → Empresa.`cnpj_basico` | `schema.py::socios` |
| `socio_identificador_socio` | código | sim | 1 = seriado, 2 = nome | idem |
| `socio_nome_socio` | texto | condicional | obrigatório quando identificador = 2 | idem |
| `socio_cnpj_cpf_socio` | texto | condicional | obrigatório quando identificador = 1; **sempre mascarado na saída** | idem |
| `socio_qualificacao_socio` | código | sim | FK → `qualificacoes_socios.qualificacao_socio_codigo` | idem |
| `socio_data_entrada_sociedade` | data | sim | AAAA-MM-DD | idem |
| `socio_pais` | código | sim | FK → `paises.pais_codigo` | idem |
| `socio_representante_legal` | código | sim | 0 = não, 1 = sim | idem |
| `socio_nome_representante` | texto | condicional | obrigatório quando representante = 1 | idem |
| `socio_qualificacao_representante_legal` | código | não | FK → `qualificacoes_socios` | idem |
| `socio_faixa_etaria` | código | não | faixa etária | idem |

- **Regra de privacidade (FR-017, FR-018)**: `socio_cnpj_cpf_socio` **nunca** sai da API em
  claro. A máscara é aplicada na fronteira do serviço de consulta, antes da serialização, e o
  valor original não entra em log.

### 1.4 Regime Tributário

| Campo | Tipo lógico | Regras | Origem |
|---|---|---|---|
| `simples_cnpj_basico` | texto | FK → Empresa | `schema.py::simples` |
| `simples_opcao_simples` | data | AAAA-MM-DD; nulo se não optou | idem |
| `simples_data_opcao_simples` | data | idem | idem |
| `simples_data_exclusao_simples` | data | idem | idem |
| `simples_opcao_mei` | data | idem | idem |
| `simples_data_opcao_mei` | data | idem | idem |
| `simples_data_exclusao_mei` | data | idem | idem |

### 1.5 Tabelas de Apoio

`paises` (código, descrição) · `municipios` (código, descrição) · `qualificacoes_socios`
(código, descrição) · `naturezas_juridicas` (código, descrição) · `cnaes` (código, descrição) ·
`motivos` (código, descrição). Todas com chave natural pelo código.

### 1.6 Competência

Mês e ano de referência dos dados (ex.: `2026-02`). Toda tela e toda resposta da API carregam a
competência. É o identificador primário de uma execução do pipeline e a chave de versionamento
dos artefatos no bucket.

### 1.7 Filtro de Busca e Resultado de Busca

- **Filtro de Busca**: `razao_social` (texto, opcional), `uf` (sigla, opcional), `municipio`
  (código, opcional), `cnae` (código ou prefixo, opcional), `porte` (enum, opcional),
  `ordenar_por` (critério), `pagina` (número), `tamanho_pagina` (1–100).
- **Resultado de Busca**: lista paginada de **resumos de empresa** + total de correspondências +
  competência consultada + trecho destacado da razão social (FR-013).
- **Resumo de empresa** (projeção para a listagem): `cnpj`, `razao_social`, `razao_social_norm`
  (derivado), `uf`, `municipio`, `porte`, `cnae_principal`, `similaridade` (só na busca fuzzy).

---

## 2. Artefatos de Dados no Bucket

### 2.1 Nomenclatura

```text
s3://<bucket>/cnpj/competencia=<AAAA-MM>/tabela=<nome>/uf=<SIGLA>/part-<NNNNN>.parquet
```

Exemplo: `s3://<bucket>/cnpj/competencia=2026-02/tabela=empresas/uf=SP/part-00000.parquet`

### 2.2 Tabelas Canônicas (já existentes, mantidas)

| Tabela | Particionamento | Ordenação dentro da partição | Origem |
|---|---|---|---|
| `empresas` | `uf` (derivada do CNPJ) | `cnpj_basico` | `schema.py::empresas` |
| `estabelecimentos` | `estabelecimento_uf` | `estabelecimento_cnpj_basico`, `estabelecimento_cnpj_ordem` | `schema.py::estabelecimentos` |
| `socios` | `uf` (derivada do CNPJ) | `socio_cnpj_basico`, `socio_identificador_socio` | `schema.py::socios` |
| `simples` | `uf` (derivada do CNPJ) | `simples_cnpj_basico` | `schema.py::simples` |
| `*_lookup` | — | `codigo` | `schema.py` (6 tabelas) |

### 2.3 Tabelas Derivadas (novas, produced pelo pipeline)

| Tabela | Linhas | Colunas | Finalidade |
|---|---|---|---|
| `busca_documentos` | 1 por empresa | `cnpj_basico`, `razao_social`, `razao_social_norm`, `uf`, `municipio`, `porte`, `cnae_principal`, `trigramas` (lista de texto) | documento indexado + colunas de filtro + conjunto completo de trigramas |
| `busca_postings` | 1 por (empresa, trigrama) | `cnpj_basico`, `trigrama`, `df` (frequência no corpus) | geração de candidatos por trigramas mais raros |

**Regras de derivação**
- `razao_social_norm` = `razao_social` em minúsculas, sem acento, sem pontuação, espaços
  colapsados em um único espaço.
- `trigramas` = janelas deslizantes de 3 caracteres sobre `razao_social_norm`, sem espaços;
  nomes com menos de 3 caracteres geram o próprio texto como único trigrama.
- `df` = número de documentos que contêm o trigrama (calculado no mesmo lote, por agregação).
- `busca_postings` guarda apenas os trigramas **selecionados** de cada documento (os mais raros
  dentro do próprio nome, limite de 8 por documento) para conter o tamanho; a lista completa
  fica em `busca_documentos.trigramas` e é usada no ranqueamento.

### 2.4 Layout Físico dos Parquet

| Parâmetro | Valor | Por quê |
|---|---|---|
| `write_statistics` | `True` (hoje `False` — corrigir) | sem statistics não há zonemap → toda consulta vira varredura de 811 MB |
| Ordenação | pela chave de consulta da tabela | poda por min/max dentro do row group |
| Row group | 100 mil – 1 milhão de linhas | abaixo de 5 mil linhas o row group fica 5–10× mais lento |
| Compressão | ZSTD | melhor razão bytes/latência em leitura remota |
| Formato | Parquet | portável entre DuckDB, Spark, Trino e Iceberg; formato canônico do projeto |
| Tamanho do arquivo | ~30 MB por partição de UF | consequência de 811 MB ÷ 27 UFs; aceitável porque o gargalo é o row group |

### 2.5 Normalização e Similaridade

- **Similaridade de dice** entre o conjunto de trigramas da consulta e o do documento:
  `dice = 2 · |A ∩ B| / (|A| + |B|)`, com `A` = trigramas da consulta, `B` = trigramas do
  documento. Faixa 0–1.
- **Ordenação**: `dice` decrescente → desempate por porte (maior capital social primeiro) →
  desempate estável por CNPJ.
- **Corte**: candidatos com `dice` abaixo do limiar (0,25) são descartados; o corte é
  parametrizável e validado no quickstart (meta SC-007: ≥ 90% no top 10).

---

## 3. Regras de Validação

| Regra | Onde é aplicada | Mensagem ao usuário |
|---|---|---|
| CNPJ com 14 dígitos e dígitos verificadores válidos | borda da API, antes de consultar | "CNPJ inválido" (FR-002) |
| CNPJ normalizado (só dígitos) para consulta | borda da API | — |
| Sigla de UF válida (27) | borda da API | "UF inválida" |
| Código CNAE com 7 dígitos, hierarquia `2.2.2-2` | borda da API | "CNAE inválido" |
| `porte` dentro do conjunto fechado | borda da API | "Porte inválido" |
| `municipio` existe na tabela de municípios | borda da API | "Município não encontrado" |
| `tamanho_pagina` entre 1 e 100 | borda da API | "Página inválida" |
| Competência existente no bucket | borda da API | "Competência não disponível" |
| Ausência de sócios/CNAE secundário/Simples | consulta | seção vazia, sem erro |

---

## 4. Transições de Estado

Não aplicável: a feature é **somente leitura**. Nenhum artefato é alterado pela tela. A escrita
ocorre exclusivamente no pipeline, por competência, de forma idempotente — reprocessar uma
competência substitui seus artefatos de forma atômica.

---

## 5. Referências

- Contrato da API: [contracts/query-api.md](./contracts/query-api.md)
- Contrato dos artefatos: [contracts/data-contracts.md](./contracts/data-contracts.md)
- Decisões de layout: [research.md](./research.md) D2 e D4
