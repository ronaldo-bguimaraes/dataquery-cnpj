# Modelo de Dados — Primeira DAG de Download de Dados CNPJ

**Feature**: `002-cnpj-download-dag` · **Data**: 2026-10-27
**Fontes**: [spec.md](./spec.md) (FR-001 a FR-020) · [research.md](./research.md) (D2, D3, D4,
D5, D6, D8, D14) · contrato da DAG em [contracts/dag-params.md](./contracts/dag-params.md)

---

## 1. Entidades de Domínio

### 1.1 Competência

Mês e ano de referência da publicação da RFB. É o identificador primário da execução e a chave
de idempotência.

| Campo | Tipo lógico | Obrigatório | Regras | Origem |
|---|---|---|---|---|
| `valor` | texto | sim | `^\d{4}-(0[1-9]\|1[0-2])$`; sem espaços após normalização; mês 01–12 | parâmetro `competencia` da DAG / `--competencia` da CLI |
| `ano` | inteiro | derivado | 4 dígitos extraídos de `valor` | derivado |
| `mes` | inteiro | derivado | 01–12 extraído de `valor` | derivado |

**Invariantes**

- `2026-02` é válida; `2026-2`, `02-2026`, `2026-13`, `2026-00`, `2026/02`, `2026-02-01`,
  texto vazio e `None` são inválidas.
- A validação ocorre antes de criar diretório, listar ou baixar (FR-002, FR-003).
- O valor normalizado é usado como nome do subdiretório de saída.

### 1.2 Arquivo da RFB

Arquivo publicado pela Receita Federal para uma competência.

| Campo | Tipo lógico | Obrigatório | Regras | Origem |
|---|---|---|---|---|
| `nome` | texto | sim | basename do caminho retornado; não vazio; diferente de `.`/`..`; sem `/` ou `\` | listagem WebDAV |
| `caminho_relativo` | texto | sim | `Dados/Cadastros/CNPJ/<AAAA-MM>/<nome>` | listagem WebDAV |
| `url` | URL | sim | URL completa de download; **nunca** é logada, persistida ou exposta em `repr` | `ClientRFB.get_file_url` |
| `tamanho_origem` | inteiro ou nulo | não | `Content-Length` quando disponível | resposta HTTP |

**Invariantes**

- A listagem ignora entradas terminadas em `/` (diretórios), como `2026-02/`.
- Competência ainda não publicada na fonte (404 `RemoteResourceNotFound`) é traduzida para
  listagem vazia, não para falha de listagem; o orquestrador a converte em
  `EmptyCompetenciaError` (FR-016).
- A ordem canônica é lexicográfica crescente por `nome`, independentemente da ordem do
  servidor (FR-006).
- O nome é a chave de idempotência dentro da competência.

### 1.3 Destino de Download

Representa os caminhos físicos de um arquivo e o arquivo de lock.

| Campo | Tipo lógico | Obrigatório | Regras | Origem |
|---|---|---|---|---|
| `diretorio_base` | caminho | sim | variável `CNPJ_DOWNLOAD_DIR`; criado se não existir | ambiente |
| `diretorio_competencia` | caminho | sim | `<diretorio_base>/<AAAA-MM>` | derivado |
| `arquivo_final` | caminho | sim | `<diretorio_competencia>/<nome>` | derivado |
| `arquivo_temporario` | caminho | sim | `<diretorio_competencia>/.<nome>.<pid>.<uuid>.part` | derivado |
| `arquivo_lock` | caminho | sim | `<diretorio_competencia>/.download.lock` | derivado |

**Invariantes**

- `arquivo_temporario` nunca é considerado completo e é removido em falha/interrupção
  tratada; `.part` remanescente é removido no início da execução seguinte, sob lock.
- `arquivo_final` só aparece após `os.replace` do temporário (FR-015).
- O diretório de competência precisa ser gravável antes de qualquer listagem/download
  (FR-010). O preflight cria e remove um arquivo-sonda.

### 1.4 Resultado por Arquivo

Estado final de um arquivo em uma execução.

| Campo | Tipo lógico | Obrigatório | Regras |
|---|---|---|---|
| `nome` | texto | sim | nome do arquivo da RFB |
| `status` | enum | sim | `baixado` \| `ignorado` \| `falhou` |
| `bytes` | inteiro | sim | `>= 0`; bytes efetivamente gravados (0 em ignorado/falha) |
| `erro` | texto ou nulo | não | somente em `falhou`; sanitizado (sem URL/token) |

**Máquina de estados**

```text
                 arquivo final existe e tamanho > 0
   PENDENTE ───────────────────────────────────────► IGNORADO
      │
      │ arquivo ausente ou tamanho == 0
      ▼
   BAIXANDO ─── erro de rede/HTTP/escrita/tamanho ──► FALHOU
      │                                                   │
      │ bytes completos + os.replace                       │ remove .part
      ▼                                                   ▼
   BAIXADO                                            (fim da execução)
```

- `IGNORADO` não faz requisição HTTP e não altera o arquivo (FR-014).
- `FALHOU` remove o `.part`; o arquivo final anterior (se existia vazio) permanece vazio ou é
  substituído apenas em caso de sucesso.
- `BAIXADO` e `IGNORADO` preservam o arquivo para a próxima execução.

### 1.5 Resumo da Execução

Registro observável devolvido pela função e publicado em XCom.

| Campo | Tipo lógico | Obrigatório | Regras |
|---|---|---|---|
| `competencia` | texto | sim | valor validado |
| `output_dir` | texto | sim | caminho absoluto do diretório da competência |
| `encontrados` | inteiro | sim | total de arquivos listados (sem diretórios) |
| `selecionados` | inteiro | sim | `encontrados` quando sem limite; `min(limite, encontrados)` com limite |
| `baixados` | inteiro | sim | quantidade com status `baixado` |
| `ignorados` | inteiro | sim | quantidade com status `ignorado` |
| `falhas` | inteiro | sim | quantidade com status `falhou` |
| `bytes_baixados` | inteiro | sim | soma dos bytes dos `baixados` |
| `duracao_segundos` | decimal | sim | `>= 0`; listagem + download |
| `parcial` | booleano | sim | verdadeiro somente quando `limite < encontrados` |
| `arquivos` | lista de Resultado por Arquivo | sim | ordenada por `nome` |

**Invariantes**

- `encontrados >= selecionados >= baixados + ignorados + falhas` (a diferença é o que não foi
  selecionado).
- `parcial = limite is not None and limite < encontrados`.
- `falhas > 0` implica task falha (`DownloadError`), mas o resumo continua disponível no erro.
- `encontrados == 0` não gera resumo de sucesso: gera `EmptyCompetenciaError` (FR-016).
- Nenhum campo contém URL, token ou credencial.

### 1.6 Execução de Download

Representa a execução no Airflow.

| Campo | Tipo lógico | Regras |
|---|---|---|
| `dag_id` | texto | `cnpj_download_manual` |
| `run_id` | texto | gerado pelo Airflow |
| `logical_date` | data/hora | data lógica do `airflow dags test`/trigger |
| `params` | objeto | `competencia` e `limite` |
| `status` | enum | `success`, `failed`, `running` (Airflow) |
| `resumo` | Resumo da Execução | XCom `return_value`/log JSON |

- O Airflow é a fonte de verdade do histórico; esta feature não cria tabela própria.
- `max_active_runs=1` limita a uma execução ativa da DAG; o lock cobre chamadas concorrentes
  fora do scheduler.

### 1.7 Configuração de Ambiente

| Variável | Onde | Obrigatória | Default | Efeito |
|---|---|---|---|---|
| `CNPJ_DOWNLOAD_DIR` | container/DAG/CLI | sim | — (compose define `/data/cnpj`) | diretório base persistente |
| `CNPJ_DATA_HOST_DIR` | host/compose | não | `../data/cnpj` (raiz `data/cnpj`) | origem do bind mount |
| `AIRFLOW_UID` | `airflow/.env` | não | `50000` (compose) | dono dos arquivos no bind mount |
| `RUN_NETWORK_TESTS` | testes | não | ausente | habilita teste real da RFB |

**Regras**

- `CNPJ_DOWNLOAD_DIR` MUST NOT ter default no código da DAG; a ausência falha com mensagem
  clara antes de qualquer rede.
- `CNPJ_DATA_HOST_DIR` é resolvido pelo compose; o código só conhece o caminho do container.
- O diretório efetivo da execução é `<CNPJ_DOWNLOAD_DIR>/<AAAA-MM>`.

---

## 2. Limite e Seleção

| Situação | `encontrados` | `selecionados` | `parcial` | Arquivos baixados |
|---|---:|---:|---|---|
| Sem limite | 37 | 37 | `false` | até 37 (menos existentes) |
| Limite 1 | 37 | 1 | `true` | no máximo 1 |
| Limite 37 | 37 | 37 | `false` | até 37 |
| Limite 100 | 37 | 37 | `false` | até 37 |
| Limite 1, primeiro já existe | 37 | 1 | `true` | 0; o primeiro conta como ignorado |
| Limite 1, segundo já existe | 37 | 1 | `true` | 1 (o primeiro); o segundo não é considerado |

**Regra de ouro**: o limite corta a lista ordenada **antes** de verificar existência. Arquivos
existentes entre os selecionados não liberam vaga para os seguintes (FR-006).

---

## 3. Layout Físico

```text
<CNPJ_DOWNLOAD_DIR>/
└── 2026-02/
    ├── .download.lock              # lock exclusivo da competência
    ├── Cnaes.zip
    ├── Empresas0.zip
    ├── Empresas1.zip
    └── ...
```

- Não há arquivo de metadados próprio nesta etapa; o resumo vive no XCom/log do Airflow.
- Arquivos `.part` são transitórios e não aparecem no estado final de uma execução bem-sucedida
  ou de uma falha tratada.
- O diretório pode conter arquivos de competências anteriores? Não: cada competência tem seu
  próprio subdiretório. Um arquivo só é considerado existente dentro da competência corrente.
- Nomes de arquivo são basename da RFB; não são renomeados.

---

## 4. Rastreabilidade dos Requisitos

| Requisito | Onde é garantido no modelo/design |
|---|---|
| FR-001 (manual) | `schedule=None`, DAG sem timetable (D10) |
| FR-002/FR-003 (competência) | `Competência.valor`, validação antes de rede (1.1) |
| FR-004 (listar) | `Arquivo da RFB` via `ClientRFB.list_files` (1.2) |
| FR-005/FR-006 (limite) | Seção 2; seleção antes da existência |
| FR-007 (limite inválido) | `InvalidLimitError` antes de rede (D6) |
| FR-008 (resumo de limite) | `selecionados` e `parcial` (1.5) |
| FR-009/FR-010 (diretório) | `Destino de Download`, preflight (1.3) |
| FR-011 (import sem trabalho) | D1/D10; import tardio na task |
| FR-012 (resumo) | `Resumo da Execução` (1.5) |
| FR-013 (falha de download) | `DownloadError` + `Resultado por Arquivo` (1.4/D6) |
| FR-014 (já baixado) | Estado `IGNORADO`, tamanho > 0 (1.4) |
| FR-015 (atômico) | `.part` + `os.replace` (1.3/D4) |
| FR-016 (sem arquivos) | `EmptyCompetenciaError` (D6) |
| FR-017 (idempotente) | Chave `competencia/nome`, estado `IGNORADO` |
| FR-018 (concorrência) | `arquivo_lock` + `max_active_runs=1` (1.3/D8) |
| FR-019 (token) | `url` com `repr` oculto, redaction (D7) |
| FR-020 (escopo) | Nenhuma entidade de Parquet/R2/web/API neste modelo |
