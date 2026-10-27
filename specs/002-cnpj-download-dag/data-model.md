# Modelo de Dados — DAG Raw → Bronze

**Feature**: `002-cnpj-download-dag` · **Data**: 2026-10-27
**Fontes**: [spec.md](./spec.md), [research.md](./research.md)

Localizações neste modelo são **chaves lógicas**. O backend (filesystem ou S3/R2) resolve
a chave para um path ou objeto; o domínio não armazena URI autenticada.

## 1. Competência

Identificador `AAAA-MM`, mês 01–12. Recebida no trigger manual, validada antes de rede e
storage, e usada nos prefixos `raw/<AAAA-MM>/` e `bronze/<AAAA-MM>/`.

## 2. Arquivo selecionado

| Campo | Tipo | Regra |
|---|---|---|
| `nome` | string | basename da listagem WebDAV; ordenação lexicográfica determinística |
| `competencia` | string | competência validada do run |
| `remote_path` | string interno | caminho relativo WebDAV; nunca logado/XCom |
| `raw_key` | string | `raw/<AAAA-MM>/<nome>` |
| `bronze_key` | string | `bronze/<AAAA-MM>/<nome-base>.parquet` |
| `status_download` | enum | `baixado`, `reutilizado`, `falhou` |
| `status_conversao` | enum | `publicado`, `falhou`, `não executado` |
| `bytes` | inteiro | bytes transferidos da RFB ou 0 se reutilizado; ≥ 0 |
| `bytes_parquet` | inteiro | tamanho do Parquet publicado; 0 se não publicado |
| `linhas` | inteiro ou nulo | soma das linhas dos blocos; nulo se conversão não concluiu |
| `duracao_segundos` | número | duração observada da instância |
| `erro` | string ou nulo | mensagem sanitizada, sem URL/token/credencial/path absoluto |

O limite seleciona os primeiros N nomes **antes** de consultar Raw. Um Raw existente
entre os N conta no limite e segue para conversão.

## 3. Backend de armazenamento

Uma instância por processo/tarefa, criada pela fábrica a partir da configuração. Não é
uma entidade persistida no Airflow.

| Campo | Tipo | Regra |
|---|---|---|
| `backend` | enum | `filesystem` \| `s3` |
| `root` | string | só `filesystem`: raiz absoluta (`/data`); não vai para XCom |
| `endpoint`, `bucket` | string | só `s3`; endpoint sem credencial embutida |
| `credenciais` | secret | só ambiente; nunca em logs, XCom, `repr` ou traceback |

Operações do contrato: preflight, `head` (existência e size), leitura em stream, publicação
atômica, remoção de temporários do backend quando aplicável.

### Layout lógico (igual nos dois backends)

```text
raw/2026-02/Empresas0.zip
raw/2026-02/Cnaes.zip
bronze/2026-02/Empresas0.parquet
bronze/2026-02/Cnaes.parquet
```

No filesystem local isso vive sob `data/` (host) / `/data` (container). No R2, sob o
bucket configurado com as mesmas chaves.

Regras:

- Raw válido: presente e size > 0.
- Publicação atômica: a chave final não recebe bytes parciais.
- Scratch de tarefa não é parte do modelo persistido.
- `max_active_runs=1` impede duas escritas concorrentes na mesma chave.

## 4. Instâncias Airflow

`listar_arquivos` devolve `{encontrados, selecionados, limite, parcial, nomes[]}`.
Mapping cria:

```text
arquivo[n].baixar_raw → arquivo[n].converter_bronze
```

Download retorna `nome`, `raw_key`, status, bytes, duração. Conversão retorna
`bronze_key`, linhas, bytes do Parquet, status. Sem conteúdo binário, URL com token ou
credencial.

## 5. Resumo do DAG Run

- `competencia`, `encontrados`, `selecionados`, `limite`, `parcial`.
- Lista por arquivo com estados, bytes, linhas, chaves lógicas Raw/Bronze, erro sanitizado.
- Totais de baixados, reutilizados, publicados, falhas de download/conversão.
- Qualquer falha de instância ⇒ run `failed`; artefatos concluídos permanecem.

## 6. Resolução de schema e Parquet

- Prefixo do nome do ZIP seleciona exatamente uma definição em `schema.py`.
- ZIP com quantidade de membros ≠ 1 falha só na conversão daquele arquivo.
- CSV: `latin1`, `;`, PyArrow em blocos, colunas do schema, valores texto.
- Tipagem/limpeza: Silver futura.

## 7. Configuração

| Configuração | Origem | Uso |
|---|---|---|
| `CNPJ_STORAGE_BACKEND` | `.env`/ambiente | `filesystem` (default local) ou `s3` (R2) |
| `CNPJ_DATA_ROOT` | `.env`/ambiente | raiz local; `/data` nos containers |
| `CNPJ_S3_ENDPOINT` | `.env`/ambiente | endpoint S3-compatível (R2) |
| `CNPJ_S3_BUCKET` | `.env`/ambiente | bucket privado |
| `CNPJ_S3_ACCESS_KEY` | `.env` ignorado | credencial |
| `CNPJ_S3_SECRET_KEY` | `.env` ignorado | credencial |
| `CNPJ_S3_REGION` | `.env`/ambiente | opcional |
| `AIRFLOW_UID` | Compose | usuário dos containers |

## 8. Rastreabilidade

| Requisito | Modelo/contrato |
|---|---|
| FR-001 | DAG única e manual: [dag-params.md](./contracts/dag-params.md) |
| FR-002 | Competência e limite: §1 e dag-params |
| FR-003 | Listagem e ordem estável: §2 e dag-params |
| FR-004 | Limite antes da checagem Raw: §2 |
| FR-005 | Par mapeado: §4 |
| FR-006 | Raw `raw/<AAAA-MM>/<nome>`: §3 |
| FR-007 | Reuso de Raw size > 0: §3 e [download-api.md](./contracts/download-api.md) |
| FR-008 | Schema, `latin1`, `;`, blocos: §6 |
| FR-009 | Bronze `bronze/<AAAA-MM>/<nome-base>.parquet`: §3 |
| FR-010–FR-011 | Isolamento e run failed: §5 e dag-params |
| FR-012 | Publicação atômica: §3 e research R7 |
| FR-013 | Scratch local; XCom pequeno: §4 |
| FR-014 | Resumo: §5 |
| FR-015 | Import sem I/O: download-api |
| FR-016–FR-020 | Backend configurável e protocolo: §3, §7, [storage.md](./contracts/storage.md) |
