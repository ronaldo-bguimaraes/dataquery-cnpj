# Quickstart — DAG manual Raw → Bronze (filesystem local)

**Feature**: `002-cnpj-download-dag` · **Data**: 2026-10-27

Valida uma execução pequena pela UI do Airflow com armazenamento **local** em `data/`,
montado como `/data`. Consulte [spec.md](./spec.md), [plan.md](./plan.md) e
[contracts](./contracts/).

MinIO **não** faz parte deste roteiro local. R2 entra só como troca de variáveis, sem
mudar a DAG.

## Pré-requisitos

- Docker e Docker Compose v2; rede até a RFB para o run real.
- `.env` na raiz a partir de `.env.example` (gitignored).
- Pasta `data/` na raiz (versionada só via `data/.gitkeep`).

```shell
cp .env.example .env
# CNPJ_STORAGE_BACKEND=filesystem
# CNPJ_DATA_ROOT=/data
docker compose build
docker compose up -d
docker compose ps
```

Esperado: serviços Airflow saudáveis **sem** depender de MinIO. UI:
<http://localhost:8080>. No host, artefatos aparecem em `data/raw/` e `data/bronze/`.

## Verificar a DAG

```shell
docker compose exec airflow-scheduler airflow dags list | grep cnpj_download_manual
docker compose exec airflow-scheduler airflow dags details cnpj_download_manual
```

A DAG é manual. O import **não** chama RFB, não lista `data/` e não converte. Graphviz
é opcional; a UI das tasks mapeadas basta para ver o par por arquivo.

## Execução mínima

Dispare `cnpj_download_manual` com:

```json
{"competencia":"2026-02","limite":1}
```

Esperado: um grupo `baixar_raw` → `converter_bronze`. Raw existente e size > 0 é
reutilizado e ainda convertido. O resumo marca `parcial`, `selecionados=1`.

Forma esperada do XCom de `consolidar_resumo` (`limite=1`):

```json
{
  "competencia": "2026-02",
  "encontrados": 37,
  "selecionados": 1,
  "limite": 1,
  "parcial": true,
  "arquivos": [
    {
      "nome": "Cnaes.zip",
      "status_download": "baixado",
      "status_conversao": "publicado",
      "raw_key": "raw/2026-02/Cnaes.zip",
      "bronze_key": "bronze/2026-02/Cnaes.parquet",
      "bytes": 123456,
      "bytes_parquet": 45678,
      "linhas": 1300,
      "erro": null
    }
  ],
  "totais": {
    "baixados": 1,
    "reutilizados": 0,
    "publicados": 1,
    "falhas_download": 0,
    "falhas_conversao": 0
  },
  "falhas": 0
}
```

Inspeção no host (mesmos nomes lógicos):

```shell
ls data/raw/2026-02/
ls data/bronze/2026-02/
```

O Parquet deve abrir no PyArrow, expor as colunas do schema do conjunto e ter linhas
iguais à soma dos blocos. Raw permanece por competência.

## Verificações de falha e idempotência

- Competência malformada ou `limite: 0`: falha antes da listagem RFB.
- Segunda execução com o ZIP já em `data/raw/...`: status `reutilizado`, zero bytes da
  RFB, conversão ainda roda.
- Falha de download: conversão pareada não executa; outras instâncias podem concluir;
  run `failed`.
- Falha de conversão: ZIP Raw permanece; Parquet anterior (se houver) intacto; sem
  arquivo parcial em `data/bronze/`.
- Logs/XCom sem token, credencial, URL autenticada ou path absoluto de scratch.
- Duas vezes `limite=1`: o mesmo primeiro nome da ordem estável; Raw existente não abre
  vaga para um segundo arquivo.

## Troca para R2 (configuração apenas)

Não faz parte do primeiro teste local. Para apontar produção:

```shell
# no ambiente dos workers, sem commit
CNPJ_STORAGE_BACKEND=s3
CNPJ_S3_ENDPOINT=https://<accountid>.r2.cloudflarestorage.com
CNPJ_S3_BUCKET=...
CNPJ_S3_ACCESS_KEY=...
CNPJ_S3_SECRET_KEY=...
```

Mesma DAG, mesmos parâmetros de trigger, mesmas chaves lógicas. Provisionar o bucket é
operação fora desta feature. Validar SC-012/SC-013: nenhum código de DAG/download/conversão
alterado para essa troca.

## Critérios de sucesso observáveis

| Critério | Como confirmar |
|---|---|
| SC-001 | Tasks `baixar_raw` e `converter_bronze` pareadas no run |
| SC-002 | Parquet em `data/bronze/...` legível, schema e linhas |
| SC-003 | Falha de download bloqueia só a conversão pareada; run failed |
| SC-004 | Reexecução com Raw presente não baixa conteúdo RFB |
| SC-005 | `limite=1` cria no máximo um grupo, mesmo com Raw existente |
| SC-006 | Sem temporários em `data/raw`/`data/bronze` nem scratch órfão |
| SC-007 | `dags list` não acessa RFB nem `data/` de competência |
| SC-008 | Logs/XCom sem segredos |
| SC-009 | Resumo com `parcial` e encontrados/selecionados |
| SC-010 | Falha identifica `nome` e etapa |
| SC-011 | Artefatos sob `data/raw/<AAAA-MM>/` e `data/bronze/<AAAA-MM>/` |
| SC-012 | Backend muda só por env |
| SC-013 | Com `s3`, operações passam pela API S3-compatível |

## Testes e encerramento

```shell
docker compose --profile test run --rm airflow-tests tests -q
```

Integração filesystem (sem S3): testes que usam `tmp_path` / raiz temporária.

Integração S3/R2: somente se `CNPJ_STORAGE_BACKEND=s3` e o endpoint estiver acessível;
caso contrário skip explícito.

```shell
docker compose down
```

Conteúdo em `data/` persiste no host. Não commitar ZIPs/Parquets (`/data/*` no
`.gitignore`).
