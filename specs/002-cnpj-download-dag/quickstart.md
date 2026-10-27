# Quickstart — Validação da Primeira DAG de Download de Dados CNPJ

**Feature**: `002-cnpj-download-dag` · **Data**: 2026-10-27
**Objetivo**: provar, de forma executável, que a DAG manual baixa uma competência para o
diretório persistente, respeita o limite, é idempotente, falha de forma visível e não vaza o
token. Cada cenário cita os requisitos da [spec](./spec.md) que valida.
**Não contém**: implementação, migrations ou a suíte completa — isso pertence a `tasks.md` e à
fase de implementação. Os contratos estão em [contracts/](./contracts/).

---

## 1. Pré-requisitos

| Item | Como verificar |
|---|---|
| Docker + Docker Compose v2 | `docker compose version` |
| Acesso de rede à RFB | `curl -I https://arquivos.receitafederal.gov.br` |
| Repositório na raiz | `test -d src && test -d airflow` |
| `.env` do Airflow | `test -f airflow/.env` (não versionado; contém `AIRFLOW_UID`; `FERNET_KEY` é opcional) |

```shell
cd /caminho/para/dataquery-cnpj
mkdir -p data/cnpj
docker compose build
docker compose up -d
docker compose ps
```

Esperado: `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-worker`,
`airflow-triggerer`, `postgres` e `redis` em `healthy`/`running`.

UI: <http://localhost:8080> — login `airflow` / `airflow` (default local).

> O `airflow-init` cria e ajusta o dono de `/data/cnpj` para `AIRFLOW_UID`. O diretório
> `data/cnpj` fica no host e sobrevive a `docker compose down`; não use `down -v` esperando
> apagar a saída (o bind mount não é volume nomeado).

---

## 2. Conferir que a DAG foi carregada sem rede

```shell
docker compose exec airflow-scheduler airflow dags list | grep cnpj_download_manual
docker compose exec airflow-scheduler airflow dags show cnpj_download_manual --save /tmp/dag.dot
```

Esperado: a DAG aparece na lista e tem uma única task `baixar_arquivos`. O comando não baixa
arquivos nem lista a RFB; o import da DAG só define metadados (FR-011/SC-007).

Também é possível desligar a rede durante o teste unitário de import; ele é parte da suíte.

---

## 3. Cenário P1/P2 — primeira execução com limite 1

```shell
rm -rf data/cnpj/2026-02
docker compose exec airflow-scheduler \
  airflow dags test cnpj_download_manual 2026-10-27 \
  -c '{"competencia": "2026-02", "limite": 1}'
```

Esperado:

- task `baixar_arquivos` com sucesso;
- exatamente **um** arquivo em `data/cnpj/2026-02/`, o primeiro em ordem lexicográfica
  (`Cnaes.zip` para a competência de referência);
- log/resumo com `encontrados` igual ao total da competência (**37** na data desta spec),
  `selecionados=1`, `parcial=true`, `baixados=1`, `ignorados=0`, `falhas=0`, bytes e duração;
- nenhum arquivo `.part`.

```shell
ls -lh data/cnpj/2026-02
```

Valida FR-004, FR-005, FR-006, FR-008, FR-009, FR-012 e SC-004/SC-009.

### 3.1 Reexecução idempotente (P3)

Rode o mesmo comando novamente. Esperado: `baixados=0`, `ignorados=1`,
`bytes_baixados=0`, `parcial=true`, arquivo inalterado. Nenhum download é feito para o
arquivo já presente (FR-014/FR-017/SC-003).

```shell
stat -c '%n %s %y' data/cnpj/2026-02/Cnaes.zip
```

### 3.2 Limite maior que o total

```shell
docker compose exec airflow-scheduler \
  airflow dags test cnpj_download_manual 2026-10-27 \
  -c '{"competencia": "2026-02", "limite": 1000}'
```

Esperado: `parcial=false`, `selecionados=37` e todos os arquivos baixados (pode demorar e
ocupar gigabytes). Use apenas quando quiser a competência completa.

---

## 4. Cenário de erro — competência inválida (FR-002/FR-003/SC-005)

```shell
docker compose exec airflow-scheduler \
  airflow dags test cnpj_download_manual 2026-10-27 \
  -c '{"competencia": "2026-13"}'
```

Esperado: task `failed`; mensagem com o formato esperado (`AAAA-MM`, mês 01–12); nenhum
diretório novo, nenhuma listagem e nenhum download. Repita com `2026-2` e `02-2026` se quiser.

Limite inválido (FR-007):

```shell
docker compose exec airflow-scheduler \
  airflow dags test cnpj_download_manual 2026-10-27 \
  -c '{"competencia": "2026-02", "limite": 0}'
```

Esperado: falha antes de listar/baixar.

---

## 5. Cenário de erro — competência válida sem arquivos (FR-016)

Use uma competência futura, ainda não publicada:

```shell
docker compose exec airflow-scheduler \
  airflow dags test cnpj_download_manual 2026-10-27 \
  -c '{"competencia": "2099-01"}'
```

Esperado: task `failed` com `EmptyCompetenciaError` ("nenhum arquivo encontrado"); nenhum
arquivo baixado. O 404 da fonte é traduzido para lista vazia, não para erro de listagem.

---

## 6. Cenário de falha de download (FR-013/SC-006)

A falha real depende de rede/HTTP e é difícil de induzir no host. A suíte cobre exatamente
este caso com um servidor local que responde 500 e outro que trunca o corpo:

```shell
docker compose --profile test run --rm airflow-tests \
  /opt/dataquery-cnpj/tests/unit/test_download_files.py -q
```

Esperado: `DownloadError`/relatório com `falhas >= 1`, arquivo afetado identificado, arquivos
bem-sucedidos preservados e nenhum `.part`.

Para repetir manualmente no container, interrompa a rede durante uma execução com limite 1;
a task deve falhar e o arquivo parcial não deve aparecer no diretório.

---

## 7. Cenário de diretório inválido (FR-010)

```shell
docker compose exec airflow-scheduler \
  env CNPJ_DOWNLOAD_DIR=/proc/cnpj \
  airflow dags test cnpj_download_manual 2026-10-27 \
  -c '{"competencia": "2026-02", "limite": 1}'
```

Esperado: falha com `OutputDirectoryError` antes de qualquer listagem/download.

> O `env` acima altera só o processo do comando; a DAG em produção usa
> `CNPJ_DOWNLOAD_DIR=/data/cnpj` definido no compose.

---

## 8. Verificar o resumo e os logs

No UI: `DAGs → cnpj_download_manual → Runs → <run> → baixar_arquivos → XCom` — o campo
`return_value` contém o resumo JSON.

No container:

```shell
docker compose logs airflow-scheduler | grep -F 'Resumo do download'
```

Esperado: JSON com `competencia`, `output_dir`, `encontrados`, `selecionados`, `baixados`,
`ignorados`, `falhas`, `bytes_baixados`, `duracao_segundos`, `parcial` e `arquivos`.
Nenhuma URL ou token (FR-012/FR-019).

### 8.1 Token fora do log (SC-008)

```shell
docker compose logs airflow-scheduler airflow-worker | grep -E 'public\.php/dav/files|Authorization' || echo 'OK: nenhum token/URL no log'
```

Esperado: `OK: nenhum token/URL no log`. Também vale para a saída do `airflow dags test` e
para o XCom.

---

## 9. Rodar a suíte automatizada

```shell
# unitários + e2e local (sem rede)
docker compose --profile test build airflow-tests
docker compose --profile test run --rm airflow-tests

# cobertura
docker compose --profile test run --rm airflow-tests \
  /opt/dataquery-cnpj/tests --cov=src --cov-report=term-missing

# integração real com a RFB (opt-in; requer rede)
docker compose --profile test run --rm -e RUN_NETWORK_TESTS=1 airflow-tests \
  /opt/dataquery-cnpj/tests/integration -q
```

Esperado: suíte padrão verde sem acessar a RFB; integração valida a listagem de `2026-02`
(37 arquivos, ordenados, sem diretórios).

---

## 10. Matriz de cobertura dos cenários

| Cenário | Onde validar | Requisitos |
|---|---|---|
| DAG manual, sem schedule | UI/`airflow dags list` | FR-001 |
| Competência válida, limite 1 | Seção 3 | FR-002/004/005/006/008 |
| Reexecução idempotente | Seção 3.1 | FR-014/FR-017, SC-003 |
| Limite maior que o total | Seção 3.2 | FR-006/FR-008 |
| Competência inválida | Seção 4 | FR-002/FR-003, SC-005 |
| Limite inválido | Seção 4 | FR-007 |
| Competência sem arquivos | Seção 5 | FR-016 |
| Falha de download | Seção 6 / testes | FR-013, SC-006 |
| Diretório inválido | Seção 7 | FR-009/FR-010 |
| Import sem rede | Seção 2 / `test_dag_import.py` | FR-011, SC-007 |
| Resumo e contagens | Seção 8 | FR-012, SC-001 |
| Token fora do log | Seção 8.1 | FR-019, SC-008 |
| Escrita atômica / sem `.part` | Seções 3 e 6 | FR-015, SC-002 |
| Duas execuções concorrentes | `test_download_competencia.py` (lock) | FR-018 |

---

## 11. Limpeza

```shell
docker compose down          # mantém data/cnpj e volumes do Airflow
rm -rf data/cnpj/2026-02     # remove apenas a competência de teste
```

Não use `docker compose down -v` para apagar a saída: o bind mount fica no host. Use
`down -v` apenas para reinicializar Postgres/Redis do Airflow.
