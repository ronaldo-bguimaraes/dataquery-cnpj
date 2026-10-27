# Contrato da DAG — `cnpj_download_manual`

**Feature**: `002-cnpj-download-dag`
**Implementação prevista**: `airflow/dags/cnpj_download.py`
**Tipo**: execução manual, uma task, sem schedule

---

## 1. Identidade

| Item | Valor |
|---|---|
| `dag_id` | `cnpj_download_manual` |
| `task_id` | `baixar_arquivos` |
| `schedule` | `None` (sem agendamento — FR-001) |
| `catchup` | `false` |
| `max_active_runs` | `1` |
| `retries` | `0` (reexecução manual é idempotente) |
| Tags | `cnpj`, `download`, `manual` |
| Operador | `PythonOperator` |

---

## 2. Acionamento

### 2.1 UI do Airflow

`DAGs → cnpj_download_manual → Trigger DAG` com o JSON de configuração:

```json
{
  "competencia": "2026-02",
  "limite": 1
}
```

### 2.2 CLI (validação local)

```shell
docker compose exec airflow-scheduler \
  airflow dags test cnpj_download_manual 2026-10-27 \
  -c '{"competencia": "2026-02", "limite": 1}'
```

### 2.3 API do Airflow

`POST /api/v2/dags/cnpj_download_manual/dagRuns` com `{"conf": {...}}`. A autenticação é a do
próprio Airflow; esta feature não adiciona endpoint novo.

---

## 3. Parâmetros (`conf`)

| Nome | JSON Schema | Obrigatório | Default | Semântica |
|---|---|---|---|---|
| `competencia` | `{"type": ["null", "string"], "pattern": "^\\d{4}-(0[1-9]\|1[0-2])$"}` | sim, na prática | `null` | mês/ano da RFB no formato `AAAA-MM` |
| `limite` | `{"type": ["null", "integer"], "minimum": 1}` | não | `null` | máximo de arquivos selecionados; `null` = todos |

**Regras**

- O schema acima é aplicado pelo próprio Airflow no momento do trigger: `competencia` fora do
  padrão e `limite` menor que `1` (ou de tipo diferente de inteiro) são rejeitados com
  `ParamValidationError` **antes** de a task existir, sem criar diretório nem acessar a fonte.
  Isso cobre FR-002/FR-003/FR-007 no caminho da DAG.
- As validações de domínio (`InvalidCompetenciaError`, `InvalidLimitError`) continuam valendo para
  a CLI e para chamadas diretas de `download_competencia`, inclusive com `competencia=null`
  vinda de um trigger sem valor.
- O tipo `null` existe apenas para o Airflow permitir o trigger sem valor e produzir uma
  mensagem clara, não para aceitar ausência como sucesso.
- O limite seleciona os `N` primeiros nomes em ordem lexicográfica; existentes entre eles
  contam como ignorados e não liberam vaga (FR-006).
- O diretório de saída **não** é parâmetro. Vem de `CNPJ_DOWNLOAD_DIR` (FR-009), para impedir
  escrita fora do volume previsto.

### 3.1 Exemplos válidos

```json
{"competencia": "2026-02"}
```

```json
{"competencia": "2026-02", "limite": 1}
```

### 3.2 Exemplos inválidos

| `conf` | Erro esperado |
|---|---|
| `{}` | `InvalidCompetenciaError` na task (o default `null` passa no schema, mas a validação de domínio rejeita) |
| `{"competencia": "2026-13"}` | `ParamValidationError` (pattern) antes da task |
| `{"competencia": "2026-2"}` | `ParamValidationError` (pattern) antes da task |
| `{"competencia": "2026-02", "limite": 0}` | `ParamValidationError` (minimum) antes da task |
| `{"competencia": "2026-02", "limite": -1}` | `ParamValidationError` (minimum) antes da task |
| `{"competencia": "2026-02", "limite": "1"}` | `ParamValidationError` (tipo) antes da task |

O `ParamValidationError` interrompe o trigger: nenhuma task é criada e nenhum diretório é
escrito. A CLI e a chamada direta da função continuam levantando `InvalidCompetenciaError` /
`InvalidLimitError` para os mesmos valores.

---

## 4. Pré-condições

| Condição | Verificação | Falha |
|---|---|---|
| `CNPJ_DOWNLOAD_DIR` definido | ambiente do container | `RuntimeError` explícito antes da rede |
| Diretório base/competência gravável | preflight de escrita | `OutputDirectoryError` antes da rede |
| Competência válida | regex/mês | `ParamValidationError` no trigger (schema) ou `InvalidCompetenciaError` na chamada direta, antes da rede |
| Limite válido | inteiro `>= 1` | `ParamValidationError` no trigger (schema) ou `InvalidLimitError` na chamada direta, antes da rede |
| Lock da competência livre | `flock` com timeout de 5 s | `ExecutionInProgressError` |
| Fonte RFB acessível | listagem WebDAV | `ListingError`, sem baixar |

---

## 5. Pós-condições

### 5.1 Sucesso total

- Todos os arquivos selecionados estão em `<CNPJ_DOWNLOAD_DIR>/<AAAA-MM>/`.
- Nenhum `.part` permanece.
- XCom `return_value` contém o resumo (seção 6).
- Log contém o resumo em JSON, sem URL/token.

### 5.2 Sucesso parcial por limite

- No máximo `N` arquivos foram processados.
- `parcial = true`; `encontrados` continua sendo o total da competência.
- A task termina com sucesso mesmo que o limite tenha ignorado o restante; o resumo deixa claro
  que a competência não foi baixada por completo (FR-008/SC-009).

### 5.3 Falha

- Pelo menos um arquivo com status `falhou` ⇒ task `failed` (`DownloadError`).
- Arquivos com `baixado` permanecem válidos e não são removidos (FR-013).
- Competência vazia ⇒ task `failed` (`EmptyCompetenciaError`), sem resumo de sucesso (FR-016).
  Inclui o 404 `RemoteResourceNotFound` de uma competência ainda não publicada: a listagem é
  tratada como vazia, não como falha de infraestrutura.
- Erro de listagem ⇒ task `failed` sem baixar arquivos.

---

## 6. Resumo (XCom `return_value`)

```json
{
  "competencia": "2026-02",
  "output_dir": "/data/cnpj/2026-02",
  "encontrados": 37,
  "selecionados": 1,
  "baixados": 1,
  "ignorados": 0,
  "falhas": 0,
  "bytes_baixados": 123456,
  "duracao_segundos": 2.41,
  "parcial": true,
  "arquivos": [
    {
      "nome": "Cnaes.zip",
      "status": "baixado",
      "bytes": 123456,
      "erro": null
    }
  ]
}
```

| Campo | Tipo | Invariante |
|---|---|---|
| `competencia` | string | `AAAA-MM` validado |
| `output_dir` | string | caminho absoluto do diretório da competência |
| `encontrados` | integer | `>= 0`; total listado |
| `selecionados` | integer | `<= encontrados` |
| `baixados` | integer | `>= 0` |
| `ignorados` | integer | `>= 0`; existentes não vazios |
| `falhas` | integer | `>= 0`; se `> 0`, task falha |
| `bytes_baixados` | integer | `>= 0` |
| `duracao_segundos` | number | `>= 0` |
| `parcial` | boolean | `limite != null && limite < encontrados` |
| `arquivos` | array | ordenado por `nome`; sem URL |

**Proibido no resumo**: URL, token, credencial, CPF, caminho absoluto fora de `output_dir`,
stack trace.

---

## 7. Observabilidade

- Log de início: competência, limite, diretório de saída.
- Log por arquivo: nome, status, bytes e motivo sanitizado em falha.
- Log final: resumo JSON.
- XCom: mesmo resumo.
- Não logar: URL de download, token, cabeçalhos de autenticação, conteúdo dos arquivos.

---

## 8. Critérios de aceite do contrato

| Cenário | Resultado esperado |
|---|---|
| Competência válida, saída vazia, limite 1 | 1 arquivo baixado; `parcial=true`; task success |
| Reexecução do mesmo limite | `baixados=0`, `ignorados=1`, `bytes_baixados=0` |
| Competência inválida | `ParamValidationError` no trigger; nenhuma task e nenhuma requisição à fonte |
| Competência válida sem arquivos | task failed; mensagem explícita |
| Um arquivo com HTTP 500 | task failed; `falhas>=1`; demais arquivos preservados |
| Duas execuções simultâneas | uma baixa; a outra falha com `ExecutionInProgressError` |
| Import da DAG sem trigger | zero requisições de rede |
| Log da execução | zero ocorrências de `public.php/dav/files/` ou do token |
