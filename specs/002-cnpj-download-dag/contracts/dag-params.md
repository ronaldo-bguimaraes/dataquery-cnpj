# Contrato da DAG — `cnpj_download_manual`

**Feature**: `002-cnpj-download-dag`
**Tipo**: DAG manual por competência, dynamic task mapping por arquivo

## Identidade e grafo

- `dag_id`: `cnpj_download_manual`; `schedule=None`; `catchup=False`; `max_active_runs=1`.
- Uma tarefa de listagem/seleção roda em runtime (nunca no import).
- Para cada arquivo selecionado, Airflow instancia `baixar_raw → converter_bronze`.
- Quantidade e paralelismo seguem a listagem e o executor. Sem cap próprio na DAG.
- Falha de um arquivo não bloqueia grupos independentes; qualquer falha deixa o DAG Run
  `failed`.

O grafo **não muda** quando o backend de armazenamento muda.

## Parâmetros do trigger

| Nome | Tipo/regra | Obrigatório | Semântica |
|---|---|---|---|
| `competencia` | string `^\d{4}-(0[1-9]\|1[0-2])$` | sim | competência publicada na RFB |
| `limite` | inteiro `>= 1` ou nulo | não | primeiros N nomes; nulo = todos |

```json
{"competencia":"2026-02","limite":1}
```

Limite: depois da ordenação estável, **antes** de verificar Raw. Arquivo existente não
libera vaga e segue para conversão.

## Dados entre tarefas

1. `listar_arquivos` valida params, faz preflight do store, lista RFB, devolve nomes e
   metadados de seleção (`encontrados`, `selecionados`, `parcial`).
2. `baixar_raw` deriva a URL internamente (não a retorna), consulta `raw/<competencia>/<nome>`
   via `ObjectStore`, reutiliza ou baixa e publica; retorna chave lógica/status/bytes.
3. `converter_bronze` lê a chave Raw pelo mesmo store, converte em scratch e publica
   `bronze/<competencia>/<nome-base>.parquet`.

## Configuração do store

A DAG obtém o store só dentro dos callables, via fábrica (ver [storage.md](./storage.md)).
Não instancia `S3ObjectStore` nem abre paths no corpo do módulo.

## Regras de estado

- Competência/limite inválidos: falha antes de RFB e store.
- Store inacessível: falha antes de transferências.
- Listagem vazia: run failed, nenhum mapping.
- Download falho: impede só a conversão pareada.
- Conversão falha: Raw mantido; Bronze anterior intacto.
- Publicação incompleta: invisível na chave final.
- Raw size > 0: sem novo GET de conteúdo à RFB.
- Resumo: por arquivo `nome`, estados, bytes, linhas, chaves lógicas, erro sanitizado.
