# Contrato das funções por arquivo

**Feature**: `002-cnpj-download-dag`
**Consumidor**: TaskGroup mapeado `baixar_raw → converter_bronze`

As funções recebem um `ObjectStore` ([storage.md](./storage.md)). Não importam filesystem,
boto3 ou provedor no fluxo feliz.

## Download de um ZIP

```python
def download_raw_file(
    competencia: str,
    nome: str,
    *,
    store: ObjectStore,
    client: ClientRFB | None = None,
) -> RawFileResult: ...
```

- Valida nome/competência; cria cliente RFB se necessário; URL só internamente.
- Chave: `raw/<competencia>/<nome>`.
- Se `store.head(raw_key)` existe e `size > 0`, retorna `reutilizado` sem baixar conteúdo.
- Se ausente ou size 0, transfere em streaming para scratch e `put_atomic`.
- Retorno: `nome`, `raw_key`, `status_download`, `bytes`, `duracao_segundos`.
- Erros sanitizados: sem URL, token, credencial ou path absoluto.

## Conversão de um ZIP Raw

```python
def convert_raw_file(
    competencia: str,
    nome: str,
    raw_key: str,
    *,
    store: ObjectStore,
) -> BronzeFileResult: ...
```

- Lê o ZIP da chave Raw para scratch da tarefa (stream → arquivo temporário).
- Exige um CSV; resolve colunas pelo registro central de schema.
- PyArrow em blocos, `latin1`, `;`, valores string.
- Temporário `<nome-base>.parquet`; publica `bronze/<competencia>/<nome-base>.parquet`
  só depois de terminar.
- Falha preserva Raw e o Bronze anterior; limpa scratch.
- Retorno: `nome`, `raw_key`, `bronze_key`, `status_conversao`, `bytes`/`bytes_parquet`,
  `linhas`, `duracao_segundos`.

## Listagem

```python
def listar_arquivos(
    competencia: str,
    *,
    limite: int | None = None,
    store: ObjectStore,
    client: ClientRFB | None = None,
) -> RawListing: ...
```

Valida params, `store.ping()`, lista RFB, filtra diretórios, ordena, aplica limite,
**sem** `head` de Raw para decidir o conjunto. Retorno pequeno: nomes + contagens.

## Fábrica usada pela DAG

```python
def get_store() -> ObjectStore: ...
```

Chamada só em runtime, dentro dos callables. Default local: filesystem.

## Regras de erro e repetição

Erros identificáveis por etapa (validação, listagem, download, ZIP, schema, conversão,
storage) sem vazar segredo. Retry de conversão reutiliza Raw. Retry de download reutiliza
Raw se a publicação já tiver concluído.

## Proibição de efeitos no import

Importar DAG ou módulos de domínio não lista RFB, não acessa `/data`, não chama S3 e não
converte arquivo. `read_files.py` não inicia conversão no topo do módulo.
