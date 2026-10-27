# Contrato da API Chamável e da CLI

**Feature**: `002-cnpj-download-dag`
**Implementação prevista**: `src/extracao_rfb/download_competencia.py`,
`src/extracao_rfb/competencia.py`, `src/extracao_rfb/extract_cnpj_files.py` e
`src/download_utils/download_files.py`
**Consumidores**: DAG `cnpj_download_manual`, operador via CLI e suíte de testes

---

## 1. Função de domínio

### 1.1 `download_competencia`

```python
def download_competencia(
    competencia: str,
    output_dir: str | os.PathLike[str],
    *,
    limite: int | None = None,
    concorrencia: int = 3,
    chunk_size: int = 1024 * 1024,
    client: ClientRFB | None = None,
    lock_timeout: float = 5.0,
) -> DownloadSummary
```

Executa a competência de ponta a ponta (validação, diretório, lock, listagem, seleção,
download, resumo) e devolve o resumo. É a função usada pela DAG e pela CLI.

**Entradas**

| Parâmetro | Tipo | Obrigatório | Default | Regras |
|---|---|---|---|---|
| `competencia` | `str` | sim | — | `AAAA-MM`, mês 01–12; espaços nas pontas são removidos |
| `output_dir` | `str \| PathLike` | sim | — | diretório base; o subdiretório da competência é criado |
| `limite` | `int \| None` | não | `None` | inteiro `>= 1`, não booleano; `None` = todos |
| `concorrencia` | `int` | não | `3` | inteiro `>= 1`; número de downloads simultâneos |
| `chunk_size` | `int` | não | `1 MiB` | inteiro `>= 1`; bytes por chunk de streaming |
| `client` | `ClientRFB \| None` | não | `None` | injeção para testes; `None` cria o cliente real |
| `lock_timeout` | `float` | não | `5.0` | segundos de espera pelo lock da competência; `>= 0`; estourado ⇒ `ExecutionInProgressError` |

**Saída**: `DownloadSummary` (seção 3). Em sucesso, todos os arquivos selecionados estão no
disco e nenhum `.part` permanece. Em falha de arquivo, levanta `DownloadError` contendo o
resumo; os arquivos concluídos permanecem.

**Efeitos colaterais permitidos**

- criar `<output_dir>/<AAAA-MM>/` e o arquivo `.download.lock`;
- criar/remover `.part` temporários;
- gravar arquivos finais;
- listar e baixar da RFB;
- logar nomes/contagens/duração (nunca URL/token).

**Proibido**

- executar trabalho no import do módulo;
- aceitar `output_dir` vindo de parâmetro da DAG;
- expor URL/token em exceção, log, `repr` ou resumo;
- converter para Parquet, publicar em storage, agendar, expor web/API.

### 1.2 `download_competencia_async`

```python
async def download_competencia_async(
    competencia: str,
    output_dir: str | os.PathLike[str],
    *,
    limite: int | None = None,
    concorrencia: int = 3,
    chunk_size: int = 1024 * 1024,
    client: ClientRFB | None = None,
    lock_timeout: float = 5.0,
) -> DownloadSummary
```

Mesmo contrato da versão síncrona, para uso em contexto já assíncrono (testes). A versão
síncrona usa `asyncio.run` e não deve ser chamada dentro de um event loop ativo. A espera
pelo lock acontece em thread separada (`asyncio.to_thread`) para não bloquear o event loop.

### 1.3 Funções de apoio

```python
def validate_competencia(value: str | None) -> str
def validate_limit(value: int | None) -> int | None
def select_files(paths: Sequence[str], limite: int | None) -> list[str]
```

| Função | Regra | Erro |
|---|---|---|
| `validate_competencia` | normaliza espaços e aplica `^\d{4}-(0[1-9]\|1[0-2])$` | `InvalidCompetenciaError` |
| `validate_limit` | `None` ou inteiro `>= 1`; rejeita `bool` | `InvalidLimitError` |
| `select_files` | ordena lexicograficamente e corta nos `N` primeiros | `InvalidLimitError` se o limite for inválido |

---

## 2. Hierarquia de erros

```text
ValueError
├── InvalidCompetenciaError
└── InvalidLimitError

RuntimeError
├── OutputDirectoryError
├── ListingError
├── EmptyCompetenciaError
├── ExecutionInProgressError
└── DownloadError          # carrega .summary: DownloadSummary
```

| Erro | Quando | Arquivos tocados | Mensagem contém |
|---|---|---|---|
| `InvalidCompetenciaError` | valor nulo, vazio ou fora de `AAAA-MM`/mês 01–12 | nenhum | formato esperado |
| `InvalidLimitError` | limite zero/negativo/booleano/não inteiro | nenhum | regra violada |
| `ValueError` | `concorrencia < 1`, `chunk_size < 1` ou `lock_timeout < 0` | nenhum | regra violada |
| `OutputDirectoryError` | não cria ou não escreve no diretório | nenhum | caminho e causa sanitizada |
| `ListingError` | token/WebDAV/listagem indisponível (exceto 404 da competência) | nenhum | competência e causa sanitizada |
| `EmptyCompetenciaError` | listagem válida com zero arquivos, inclusive 404 de competência não publicada | nenhum download | competência |
| `ExecutionInProgressError` | lock ocupado após `lock_timeout` (default 5 s) | nenhum | competência |
| `DownloadError` | uma ou mais falhas por arquivo | arquivos concluídos preservados | contagens + nomes afetados |

**Regra de segurança**: nenhuma mensagem ou atributo de erro contém URL, token ou credencial.
O encadeamento (`raise ... from ...`) não é usado quando a causa puder conter URL; a causa é
logada sanitizada e o erro é levantado com `from None`.

---

## 3. Tipos de resultado

### 3.1 `DownloadSummary`

| Campo | Tipo | Invariante |
|---|---|---|
| `competencia` | `str` | validado |
| `output_dir` | `str` | absoluto |
| `encontrados` | `int` | `>= 0` |
| `selecionados` | `int` | `<= encontrados` |
| `baixados` | `int` | `>= 0` |
| `ignorados` | `int` | `>= 0` |
| `falhas` | `int` | `>= 0`; `> 0` ⇒ `DownloadError` |
| `bytes_baixados` | `int` | `>= 0` |
| `duracao_segundos` | `float` | `>= 0` |
| `parcial` | `bool` | `limite is not None and limite < encontrados` |
| `arquivos` | `tuple[FileResult, ...]` | ordenado por `nome` |

Método `to_dict() -> dict` devolve apenas tipos JSON (sem `Path`, sem URL) para XCom/log.

### 3.2 `FileResult`

| Campo | Tipo | Valores |
|---|---|---|
| `nome` | `str` | basename da RFB |
| `status` | `Literal["baixado", "ignorado", "falhou"]` | estado final |
| `bytes` | `int` | bytes gravados (0 em ignorado/falha) |
| `erro` | `str \| None` | sanitizado; só em `falhou` |

### 3.3 `DownloadReport` (interno, usado nos testes)

```python
@dataclass(frozen=True)
class DownloadTarget:
    nome: str
    url: str  # repr=False

@dataclass(frozen=True)
class DownloadReport:
    resultados: tuple[FileResult, ...]
    baixados: int
    ignorados: int
    falhas: int
    bytes_baixados: int
    duracao_segundos: float
```

`download_files` não levanta erro por falha individual; devolve o relatório. A orquestração
decide falhar no final. Erros de configuração (`concorrencia <= 0`, `chunk_size <= 0`) são
`ValueError` antes de abrir a sessão.

---

## 4. Garantias do downloader

| Garantia | Como |
|---|---|
| Streaming | `iter_chunked(chunk_size)` gravando direto no `.part`; nenhum arquivo em memória |
| Atômico | `.part` único → `os.replace` no nome final |
| Idempotente | arquivo final `is_file()` e `st_size > 0` ⇒ `ignorado`, sem HTTP |
| Arquivo vazio | `st_size == 0` ⇒ baixa de novo e substitui |
| Falha isolada | cada arquivo em `try/except Exception`; erro sanitizado; demais continuam |
| Temporário | removido em `finally`; `.part` antigo limpo no início sob lock |
| Tamanho | se `Content-Length` existir e não houver `Content-Encoding`, valida bytes recebidos |
| Concorrência | fila + `concorrencia` workers; resultados ordenados por nome no fim |
| Sem token | URL com `repr=False`; log só nome/status/classe do erro |
| Sem `except:` | apenas `except Exception` com registro e resultado `falhou` |

---

## 5. CLI — `python -m src.extracao_rfb.extract_cnpj_files`

```shell
python -m src.extracao_rfb.extract_cnpj_files \
  --competencia 2026-02 \
  --output-dir /data/cnpj \
  --limite 1
```

| Argumento | Obrigatório | Default | Regra |
|---|---|---|---|
| `--competencia` | sim | — | `AAAA-MM` |
| `--output-dir` | não | `CNPJ_DOWNLOAD_DIR` | falha se ambos ausentes |
| `--limite` | não | `None` | inteiro `>= 1` |
| `--concorrencia` | não | `3` | inteiro `>= 1` |

**Saída padrão**: resumo JSON (`DownloadSummary.to_dict()`), uma linha.
**Erros**: mensagem sanitizada em stderr; resumo (quando existir) em JSON.

| Código | Significado |
|---|---|
| `0` | todos os arquivos selecionados baixados ou já presentes |
| `1` | uma ou mais falhas de download (`DownloadError`); resumo disponível |
| `2` | validação, configuração, diretório, listagem ou competência vazia |

O código de saída distingue sucesso total de sucesso parcial/falha, conforme o Princípio IV.
A DAG não usa o código da CLI: chama a função diretamente e deixa a exceção marcar a task.

---

## 6. Exemplos de uso

### 6.1 DAG (import tardio)

```python
def executar(**context):
    from src.extracao_rfb.download_competencia import download_competencia

    resumo = download_competencia(
        competencia=context["params"]["competencia"],
        output_dir=os.environ["CNPJ_DOWNLOAD_DIR"],
        limite=context["params"].get("limite"),
    )
    return resumo.to_dict()
```

### 6.2 Teste com cliente fake

```python
class FakeClient:
    def list_files(self, competencia):
        return ["Cnaes.zip"]

    def get_file_url(self, caminho):
        return f"http://127.0.0.1:{port}/Cnaes.zip"
```

O teste usa um servidor `aiohttp.web` local; nenhuma chamada à RFB é feita na suíte padrão.

---

## 7. Critérios de aceite do contrato

| Cenário | Resultado |
|---|---|
| Importar o módulo sem chamar a função | nenhuma rede, nenhum arquivo criado |
| Competência inválida | `InvalidCompetenciaError` antes de diretório/rede |
| Limite inválido | `InvalidLimitError` antes de diretório/rede |
| Diretório sem permissão | `OutputDirectoryError` antes de listar/baixar |
| Listagem vazia | `EmptyCompetenciaError`; nenhum download |
| Um arquivo falha | `DownloadError.summary.falhas >= 1`; demais preservados |
| Reexecução | `baixados=0`, `ignorados=encontrados` (sem limite), `bytes_baixados=0` |
| `limite=1` com 37 arquivos | `selecionados=1`, `parcial=true`, no máximo 1 baixado |
| Duas chamadas concorrentes na mesma competência | uma baixa; a outra falha com `ExecutionInProgressError` após `lock_timeout` |
| URL/token em qualquer saída | ausente em log, exceção, `repr` e resumo |
