# Pesquisa Técnica — Primeira DAG de Download de Dados CNPJ

**Feature**: `002-cnpj-download-dag` · **Data**: 2026-10-27
**Escopo**: executar manualmente, no Airflow, o download de uma competência `AAAA-MM` da RFB
para um diretório persistente, com limite opcional e resumo observável.
**Fora do escopo**: agendamento, Parquet, R2, web e API (FR-020).

As decisões abaixo resolvem todos os pontos em aberto do `Technical Context`. Não há
`NEEDS CLARIFICATION` pendente.

---

## 1. Decisões

### D1 — Núcleo chamável, CLI fina e DAG como casca

**Decision:** mover a lógica de `extract_cnpj_files.py` para
`src.extracao_rfb.download_competencia.download_competencia`, uma função síncrona com
parâmetros explícitos. `extract_cnpj_files.py` permanece como entrypoint de CLI (`argparse`,
sem trabalho no import) e a DAG importa a função **dentro** do callable da task.

**Rationale:** cumpre FR-011 e o Princípio I. O parser do Airflow importa a DAG a cada
varredura; qualquer acesso à rede ou leitura de arquivo no import atrasaria o parser e poderia
falhar por indisponibilidade da RFB. O import tardio também mantém a DAG independente de
`webdav3` no processo de parsing.

**Alternatives considered:** manter o script e chamá-lo via `BashOperator` (perde o resumo
estruturado, o tratamento de erro e a testabilidade); importar `src` no topo da DAG (funciona,
mas acopla o parser ao `webdav3` e a qualquer efeito colateral futuro); usar `TaskFlow` com
`@task` (não traz ganho para uma única task e adiciona dependência de contexto).

### D2 — Layout de saída por competência e volume persistente

**Decision:** gravar em `<CNPJ_DOWNLOAD_DIR>/<AAAA-MM>/<nome-do-arquivo>`. No compose,
`CNPJ_DOWNLOAD_DIR=/data/cnpj` e o host monta `${CNPJ_DATA_HOST_DIR:-../data/cnpj}` nesse
caminho. O diretório base é configurado por ambiente, nunca por constante no código.

**Rationale:** competências diferentes podem repetir nomes de arquivo; separar por competência
torna a idempotência e a inspeção previsíveis. Bind mount no host sobrevive a `docker compose
down`/recriação do container e é acessível ao operador. O default `../data/cnpj`, relativo ao
diretório `airflow/`, cai em `data/cnpj` na raiz do repositório.

**Alternatives considered:** diretório plano (colisão entre competências e mistura de
arquivos); volume nomeado do Docker (persistente, mas menos transparente para inspeção e
backup); diretório dentro do container sem mount (perde tudo ao recriar); gravar direto em
`airflow/dags` (polui o bundle e pode ser lido pelo parser).

**Operação:** `data/` entra no `.gitignore`; o `airflow-init` cria/chowna `/data/cnpj` para
`AIRFLOW_UID:0`, e o DAG/CLI falham antes de baixar se o diretório não for gravável.

### D3 — Resultado estruturado por arquivo, sem `except:` genérico

**Decision:** reescrever `src/download_utils/download_files.py` para devolver
`DownloadReport` com uma lista ordenada de `FileResult` (`baixado`, `ignorado`, `falhou`,
bytes e erro sanitizado). Cada worker trata a própria exceção, registra o motivo e continua;
ao final a orquestração conta as falhas e levanta `DownloadError` com o resumo.

**Rationale:** o código atual engole toda exceção, não retorna nada e faz um download
incompleto parecer sucesso — violação direta do Princípio IV. O resultado por arquivo permite
cumprir FR-012/FR-013, preservar os arquivos já concluídos e testar cada cenário sem depender
da rede.

**Alternatives considered:** deixar a exceção subir no primeiro erro (perde as contagens e os
demais downloads); usar `asyncio.gather` sem tratamento (uma exceção cancela o grupo e perde o
resultado dos outros); manter `download_files` sem retorno e inspecionar o diretório (não
distingue ignorado de falha).

### D4 — Escrita atômica, arquivo vazio e temporário

**Decision:** baixar para um `.part` único (`.nome.<pid>.<uuid>.part`), validar o tamanho
recebido quando o `Content-Length` estiver disponível e só então aplicar `os.replace` no
arquivo final. Arquivo final existente com tamanho maior que zero é ignorado; arquivo de 0 byte
é baixado novamente. No início de cada execução, com o lock adquirido, remover `.part`
remanescentes de execuções interrompidas.

**Rationale:** cumpre FR-014/FR-015 e o padrão de rename atômico do Princípio IV. Nome único
evita que duas execuções concorrentes escrevam no mesmo temporário; `os.replace` garante que
um leitor nunca veja conteúdo parcial. O `.part` remanescente nunca é confundido com arquivo
completo.

**Alternatives considered:** gravar direto no destino (arquivo parcial visível); temporário de
nome fixo (conflito entre execuções); confiar só no `Content-Length` (o header pode faltar);
usar `shutil.move` (não é atômico entre sistemas de arquivos).

### D5 — Limite determinístico e semântica de "já baixado"

**Decision:** ordenar os nomes de arquivo lexicograficamente e selecionar os `N` primeiros
quando `limite` for informado. Arquivos já presentes **entre os selecionados** contam como
ignorados e não liberam vaga. `parcial = limite is not None and limite < encontrados`.

**Rationale:** cumpre FR-006/FR-008 e SC-004/SC-009. Uma ordem estável torna o teste
reproduzível e evita que uma reexecução com o mesmo limite baixe um conjunto diferente.

**Alternatives considered:** limitar após filtrar os já existentes (a reexecução passaria a
baixar arquivos diferentes e o resumo mentiria sobre o conjunto); usar a ordem do servidor
(varia entre chamadas); ordenar por tamanho (a listagem WebDAV usada não traz tamanho de forma
confiável).

### D6 — Falhas explícitas e códigos de saída distinguíveis

**Decision:** hierarquia de erros de domínio:

| Erro | Quando | Efeito |
|---|---|---|
| `InvalidCompetenciaError` | formato/mês inválidos | falha antes de criar diretório, listar ou baixar |
| `InvalidLimitError` | limite zero, negativo, booleano ou não inteiro | falha antes da rede |
| `OutputDirectoryError` | não cria ou não escreve no diretório | falha antes de listar/baixar |
| `ListingError` | token/WebDAV/listagem indisponível (exceto 404 da competência) | falha sem baixar; distingue de download |
| `EmptyCompetenciaError` | competência válida sem arquivos, inclusive diretório ainda não publicado (404) | falha explícita (não é sucesso vazio) |
| `ExecutionInProgressError` | lock da competência ocupado | falha sem tocar em arquivos |
| `DownloadError` | uma ou mais falhas de arquivo | falha ao final, com resumo e arquivos afetados |

Na CLI, `0` = sucesso total, `1` = falha de download (parcial), `2` = validação/configuração/
listagem/competência vazia. Na DAG, qualquer exceção marca a task como `failed`.

**Rationale:** cumpre FR-003/FR-007/FR-013/FR-016 e o Princípio IV, que exige status distinto
de sucesso e de sucesso parcial. A mensagem diferencia "não consegui listar" de "liste mas
falhei ao baixar".

**Alternatives considered:** um único `RuntimeError` para tudo (operador não sabe onde agir);
sucesso com resumo vazio (confunde competência errada com ingestão concluída); retornar
`False` em vez de exceção (o Airflow mostraria task verde).

### D7 — Token fora de logs e de mensagens de erro

**Decision:** nunca logar URL completa, `RFBFile`/`DownloadTarget` com `url` marcada como
`repr=False`, e sanitizar toda exceção antes de logar/retornar. O resumo contém apenas nomes,
status, bytes, contagens e duração. `src/download_utils/redaction.py` substitui URLs por
`<url>` e, para `aiohttp.ClientResponseError`, reporta somente `HTTP <status> <reason>`; para
`ClientError`, somente o nome da classe.

**Rationale:** o `webdav_hostname` do `webdavclient3` inclui `/public.php/dav/files/<token>`
(verificado), e mensagens do `aiohttp` podem carregar a URL. Sem sanitização, uma falha de
download imprimiria o token no log do Airflow (FR-019/SC-008).

**Alternatives considered:** confiar em `str(exc)` (vaza URL); remover o token do path (não é
nosso contrato com a RFB); logar só o status HTTP (perde o diagnóstico de conexão, mas é o
mínimo aceitável — mantemos o nome do arquivo e a classe do erro).

### D8 — Lock de competência e `max_active_runs=1`

**Decision:** a DAG usa `max_active_runs=1` e a orquestração adquire um `flock` exclusivo em
`<competencia_dir>/.download.lock`. Se outra execução estiver ativa, espera até 5 s e falha
com `ExecutionInProgressError`. Com o lock em mãos, limpa `.part` antigos antes de listar.

**Rationale:** cumpre FR-018 e SC-002. `max_active_runs` protege o caminho normal do Airflow;
o lock protege execuções via CLI/`airflow dags test` concorrentes, que não passam pelo
scheduler. Falhar a segunda execução é preferível a baixar o mesmo arquivo duas vezes ou
apagar o temporário da primeira.

**Alternatives considered:** confiar só na atomicidade por arquivo (não corrompe, mas deixa
temporários e duplica rede); lock no banco do Airflow (mais complexo e não cobre a CLI);
fila externa (YAGNI).

### D9 — Imagem do Airflow: overlay mínimo, código read-only e PYTHONPATH

**Decision:** criar `airflow/Dockerfile` multi-stage:

- `base`: `FROM apache/airflow:3.3.2`, instala `airflow/requirements.txt` (somente
  `webdavclient3==3.14.7`, que a imagem não traz; `aiohttp`, `requests`, `lxml`,
  `python-dateutil`, `psutil` e `python-dotenv` já estão na imagem).
- `test`: acrescenta `pytest`, `pytest-asyncio` e `pytest-cov`.

O compose usa `build` no `x-airflow-common`, monta a raiz do repositório em
`/opt/dataquery-cnpj:ro`, define `PYTHONPATH=/opt/dataquery-cnpj`,
`PYTHONDONTWRITEBYTECODE=1` e `CNPJ_DOWNLOAD_DIR=/data/cnpj`. Um serviço `airflow-tests`, no
profile `test`, usa o alvo `test`, tem entrypoint `python -m pytest` e roda a suíte por
padrão; argumentos extras do `docker compose run` substituem o comando e permitem escolher
arquivo/cobertura. O comando inclui `-p no:cacheprovider` para não tentar escrever cache no
mount read-only.

**Rationale:** copiar o `requirements.txt` canônico inteiro para a imagem faria downgrade de
`pandas 3.0.5`, `numpy 2.5.3` e `pyarrow 25.0.1` (versões da imagem) para pins antigos do
projeto, podendo quebrar o Airflow. O overlay instala apenas o que falta. Montar o código
read-only garante que a DAG e os testes usem exatamente o working tree, sem rebuild a cada
mudança, e evita que o container altere o repositório. `PYTHONDONTWRITEBYTECODE` evita
tentativa de escrita de `__pycache__` no mount read-only.

**Alternatives considered:** `_PIP_ADDITIONAL_REQUIREMENTS` (instala a cada start, não é
reprodutível e é desaconselhado pela própria imagem); copiar o código para dentro da imagem
(exige rebuild e diverge do working tree); montar só `src` (funciona, mas os testes precisam
de `tests/` e fica mais difícil validar a própria DAG); imagem com `requirements.txt` completo
(risco de conflito de versões).

### D10 — DAG fina, parâmetros tipados e resumo em XCom

**Decision:** `airflow/dags/cnpj_download.py`, `dag_id="cnpj_download_manual"`,
`schedule=None`, `catchup=False`, `max_active_runs=1`, uma `PythonOperator`. Parâmetros:

| Nome | Tipo | Regra |
|---|---|---|
| `competencia` | string ou nulo | `^\d{4}-(0[1-9]\|1[0-2])$`; obrigatório na prática (nulo falha na validação de domínio) |
| `limite` | inteiro ou nulo | `>= 1`; nulo = sem limite |

O callable lê `CNPJ_DOWNLOAD_DIR` do ambiente, chama `download_competencia` e devolve o resumo
(dict) para XCom; o resumo também é logado em JSON. `start_date` fixo, tags `cnpj`, `download`,
`manual` e `doc_md` com o comando de teste.

**Rationale:** cumpre FR-001/FR-005/FR-012. `Param` com `type=["null", "string"]` e
`type=["null", "integer"]` foi validado no container (Airflow 3.3.2): aceita nulo, aplica
`pattern`/`minimum` e rejeita tipos errados antes da task. Colocar o diretório em variável de
ambiente (e não em parâmetro) impede que um trigger escreva fora do volume previsto.

**Alternatives considered:** `BashOperator` (perde o retorno estruturado); `schedule="@daily"`
(proibido nesta etapa); `limite` como string (mais uma conversão e um erro possível); receber
`output_dir` como parâmetro (abre caminho arbitrário e foge do contrato de volume).

### D11 — Testes: unitários locais, integração opt-in e validação da DAG

**Decision:**

- `tests/unit/`: validação de competência/limite, seleção, downloader contra servidor
  `aiohttp.web` local (sucesso, 500, arquivo vazio, existente, temporário, disco/permissão),
  redaction, orquestração com cliente fake, lock e import da DAG sem rede.
- `tests/integration/test_rfb_listing.py`: marca `network`, só roda com
  `RUN_NETWORK_TESTS=1`; valida a listagem real da competência `2026-02` (não vazia, ordenada,
  sem diretórios).
- `tests/e2e/test_download_local_server.py`: fluxo completo da função contra servidor local.
- `airflow dags test cnpj_download_manual ... -c '{...}'`: validação manual real no container.
- Cobertura: `pytest --cov=src --cov-report=term-missing`, com meta inicial de 80% nos módulos
  alterados e registro do baseline no README/quickstart. `pytest.ini` fixa `testpaths=tests`,
  `asyncio_mode=auto`, o marker `network` e desabilita o cache (mount read-only).

**Rationale:** cumpre o Princípio V sem tornar o CI dependente da disponibilidade da RFB. O
servidor local permite induzir 500, corpo truncado, arquivo de 0 byte e concorrência de forma
determinística. O teste de import com `socket.connect` bloqueado prova FR-011/SC-007.

**Alternatives considered:** `aioresponses` (mais uma dependência; o servidor `aiohttp` já é
dependência da imagem); bater na RFB em todo teste (lento, instável e não testa falhas);
`unittest.mock` para o downloader inteiro (não exercita streaming/atomicidade).

### D12 — Dependências: limpeza e declaração

**Decision:** no `requirements.txt` canônico: remover a linha duplicada de `numpy`, remover
`pyspark` (não usado) e declarar `aiohttp==3.14.3` (usado pelo downloader, hoje só presente por
acaso na imagem do Airflow). `webdavclient3` permanece. `airflow/requirements-dev.txt` declara
`pytest`, `pytest-asyncio` e `pytest-cov` e referencia o overlay da imagem de teste.

**Rationale:** cumpre o Princípio VI. `pyspark` é dependência declarada sem uso; `aiohttp` é
usado e não declarado. A imagem do Airflow já traz `aiohttp`, mas o projeto não pode depender
disso implicitamente.

**Alternatives considered:** manter `pyspark` "para o futuro" (viola YAGNI e o Princípio VI);
instalar `requirements.txt` inteiro no container (ver D9).

### D13 — Endurecimento mínimo do cliente WebDAV

**Decision:** `auto_token.get_webdav_token` passa a usar `timeout=30` e a levantar erro
explícito quando não encontra o token; `auto_webdav_client.get_webdav_url` troca o `except:`
por `except Exception`, loga a causa sanitizada e levanta `RuntimeError(...) from None` para
não arrastar URL/token no traceback. `ClientRFB.list_files` ordena o resultado, filtra
diretórios e envolve falhas em `ListingError` com mensagem sem URL. A exceção
`webdav3.exceptions.RemoteResourceNotFound` (verificada para uma competência ainda não
publicada, como `2099-01`) é traduzida para lista vazia, e não para `ListingError`, para que
o caso "competência válida sem arquivos" chegue como `EmptyCompetenciaError` (FR-016). A
classe aceita `hostname`/`base_path` no construtor para testes.

**Rationale:** o cliente atual engole qualquer erro e pode deixar a causa original (com URL)
no encadeamento. Ordenação é pré-requisito de FR-006. Injeção de host/path permite testar sem
rede.

**Alternatives considered:** reescrever o cliente WebDAV (fora do escopo); manter o
`except:` (viola o Princípio IV); ordenar no orquestrador (funciona, mas deixa a
responsabilidade de contrato no lugar errado).

### D14 — Fonte da verdade dos nomes de arquivo

**Decision:** usar o nome retornado pela listagem WebDAV, reduzido a `Path(...).name` no
orquestrador, e rejeitar nomes vazios, `.`, `..` ou com separadores. Não derivar nomes de
URLs nem de regex de competência.

**Rationale:** o nome é o contrato com a RFB e a chave de idempotência no diretório. Reduzir a
basename evita path traversal e mantém o log seguro.

**Alternatives considered:** usar a URL inteira (vaza token); confiar cegamente no servidor
(risco de path traversal); renomear arquivos (quebra a correlação com a fonte e a
reexecução).

---

## 2. Evidências verificadas no ambiente

| Verificação | Resultado |
|---|---|
| Versão Python da imagem | 3.13.15 |
| Airflow | 3.3.2 (imagem `apache/airflow:3.3.2`) |
| `webdavclient3==3.14.7` | wheel `py3-none-any` disponível; instala com `requests`, `lxml`, `python-dateutil` (todos já presentes na imagem) |
| `aiohttp`, `tqdm`, `requests`, `dotenv`, `psutil`, `pyarrow`, `pandas`, `boto3` | presentes na imagem |
| `duckdb` | ausente (fora do escopo desta feature) |
| `webdavclient3` `client.webdav.hostname` | retorna a URL base com `/public.php/dav/files/<token>` — por isso a URL nunca é logada |
| Listagem real de `2026-02` | 38 entradas, incluindo o diretório `2026-02/`; 37 arquivos após filtrar diretórios |
| Competência ainda não publicada (`2099-01`) | `webdavclient3` levanta `RemoteResourceNotFound`; o cliente traduz para lista vazia → `EmptyCompetenciaError` |
| `Param` do Airflow 3 | aceita `type=["null","string"]` com `pattern` e `type=["null","integer"]` com `minimum`; rejeita tipo errado com `ParamValidationError` |
| `docker compose config` | mounts relativos ao diretório `airflow/`; `AIRFLOW_UID` atual = 1000 |
| `airflow dags test` | disponível na imagem; aceita `-c` para conf JSON |

> Nenhum token, URL com token ou credencial é reproduzido neste documento. A competência
> `2026-02` é usada apenas como referência pública de teste.

## 3. Riscos e mitigações

| Risco | Mitigação |
|---|---|
| Listagem/token da RFB indisponível | `ListingError` distingue de falha de download; task falha sem baixar; teste de rede opt-in |
| Competência válida ainda não publicada | `EmptyCompetenciaError` explícito, task falha; operador confere o mês |
| Download interrompido deixa `.part` | lock + limpeza no início; `.part` nunca é lido como completo; `os.replace` atômico |
| Duas execuções concorrentes | `max_active_runs=1` + `flock`; segunda falha com mensagem clara |
| Token em log/traceback | `repr` da URL oculto, `from None`, redaction de URLs, teste dedicado |
| Permissão do bind mount (UID 1000) | `airflow-init` cria/chowna `/data/cnpj`; preflight de escrita antes da rede |
| Imagem oficial mudar versões | overlay instala só `webdavclient3`; pins da imagem documentados; teste de import no container |
| Competência inteira é cara | `limite` para o primeiro teste; resumo marca `parcial` |
| Disco cheio | falha por arquivo, temporário removido, arquivos concluídos preservados |
