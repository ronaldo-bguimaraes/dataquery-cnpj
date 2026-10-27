# Implementation Plan: Download e conversão CNPJ com Airflow

**Branch**: `002-cnpj-download-dag` | **Date**: 2026-10-27 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/002-cnpj-download-dag/spec.md`

## Summary

Manter a DAG manual `cnpj_download_manual` (uma execução por competência `AAAA-MM`) e o
dynamic task mapping já definido: listagem em runtime e, para cada ZIP selecionado,
`baixar_raw → converter_bronze`. Esta revisão de plano troca o armazenamento local de
MinIO obrigatório por uma **interface genérica de armazenamento**, com dois backends
selecionados só por configuração:

- **local**: filesystem em `data/` na raiz do repositório, montado como `/data` nos
  containers que executam tarefas;
- **produção**: Cloudflare R2 via API compatível com S3.

Download e conversão não conhecem filesystem, bucket nem provedor. Localizações lógicas
permanecem `raw/<AAAA-MM>/<nome-original>` e `bronze/<AAAA-MM>/<nome-base>.parquet`.
XCom transporta só metadados pequenos. Silver/Gold, schedule, frontend, API e o
provisionamento operacional do R2 ficam fora do escopo.

O código e os testes atuais ainda falam em `S3ObjectStore`/MinIO; a implementação seguinte
deve introduzir o protocolo, o backend de filesystem e a seleção por configuração, e
desacoplar o Compose local do MinIO.

## Technical Context

**Language/Version**: Python 3.13 em Airflow 3.3.2 com CeleryExecutor.

**Primary Dependencies**: Airflow existente; `webdavclient3`, `aiohttp` e `pyarrow` na
imagem de execução; `boto3` somente no backend S3/R2. Pytest na imagem de teste. Sem
instalar o `requirements.txt` completo do projeto sobre a imagem Airflow. Nenhuma
dependência nova além das já usadas (filesystem usa a stdlib).

**Storage**: interface genérica (`exists`/`head`, leitura, escrita atômica, remoção) com
prefixos lógicos `raw/` e `bronze/`. Backend local: diretório base `/data` (host:
`data/`). Backend de produção: R2 via endpoint/bucket/credenciais S3-compatíveis.
Seleção e parâmetros vêm de ambiente (`.env` ignorado), nunca hardcoded.

**Testing**: pytest; unitários com fake da interface; filesystem temporário para o backend
local; testes S3/R2 com cliente fake ou endpoint configurado (opt-in); DAG import sem I/O;
E2E local com amostra em `data/` (sem MinIO obrigatório). Teste de rede RFB continua opt-in.

**Target Platform**: containers Docker Compose (Airflow workers/scheduler) em Linux; o
mesmo código de domínio deve rodar em testes locais fora do Compose.

**Project Type**: pipeline de dados / DAG Airflow (sem frontend nem API).

**Performance Goals**: conversão em streaming PyArrow (`open_csv` + `block_size` pela
memória disponível); não carregar ZIP/CSV inteiros em RAM; baseline do README permanece o
piso; medir pico de memória por worker no primeiro run com `limite=1`.

**Constraints**: trigger manual; limite aplicado antes da existência Raw; Raw retido por
competência; Bronze substituído só após geração completa; parse da DAG sem I/O; logs sem
token/credenciais; sem Silver/Gold, schedule, web ou API; sem provisionar conta/bucket R2
nesta entrega.

**Scale/Scope**: uma competência por run; dezenas de ZIPs por competência; primeiro teste
operacional com `limite=1`.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Princípio | Verificação |
|---|---|
| I. Estágios desacoplados | Funções chamáveis recebem competência, nome e um `ObjectStore` injetado; nada executa no import. Backend escolhido só na fábrica de configuração. |
| II. Schema-first | Conversão consulta o registro central (`schema.py`); sem lista de colunas duplicada nem inferência. |
| III. Streaming | Download, abertura do CSV e escrita Parquet em blocos; scratch local de tarefa, não o arquivo final. |
| IV. Falhas visíveis | Estado por arquivo, run `failed` se qualquer instância falhar, temporários removidos, localização final só com artefato completo. |
| V. Test-first | Testes falhando da interface, filesystem, seleção de backend e DAG antes de cada mudança de comportamento. |
| VI. Simplicidade | Protocolo justificado por **dois usos reais** (filesystem local e S3/R2). Sem MinIO no caminho local. `boto3` permanece só no backend S3. |
| Segurança e retenção | Credenciais só no ambiente; logs/XCom sanitizados; Raw preservado por competência (constituição 1.1.0); `socios` não vai para bucket público. |
| Infraestrutura | Compose local sobe Airflow e monta `data/` → `/data`. **Desvio**: a constituição 1.1.0 exige storage apenas via API S3/boto3 (MinIO local + object storage gerenciado). Esta feature, por FR-016–FR-020, usa filesystem local e R2 S3-compatível atrás da mesma interface. Ver Complexity Tracking. |

**Gate**: aprovado com o desvio de infraestrutura documentado e justificado pela spec
atualizada (checklist de requisitos de 2026-10-27). Não há `NEEDS CLARIFICATION` de
produto. Uma emenda futura da constituição deveria substituir “somente S3/MinIO” por
“interface de armazenamento + backends configuráveis”, para eliminar o desvio permanente.

**Reavaliação pós-design**: `research.md` resolve seleção de backend, atomicidade por
implementação e volume compartilhado local; `data-model.md` trata localizações lógicas
independentes do provedor; `contracts/` define o protocolo e a fábrica; `quickstart.md`
valida o fluxo em `data/` e a troca só por configuração. O TDD permanece obrigatório.

## Project Structure

### Documentation (this feature)

```text
specs/002-cnpj-download-dag/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── README.md
│   ├── dag-params.md
│   ├── download-api.md
│   └── storage.md
└── tasks.md              # /speckit-tasks — não gerado aqui
```

### Source Code (repository root)

```text
src/
├── storage/                 # protocolo + backends (filesystem, S3/R2) + fábrica
├── extracao_rfb/            # listagem e download de um ZIP
├── conversao/               # ZIP Raw → Parquet Bronze
├── pipeline/                # orquestração pura e resumo
├── download_utils/          # streaming/redação já existentes
└── webdav/                  # cliente RFB já existente
airflow/dags/cnpj_download.py
data/                        # raiz local Raw/Bronze (gitignored, .gitkeep)
tests/
├── unit/
├── integration/             # filesystem local; S3/R2 opt-in
├── e2e/
└── fakes.py
```

**Structure Decision**: repositório único. A DAG só importa domínio dentro dos callables.
`src/storage/` deixa de ser “cliente MinIO” e passa a ser o ponto único de persistência
usado por extração e conversão. Compose monta `./data` em `/data` nos serviços que
executam tarefas; MinIO deixa de ser dependência de subida do Airflow local.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Constituição: “storage MUST usar apenas a API S3 (boto3)” vs filesystem local + R2 | FR-016/019 exigem `data/`/`/data` no modo local, sem MinIO; FR-020 exige R2 S3-compatível em produção, sem mudar DAG/download/conversão | Manter MinIO local (plano anterior) contradiz a spec revisada e o checklist; dois códigos paralelos (paths vs boto3 nas tasks) quebraria FR-018 |
| Protocolo `ObjectStore` (abstração extra) | Dois backends reais na mesma entrega | Um único backend S3 não cobre o modo local; um único filesystem não cobre R2 |

## Topologia da DAG

Inalterada em relação ao incremento já entregue:

1. `listar_arquivos`: valida competência/limite, confirma acesso ao backend configurado,
   consulta a RFB, filtra diretórios, ordena nomes, aplica o limite **antes** de checar
   Raw, devolve lista pequena `{nome}` para XCom.
2. `pipeline_arquivo.expand(...)`:
   - `baixar_raw`: reutiliza `raw/<competencia>/<nome>` se existir e tamanho > 0; senão
     baixa em streaming para scratch e publica atomicamente no store.
   - `converter_bronze`: lê o ZIP lógico Raw, converte em blocos, publica
     `bronze/<competencia>/<nome-base>.parquet`.
3. `consolidar_resumo`: agrega estados; falha o run se qualquer arquivo falhou.

Paralelismo efetivo: CeleryExecutor / configuração Airflow; a DAG não adiciona cap.

## Decisões de implementação

| Decisão | Implementação |
|---|---|
| Interface | Protocolo mínimo: `head`/`exists`, `open`/`get` em stream, `put_atomic` (origem arquivo local), `delete`, `ping`/preflight. Chaves lógicas, nunca URI com credencial. |
| Fábrica | `CNPJ_STORAGE_BACKEND=filesystem\|s3` (default local `filesystem`). S3 cobre R2. |
| Filesystem | Base `CNPJ_DATA_ROOT` (`/data` nos containers; `data/` no host). Escrita: arquivo em `tmp/` da base ou `NamedTemporaryFile` no mesmo filesystem + `os.replace`. |
| S3/R2 | Reutilizar a semântica atual de `S3ObjectStore` (PUT/multipart; chave final só após complete). Endpoint R2 e bucket vêm do ambiente. |
| Compose local | Remover `depends_on` MinIO do Airflow; montar `../data:/data`; garantir `data/.gitkeep`. Scratch de tarefa continua em temp do worker, não em `data/raw`. |
| Volume compartilhado | No Compose local, workers compartilham `/data`. Em produção R2, workers não compartilham disco; o store S3 é o canal. |
| XCom | `nome`, chaves lógicas, status, bytes, linhas, duração. Sem path absoluto, URI S3 autenticada, token ou bytes. |
| MinIO | Fora do caminho feliz local. Testes de integração S3 permanecem opt-in contra qualquer endpoint compatível (incluindo R2/MinIO se alguém configurar). |
