# Pesquisa Técnica — DAG mapeada Raw → Bronze (storage genérico)

**Feature**: `002-cnpj-download-dag` · **Data**: 2026-10-27
**Escopo**: uma DAG manual por competência; tarefas mapeadas por arquivo; Raw/Bronze no
armazenamento configurado (filesystem local ou R2 S3-compatível).

## Decisões

### R1 — Dynamic task mapping por ZIP

**Decisão**: uma tarefa de listagem devolve apenas nomes; um TaskGroup mapeado cria
`baixar_raw` e `converter_bronze` por nome, com dependência sequencial no grupo.

**Motivo**: quantidade de instâncias só é conhecida em runtime; cada arquivo tem
estado/log/retry; o parse da DAG permanece estável. Falha de um arquivo não cancela
instâncias independentes já mapeadas.

**Alternativas consideradas**: uma tarefa única para todos os ZIPs (esconde progresso e
força reprocessar tudo); loop na definição da DAG (os nomes não existem no parse).

### R2 — Airflow é dono do paralelismo por arquivo

**Decisão**: cada task de download/conversão processa um ZIP. Sem fan-out interno
multi-arquivo. Sem pool/cap extra nesta entrega.

**Motivo**: evita multiplicar concorrência do Celery com semáforo interno; retry é por
instância.

### R3 — Interface genérica, não o backend, nas tarefas

**Decisão**: extração, conversão, pipeline e DAG dependem de um protocolo
`ObjectStore` (verificar, ler, escrever atomicamente, remover). A fábrica lê
`CNPJ_STORAGE_BACKEND` e devolve a implementação.

**Motivo**: FR-018 e SC-012. Trocar local por R2 não altera DAG nem funções de domínio.

**Alternativas consideradas**: `S3ObjectStore` direto nas tasks (plano anterior / código
atual) — amarra o local a MinIO e contradiz FR-019; paths de filesystem nas tasks —
impede R2 sem duplicar o fluxo.

### R4 — Backend local: filesystem em `data/` → `/data`

**Decisão**: no modo local, a raiz de dados é `data/` no host e `/data` nos containers
Airflow que executam tarefas (`CNPJ_DATA_ROOT=/data`). Chave lógica
`raw/2026-02/Cnaes.zip` mapeia para `/data/raw/2026-02/Cnaes.zip`. Compose **não**
depende de MinIO para subir o Airflow.

O volume é compartilhado entre scheduler/workers no Compose local, então um worker pode
ler o Raw escrito por outro. Isso é aceitável **somente** no backend filesystem local.
Scratch da tarefa (ZIP aberto, Parquet em construção) permanece em diretório temporário
do processo e é apagado em `finally`, para não deixar parciais em `raw/` ou `bronze/`.

**Motivo**: FR-016/019, SC-011. O operador inspeciona artefatos no repositório sem
cliente S3.

**Alternativas consideradas**: manter MinIO local (simplicidade S3-only da constituição)
— rejeitado pela spec revisada; montar `data/cnpj` como hoje (`/data/cnpj`) — o prefixo
extra diverge de `raw/<AAAA-MM>/` sob `/data`.

### R5 — Backend de produção: Cloudflare R2 via API S3

**Decisão**: `CNPJ_STORAGE_BACKEND=s3` reutiliza a semântica já implementada em
`S3ObjectStore` (boto3, PUT/multipart, chave final só após complete). Endpoint, bucket,
região e credenciais vêm do ambiente. O provisionamento da conta/bucket R2 é operacional
e está fora do escopo; a **seleção** do backend faz parte do escopo (FR-017, FR-020).

**Motivo**: R2 é S3-compatível; o código S3 já existente cobre atomicidade e redaction.
Não introduzir SDK Cloudflare específico (YAGNI, VI).

**Alternativas consideradas**: SDK R2 nativo; segundo pipeline só para produção.

### R6 — Layout Medallion e retenção (independente do backend)

**Decisão**:

- `raw/<AAAA-MM>/<nome-original>`: ZIP de origem, preservado por competência.
- `bronze/<AAAA-MM>/<nome-base>.parquet`: um Parquet por ZIP.
- XCom e resumo usam essas chaves lógicas, não `file://` nem `s3://bucket/...`.

**Motivo**: constituição 1.1.0; reprocessar sem baixar de novo da RFB; Bronze é só
normalização de formato.

### R7 — Atomicidade por backend

**Decisão**:

- **Filesystem**: gravar em arquivo temporário no **mesmo filesystem** da destino
  (`CNPJ_DATA_ROOT/tmp/` ou diretório pai da chave) e promover com `os.replace`.
  Falha/interrupção remove o temporário; a chave final anterior permanece. Tamanho 0
  não conta como Raw válido.
- **S3/R2**: PUT simples ou multipart; a chave final só existe após `complete`. Abort
  remove upload incompleto. Objeto size 0 não é reutilizado.

Duas execuções da mesma competência: `max_active_runs=1` evita escrita concorrente na
mesma chave.

**Motivo**: FR-012, SC-006.

### R8 — Conversão segue o contrato existente

**Decisão**: schema central, um CSV por ZIP, PyArrow `open_csv` em blocos, `latin1`,
`;`, colunas como texto, um Parquet por ZIP. `read_files.py` não pode disparar
conversão no import.

**Motivo**: Principle II/III; FR-008.

### R9 — Configuração e Compose

**Decisão**:

| Variável | Papel |
|---|---|
| `CNPJ_STORAGE_BACKEND` | `filesystem` (default local) ou `s3` |
| `CNPJ_DATA_ROOT` | raiz filesystem (`/data` nos containers) |
| `CNPJ_S3_ENDPOINT` | endpoint S3/R2 (obrigatório se `s3`) |
| `CNPJ_S3_BUCKET` | bucket (obrigatório se `s3`) |
| `CNPJ_S3_ACCESS_KEY` / `CNPJ_S3_SECRET_KEY` | credenciais (obrigatório se `s3`) |
| `CNPJ_S3_REGION` | opcional; R2 aceita região placeholder |

Preflight: filesystem verifica que a raiz existe e é gravável; S3 faz `head_bucket`
(ou equivalente) **depois** da validação de competência/limite e **antes** de baixar
conteúdo da RFB. A listagem RFB ainda ocorre após o preflight do store.

Segredos nunca têm default de produção no código. `.env` permanece gitignored.

**Motivo**: FR-016, SC-012, SC-013.

### R10 — Testes

**Decisão**: o fake de testes implementa o **protocolo**, não a API boto3. Unitários do
filesystem usam `tmp_path`. Integração S3/R2 é skip se o backend `s3` não estiver
configurado. E2E local escreve em diretório temporário ou `data/` de teste, sem MinIO.

**Motivo**: Principle V; o checklist marca a troca MinIO → filesystem como trabalho de
plano/implementação.

## Cenários de falha e resultado

| Cenário | Resultado |
|---|---|
| Competência/limite inválidos | Falha antes de RFB e store |
| Store inacessível no preflight | Falha antes de transferências |
| Competência sem arquivos | Nenhum grupo mapeado; run falha (conjunto vazio) |
| ZIP Raw existente e size > 0 | Download `reutilizado`; conversão segue |
| Raw size 0 ou ausente | Download da RFB (ou falha explícita se a fonte falhar) |
| Download falha | `converter_bronze` daquele arquivo não executa; demais seguem; run `failed` |
| Conversão falha | Raw permanece; Bronze anterior intacto; demais seguem; run `failed` |
| Import da DAG | Sem rede, disco de dados ou conversão |

## Registro de pendências

Nenhum `NEEDS CLARIFICATION`. Fora de escopo operacional: criar bucket R2, migrar
objetos MinIO existentes, política de expiração de Raw.
