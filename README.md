# Teste com input comprimido
 - tamanho real do csv: 2047 MB
 - tamanho do output em parquet: 811 MB
 - quantidade de linhas: 26,228,745.00

## Resultados da conversao bruta de csv -> parquet

### Pandas para leitura de csv (read_csv com chunksize) e PyArrow para escrita de parquet
 - iterações: 26
 - tempo: 162.00s

### PyArrow para leitura de csv (open_csv com block_size) e escrita de parquet
 - iterações: 2000
 - tempo: 83.14s
 - 48% mais rápido em relação a leitura com pandas

### Polars para leitura de csv (scan_csv modo lazy) e PyArrow para escrita de parquet
 - Sem resultado, Polars suporta apenas os encodings utf8 e utf8-lossy


# Levanta Infra
## Requisitos
Ter docker instalado

## Comando
```shell
docker compose up -d
```

# Configuração Minio

[MinIO Console](http://localhost:9001)

---

# Download de arquivos CNPJ

Baixa os arquivos de uma competência (`AAAA-MM`) publicados pela Receita Federal
para um diretório persistente. A mesma função é usada pela CLI e pela DAG manual do
Airflow (`airflow/README.md`).

## CLI

```shell
python -m src.extracao_rfb.extract_cnpj_files \
  --competencia 2026-02 \
  --output-dir data/cnpj \
  --limite 1
```

| Argumento | Obrigatório | Default | Regra |
|---|---|---|---|
| `--competencia` | sim | — | `AAAA-MM`, mês `01`–`12` |
| `--output-dir` | não | `CNPJ_DOWNLOAD_DIR` | falha se ambos ausentes |
| `--limite` | não | `null` (todos) | inteiro `>= 1`, primeiros nomes em ordem lexicográfica |
| `--concorrencia` | não | `3` | inteiro `>= 1` |

A saída padrão é o resumo JSON (`DownloadSummary.to_dict()`), uma linha. Códigos de
saída: `0` sucesso total, `1` uma ou mais falhas de download (resumo ainda é
impresso) e `2` validação/configuração/diretório/listagem/competência vazia.

Os arquivos são gravados em `<output-dir>/<AAAA-MM>/`. Arquivos já existentes e não
vazios são ignorados; um arquivo de `0` byte é baixado de novo.

## Segurança

`data/cnpj/` contém dados brutos da RFB, incluindo CPF de sócios, e é **gitignored**.
Não publique, versione nem envie esse diretório para storage público.

